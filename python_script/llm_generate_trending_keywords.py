import os
import re
from collections import Counter, defaultdict
from timeit import main
from dotenv import load_dotenv
import psycopg2 
from openai import OpenAI
import json
from token_tracker import track
import token_tracker

load_dotenv()


DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "twitter_conflicts"),
    "user":     os.getenv("DB_USER", "tw_user"),
    "password": os.getenv("DB_PASSWORD"),
}

def get_db_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )


client = OpenAI(
    base_url="http://localhost:8081/v1",
    api_key="",
)

SQL_GET_EVENTS = """
SELECT
    SUMMARY_TEXT,
    CREATED_AT::DATE AS DAY
FROM TWEETS T
WHERE
    T.CREATED_AT >= NOW() - INTERVAL '{days} days'
    AND T.IS_DUPLICATE = 'false'
    AND T.GEOM IS NOT NULL
    AND (T.IS_DELAYED = 'false' OR T.IS_DELAYED IS NULL)
    AND NOT (T.CONFLICT_TYPOLOGY = 'MIL' AND T.NOMINATIM_QUERY NOT LIKE '%,%')
    AND IMPORTANCE_SCORE >= 2
"""

# NOTE: schema attendu désormais :
# KW_TENDANCIES(CREATED_AT, KW1, KW1_CTX, KW2, KW2_CTX, KW3, KW3_CTX, KW4, KW4_CTX, KW5, KW5_CTX)
SQL_INSERT_KEYWORDS = """
INSERT INTO
	KW_TENDANCIES (CREATED_AT, KW1, KW1_CTX, KW2, KW2_CTX, KW3, KW3_CTX, KW4, KW4_CTX, KW5, KW5_CTX)
VALUES
	(CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (CREATED_AT) 
DO UPDATE SET
    KW1 = EXCLUDED.KW1,
    KW1_CTX = EXCLUDED.KW1_CTX,
    KW2 = EXCLUDED.KW2,
    KW2_CTX = EXCLUDED.KW2_CTX,
    KW3 = EXCLUDED.KW3,
    KW3_CTX = EXCLUDED.KW3_CTX,
    KW4 = EXCLUDED.KW4,
    KW4_CTX = EXCLUDED.KW4_CTX,
    KW5 = EXCLUDED.KW5,
    KW5_CTX = EXCLUDED.KW5_CTX
"""

def build_system_prompt() -> str:
    return """You are a precise OSINT data analyst.
Analyze the provided text to identify the top 5 emerging trends or patterns.

CRITICAL RULES:
1. Extract EXACTLY 5 entities/events. For each one, provide two fields:
   - "term": the normalized keyword used for filtering/matching against the source text.
   - "context": a short, SELF-CONTAINED headline-style phrase (2 to 4 words)
     explaining WHAT is happening and WHERE/WHOM it involves. It MUST include
     the entity name from "term" (or a natural reference to it) so the phrase
     reads correctly on its own, without needing "term" prepended to it.
     Examples: "Coup attempt Niamey", "Ozon warehouses hit",
     "Wildberries logistics struck". This is for display only.

2. "term" MUST follow these constraints:
   - Its root word(s) MUST come from the input text (see normalization rule below for how to trim it).
   - DO NOT include weapons or armament terms (exclude "missile", "drone", "FAB-500", "UAV", "FPV", "artillery").

3. "context" constraints:
   - Must be a short, complete-reading phrase, 2 to 4 words, never a full sentence with a trailing period.
   - MUST explicitly include the term's entity name (proper name or place) inside the phrase itself.
   - Should state the specific event/reason driving the trend, in your own words.
   - MAY reference weapons/armament terms if relevant (the weapons exclusion in rule 2 applies only to "term").
   - Must NOT be just "term" followed by a generic word (e.g. "Ozon logistics" is too thin) — it needs to convey the actual event.

4. NORMALIZATION RULE (applies to "term" only) — CHOOSE ONE FORM, NEVER COMBINE:
   For each entity, decide whether it has a distinctive PROPER NAME (a brand,
   company, or place name) or only a GENERIC TYPE (a facility category with
   no distinctive name).

   - IF the entity has a proper name (brand/company name) → output ONLY that
     proper name, alone. Strip any generic descriptor attached to it
   - NEVER output "ProperName + generic type" together
     (e.g. "Ozon logistics center" is FORBIDDEN — output "Ozon" only).
   - NEVER output "PlaceName + generic type" together
   - When in doubt whether a name is a brand or just a place, prefer the
     generic type — it groups more events and avoids false precision.

5. Focus "term" on targeted assets, non-military entities, locations, or impact mechanisms.

Output JSON format strictly:
{
  "keywords": [
    {"term": "exact_term_1", "context": "self-contained headline including exact_term_1"},
    {"term": "exact_term_2", "context": "self-contained headline including exact_term_2"},
    {"term": "exact_term_3", "context": "self-contained headline including exact_term_3"},
    {"term": "exact_term_4", "context": "self-contained headline including exact_term_4"},
    {"term": "exact_term_5", "context": "self-contained headline including exact_term_5"}
  ]
}"""

def _call_llm(user_content: str) -> dict | None:

    response = client.chat.completions.create(
        model="gemma-4-26B-A4B",
        messages=[
            {"role": "system", "content": build_system_prompt()},
            {"role": "user", "content": user_content},
        ],
        response_format={"type": "json_object"},
        temperature=0,
        top_p=0.8,
    )
    track(response)
    raw = response.choices[0].message.content
    if raw is None:
        print("[WARN] Réponse vide du modèle")
        return None
    raw = raw.strip()

    # Nettoyage des fences Markdown éventuelles (```json ... ```)
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw.startswith("json"):
            raw = raw[4:].strip()

    if not raw:
        print("[WARN] Réponse vide après nettoyage")
        return None
    return json.loads(raw)


def _normalize_keywords(raw_keywords: list) -> list[dict]:
    """
    Normalise la sortie LLM en une liste de dicts {"term": str, "context": str}.
    Tolère une éventuelle sortie legacy (liste de strings) pour ne pas casser
    si le prompt/modèle régresse un jour vers l'ancien format.
    """
    normalized = []
    for kw in raw_keywords[:5]:
        if isinstance(kw, dict):
            term = str(kw.get("term", "")).strip()
            context = str(kw.get("context", "")).strip()
        else:
            # fallback legacy: simple string, pas de contexte disponible
            term = str(kw).strip()
            context = ""
        normalized.append({
            "term": term.capitalize() if term else "",
            "context": context,
        })
    while len(normalized) < 5:
        normalized.append({"term": "", "context": ""})
    return normalized

# ------------------------------------------------------------------
# DB
# ------------------------------------------------------------------
def build_keywords(days: int = 3) -> dict:
    """Retourne {date: [TEXT, ...]} pour les `days` derniers jours."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(SQL_GET_EVENTS.format(days=days))
    rows = cur.fetchall()
    results = defaultdict(list)
    result_str = ""

    for i in rows:
        results[i[1]].append(i[0])

    for i in results:
        result_str += f'{i} : {" ".join(results[i])}'
        result_str += "\n" + "-" * 50 + "\n"

    llm_output = _call_llm(result_str)
    keywords = _normalize_keywords(llm_output["keywords"])

    print(keywords)

    if cur and conn:
        params = []
        for kw in keywords:
            params.append(kw["term"])
            params.append(kw["context"])
        cur.execute(SQL_INSERT_KEYWORDS, params)
        conn.commit()

    llm_output["keywords"] = keywords
    return llm_output


if __name__ == "__main__":
    result = build_keywords()