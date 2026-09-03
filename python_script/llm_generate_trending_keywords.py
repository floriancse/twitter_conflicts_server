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
    AND IMPORTANCE_SCORE >= 3
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
     proper name, alone. Strip any generic descriptor attached to it.
   - NEVER output "ProperName + generic type" together
     (e.g. "Ozon logistics center" is FORBIDDEN — output "Ozon" only).
   - NEVER output "PlaceName + generic type" together.
   - When in doubt whether a name is a brand or just a place, prefer the
     generic type — it groups more events and avoids false precision.

5. CATEGORY DIVERSITY (mandatory):
   Classify each candidate entity into exactly ONE category:
   - MILITARY (strikes, front-line movements, attacks, military bases or
     units targeted — the event/action itself, not the weapon used)
   - LOGISTICS (warehouses, retailers, distribution centers)
   - ENERGY (refineries, pipelines, power grid)
   - GOVERNANCE/POLITICAL (coups, elections, leadership changes, unrest)
   - INFRASTRUCTURE (bases, airfields, ports, transport — non-weapon)
   - ECONOMIC/FINANCIAL (sanctions, currency, trade routes)
   - CYBER (breaches, outages, digital infrastructure)
   - SOCIAL/CIVIL (protests, humanitarian crises, displacement)

   The final 5 entities MUST span AT LEAST 3 different categories.
   MAXIMUM 2 entities from the same category.
   If the source text is dominated by one category, still select the
   single strongest signal from that category, then actively search the
   rest of the text for the strongest entity in an under-represented
   category — do not default to the next-best entity in the same
   category just because it scored higher in isolation.

   MILITARY / POLITICAL BALANCE (mandatory):
   If the source text contains signal for BOTH military events (strikes,
   combat, front-line activity) AND political/governance events (coups,
   elections, leadership changes, unrest), the final 5 MUST include AT
   LEAST ONE entity from MILITARY and AT LEAST ONE entity from
   GOVERNANCE/POLITICAL. Do not let one type dominate all 5 slots just
   because it has more raw mentions in the source text — actively search
   for the strongest signal in the under-represented type before finalizing.

6. GEOGRAPHIC DIVERSITY:
   If the input text covers multiple countries/conflicts/regions, the top 5
   must reflect at least 2 distinct geographic contexts, unless the text
   genuinely contains signal for only one. MAXIMUM 3 entities from the same
   country/conflict. Do not let sheer volume of mentions dictate selection —
   an entity with fewer but more novel mentions in an under-represented
   region may be more "emerging" than a saturated one elsewhere.

7. SELECTION PROCESS (internal, do not output):
   Before producing the final JSON, internally draft up to 10 candidate
   entities spanning as many different categories and regions as the text
   supports, with a one-line justification for each. Then apply rules 5 and
   6 to select the final 5. Do NOT show this draft list — output ONLY the
   final JSON.

8. Focus "term" on targeted assets, non-military entities, locations, or
   impact mechanisms. Output the most important keywords first, but
   "most important" must be evaluated AFTER applying the diversity
   constraints above, not before.

9. CRITICAL SELF-CHECK BEFORE OUTPUT:
   For each keyword object, the "context" string MUST contain the exact
   "term" string (case-insensitive substring match), OR a direct
   morphological variant of it (e.g. plural, adjective form). If it does
   not, rewrite the context until it does. This is a hard requirement — an
   object where "context" does not contain "term" is INVALID output.

   Example of INVALID output (term missing from context):
   {"term": "Ozon", "context": "Warehouse struck near Moscow"}   ✗ "Ozon" absent

   Example of VALID output:
   {"term": "Ozon", "context": "Ozon warehouse struck near Moscow"}   ✓

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
def build_keywords(days: int = 2) -> dict:
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