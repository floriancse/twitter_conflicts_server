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
    AND FK_TOPIC IN (2, 5, 6, 1)
"""

SQL_INSERT_KEYWORDS = """
INSERT INTO
	KW_TENDANCIES (CREATED_AT, KW1, KW2, KW3, KW4, KW5)
VALUES
	(CURRENT_DATE, %s, %s, %s, %s, %s)
ON CONFLICT (CREATED_AT) 
DO UPDATE SET
    KW1 = EXCLUDED.KW1,
    KW2 = EXCLUDED.KW2,
    KW3 = EXCLUDED.KW3,
    KW4 = EXCLUDED.KW4,
    KW5 = EXCLUDED.KW5
"""

def build_system_prompt() -> str:
    return """You are a precise OSINT data analyst.
Analyze the provided text to identify the top 5 emerging trends or patterns.

CRITICAL RULES:
1. Extract EXACTLY 5 keywords or keyphrases.
2. Every keyword's root word(s) MUST come from the input text (see normalization rule below for how to trim it).
3. DO NOT include weapons or armament terms (exclude "missile", "drone", "FAB-500", "UAV", "FPV", "artillery").

4. NORMALIZATION RULE — CHOOSE ONE FORM, NEVER COMBINE:
   For each entity, decide whether it has a distinctive PROPER NAME (a brand,
   company, or place name) or only a GENERIC TYPE (a facility category with
   no distinctive name).

   - IF the entity has a proper name (brand/company name) → output ONLY that
     proper name, alone. Strip any generic descriptor attached to it
     (facility type, "plant", "center", "hypermarket", "refinery", "complex", etc.).
       "Ozon logistics center"           -> "Ozon"
       "Afipsky oil refinery"            -> "Afipsky"
       "Epicentr hypermarket"            -> "Epicentr"
       "Amur gas chemical complex"       -> "Amur"

   - IF the entity has NO distinctive brand name and is only identified by a
     PLACE NAME + generic facility type (e.g. "Astrakhan Gas Processing Plant",
     "Kirov substation") → output ONLY the generic facility type, 1-2 words,
     with no location attached.
       "Astrakhan Gas Processing Plant"  -> "Gas processing plant"
       "Kirov substation"                -> "Substation"
       "the regional airbase"            -> "Airbase"
       "an unnamed oil depot"            -> "Oil depot"

   - NEVER output "ProperName + generic type" together
     (e.g. "Ozon logistics center" is FORBIDDEN — output "Ozon" only).
   - NEVER output "PlaceName + generic type" together
     (e.g. "Astrakhan gas processing plant" is FORBIDDEN — output
     "Gas processing plant" only; the place name alone, without the brand
     it belongs to, is not a useful keyword for filtering).
   - When in doubt whether a name is a brand or just a place, prefer the
     generic type — it groups more events and avoids false precision.

5. Focus on targeted assets, non-military entities, locations, or impact mechanisms.

EXAMPLES (input mention -> correct keyword):
- "the Ozon logistics center was struck"        -> "Ozon"
- "Afipsky oil refinery caught fire"            -> "Afipsky"
- "Epicentr hypermarket damaged"                -> "Epicentr"
- "Astrakhan Gas Processing Plant hit by drone" -> "Gas processing plant"
- "a substation near Kirov was hit"             -> "Substation"
- "Amur gas chemical complex reported damage"   -> "Amur"

Output JSON format strictly:
{
  "keywords": [
    "exact_term_1",
    "exact_term_2",
    "exact_term_3",
    "exact_term_4",
    "exact_term_5"
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
    keywords = llm_output["keywords"]

    for i in range(5):
        keywords[i] = keywords[i].capitalize()

    print(keywords)
    if cur and conn:
        cur.execute(SQL_INSERT_KEYWORDS, (keywords[0], keywords[1], keywords[2], keywords[3], keywords[4]))
        conn.commit()

    return llm_output


if __name__ == "__main__":
    result = build_keywords()