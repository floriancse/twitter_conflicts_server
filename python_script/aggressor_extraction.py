"""
Extraction (aggressor, action, target) via LLM local (Ollama)
Basé sur le module existant d'extraction géopolitique.
"""

import json
from openai import OpenAI
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "twitter_conflicts"),
    "user": os.getenv("DB_USER", "tw_user"),
    "password": os.getenv("DB_PASSWORD"),
    "sslmode": os.getenv("DB_SSLMODE", "disable"),
}


def get_db_connection():
    """Établit et retourne une connexion à la base de données PostgreSQL"""
    conn = psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )
    return conn


conn = get_db_connection()
cur = conn.cursor()

client = OpenAI(
    base_url="http://localhost:11434/v1",
    api_key="ollama",
)


def build_prompt(countries: list[str]) -> str:
    """
    Construit le prompt en injectant la liste de pays dynamiquement.
    Si ta liste change en DB, le prompt se met à jour automatiquement.
    """
    liste_formatee = ", ".join(f'"{p}"' for p in countries)

    return f"""You are an OSINT analyst. Respond ONLY in JSON.

        From a conflict summary, extract WHO did WHAT to WHOM.

        IMPORTANT: actor and target MUST be chosen EXACTLY (copy-paste) from this list, in French:
        {liste_formatee}

        Rules:
        - actor: copy the EXACT string from the list above, character for character.
        - target: copy the EXACT string from the list above, character for character. 
        - target: pick the closest match from the list. Never use a city or military unit.
            If the target is a city, use its country instead (e.g. "Tel Aviv" → "Israël", "Manama" → "Bahreïn", "Moscow" → "Russie").
            If the target is a military base or installation, use the country it's located in.
        - If the sentence is passive ("base was struck by Iran"), still put Iran as actor.
        - If two actors act jointly ("Israel and US struck Iran"), pick the most prominent one (the one named first or most active).
        - If multiple targets, pick the primary one (the one directly struck, not secondary).
        - If no match found in the list, use null.

        Output format (strict JSON, no markdown):
        {{
        "actor": "string | array | null",
        "action": "string or null",
        "target": "string | array | null"
        }}"""


def keep_first_entity(valeur) -> str | None:
    """
    Force un seul aggressor/target, même si le LLM en renvoie plusieurs.
      ["Israël", "États-Unis"]  ->  "Israël"
      "États-Unis, Israël"      ->  "États-Unis"
      "Iran"                    ->  "Iran"
    """
    if not valeur:
        return None
    if isinstance(valeur, list):
        return valeur[0].strip() if valeur else None
    if "," in valeur:
        return valeur.split(",")[0].strip()
    return valeur.strip()


def extract_triplet(summary_text: str) -> dict | None:
    try:
        response = client.chat.completions.create(
            model="mistral-small:24b",
            messages=[
                {"role": "system", "content": build_prompt(countries)},
                {"role": "user", "content": summary_text},
            ],
            response_format={"type": "json_object"},
            temperature=0.0,
            max_tokens=128,
        )

        raw = response.choices[0].message.content.strip()
        return json.loads(raw)

    except Exception as e:
        print(f"Erreur : {e}")
        return None


SQL_COUNTRY_NAMES = """
    SELECT
        entity_name
    FROM
        WORLD_AREAS
    """

cur.execute(SQL_COUNTRY_NAMES)
countries = [i[0] for i in cur.fetchall()]

SQL_RECENT_TWEETS = """
    SELECT
        TWEET_ID,
        CREATED_AT::DATE,
        SUMMARY_TEXT,
        LOCATION_NAME,
        ST_Y (GEOM) AS LONG,
        ST_X (GEOM) AS LAT
    FROM
        TWEETS
    WHERE
        CONFLICT_TYPOLOGY = 'MIL'
        AND GEOM IS NOT NULL
        AND SUMMARY_TEXT IS NOT NULL
    ORDER BY
        CREATED_AT DESC
    LIMIT
        20
    """

cur.execute(SQL_RECENT_TWEETS)
tweets = cur.fetchall()

country_dict = {}
SQL_GET_CAPITALS = """
    SELECT
        entity_name,
        NAME,
        ST_Y (C.GEOM) AS LONG,
        ST_X (C.GEOM) AS LAT
    FROM
        WORLD_AREAS A
        LEFT JOIN WORLD_CAPITALS C ON ST_INTERSECTS (A.GEOM, C.GEOM)
    WHERE
        C.GEOM IS NOT NULL
    """
    
cur.execute(SQL_GET_CAPITALS)
rows = cur.fetchall()

for i in rows:
    country_dict[i[0]] = [i[1], (i[2], i[3])]

print("=== EXTRACTION DES TRIPLETS VIA LLM ===\n")

for row in tweets :
    tweet_id, date, summary, loc_name, lon_tweet, lat_tweet = row
    resultat = extract_triplet(summary)

    if resultat:
        aggressor = keep_first_entity(resultat.get("actor"))
        action = resultat.get("action")
        target = keep_first_entity(resultat.get("target"))

        if country_dict.get(aggressor):


            aggressor_coords = country_dict.get(aggressor)
            target_coords = country_dict.get(target)

            aggressor_geom = None
            target_geom = None

            if aggressor_coords:
                lon, lat = aggressor_coords[1]
                aggressor_geom = f"SRID=4326;POINT({lat} {lon})"

            if target_coords:
                lon, lat = target_coords
                target_geom = f"SRID=4326;POINT({lat} {lon})"

            print(
                f"{aggressor} {aggressor_coords}  --[{action}]--> {target} {target_coords}"
            )
            try:
                cur.execute(
                    """
                    INSERT INTO MILITARY_ACTIONS
                        (TWEET_ID, AGGRESSOR, TARGET, ACTION, AGGRESSOR_GEOM, TARGET_GEOM)
                    VALUES
                        (%s, %s, %s, %s, ST_GeomFromEWKT(%s), ST_GeomFromEWKT(%s))
                    ON CONFLICT (TWEET_ID) DO UPDATE SET
                        AGGRESSOR      = EXCLUDED.AGGRESSOR,
                        TARGET         = EXCLUDED.TARGET,
                        ACTION         = EXCLUDED.ACTION,
                        AGGRESSOR_GEOM = EXCLUDED.AGGRESSOR_GEOM,
                        TARGET_GEOM    = EXCLUDED.TARGET_GEOM
                """,
                    (tweet_id, aggressor, target, action, aggressor_geom, target_geom),
                )
                conn.commit()
                print(f"Inséré : {tweet_id}")
            except Exception as e:
                conn.rollback()
                print(f"Erreur insert {tweet_id} : {e}")

cur.close()
conn.close()
