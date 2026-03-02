"""
Extraction (aggressor, action, target) via LLM local (Ollama)
Basé sur le module existant d'extraction géopolitique.
"""

import json
from openai import OpenAI
import psycopg2

SQL_GET_MIL_TWEETS = """
    SELECT tweet_id,
           created_at::DATE,
           summary_text,
           location_name,
           ST_X(geom) AS lon,
           ST_Y(geom) AS lat
    FROM   tweets
    WHERE  conflict_typology = 'MIL'
      AND  geom IS NOT NULL
      AND  summary_text IS NOT NULL
      AND  created_at >= NOW() - INTERVAL '24 hours'
    ORDER BY created_at DESC
"""

SQL_GET_CAPITALS = """
    SELECT a.entity_name,
           c.name,
           ST_X(c.geom) AS lon,
           ST_Y(c.geom) AS lat
    FROM   world_areas   a
    LEFT JOIN world_capitals c ON ST_Intersects(a.geom, c.geom)
    WHERE  c.geom IS NOT NULL
"""

SQL_GET_PROCESSED_ACTIONS = "SELECT tweet_id FROM MILITARY_ACTIONS"

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

def fetch_aggressor_data(cur):
    """Récupère toutes les données nécessaires à l'extraction des aggresseurs"""
    cur.execute(SQL_GET_MIL_TWEETS)
    tweets = cur.fetchall()

    cur.execute(SQL_GET_CAPITALS)
    country_dict = {
        row[0]: [row[1], (row[2], row[3])]
        for row in cur.fetchall()
    }

    cur.execute(SQL_GET_PROCESSED_ACTIONS)
    already_processed = {row[0] for row in cur.fetchall()}

    return tweets, country_dict, already_processed


def keep_first_entity(valeur) -> str | None:
    """
    Force un seul aggressor/target, même si le LLM en renvoie plusieurs.
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


def extract_triplet(summary_text: str, countries: list[str]) -> dict | None:
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


def generate_aggressor(cur, conn):
    tweets, country_dict, already_processed = fetch_aggressor_data(cur)
    countries = list(country_dict.keys())
    for row in tweets :
        tweet_id, date, summary, loc_name, lon_tweet, lat_tweet  = row
        
        if tweet_id in already_processed:
            print(tweet_id)
            continue
        
        resultat = extract_triplet(summary, countries)

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
                    aggressor_geom = f"SRID=4326;POINT({lon} {lat})"

                target_coords = country_dict.get(target)
                if target_coords:
                    target_geom = f"SRID=4326;POINT({lon_tweet} {lat_tweet})"

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
                except Exception as e:
                    conn.rollback()
                    print(f"Erreur insert {tweet_id} : {e}")
