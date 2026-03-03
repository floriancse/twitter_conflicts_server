"""
Extraction des triplets (agresseur, action, cible) depuis des résumés de conflits via LLM local (Ollama).
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
    Construit le prompt système en injectant la liste de pays depuis la DB.
    La liste est mise à jour automatiquement à chaque exécution.
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
    """Récupère les tweets militaires récents, le dictionnaire pays/capitales et les actions déjà traitées."""
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
    Normalise la réponse du LLM en ne conservant qu'une seule entité.
    Gère les cas où le LLM retourne une liste ou une chaîne avec virgules.
    """
    if not valeur:
        return None
    if isinstance(valeur, list):
        return valeur[0].strip() if valeur else None
    if "," in valeur:
        return valeur.split(",")[0].strip()
    return valeur.strip()


def extract_triplet(summary_text: str, countries: list[str]) -> dict | None:
    """Envoie un résumé au LLM et retourne le triplet (actor, action, target) extrait."""
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
        print(f"Erreur extraction LLM : {e}")
        return None


def generate_aggressor(cur, conn):
    """
    Parcourt les tweets militaires récents, extrait le triplet agresseur/action/cible
    via LLM et insère les résultats dans MILITARY_ACTIONS.
    Les tweets déjà traités sont ignorés.
    """
    tweets, country_dict, already_processed = fetch_aggressor_data(cur)
    countries = list(country_dict.keys())

    for row in tweets:
        tweet_id, date, summary, loc_name, lon_tweet, lat_tweet = row

        # Ignore les tweets déjà présents dans MILITARY_ACTIONS
        if tweet_id in already_processed:
            continue

        resultat = extract_triplet(summary, countries)

        if not resultat:
            continue

        aggressor = keep_first_entity(resultat.get("actor"))
        action    = resultat.get("action")
        target    = keep_first_entity(resultat.get("target"))

        # On n'insère que si l'agresseur est un pays reconnu dans notre référentiel
        if not country_dict.get(aggressor):
            continue

        aggressor_coords = country_dict.get(aggressor)
        target_coords    = country_dict.get(target)

        aggressor_geom = None
        target_geom    = None

        if aggressor_coords:
            lon, lat = aggressor_coords[1]
            aggressor_geom = f"SRID=4326;POINT({lon} {lat})"

        # La géométrie cible utilise les coordonnées du tweet (lieu de l'événement)
        if target_coords:
            target_geom = f"SRID=4326;POINT({lon_tweet} {lat_tweet})"

        print(f"{aggressor} {aggressor_coords}  --[{action}]--> {target} {target_coords}")

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
            print(f"Erreur insert tweet {tweet_id} : {e}")