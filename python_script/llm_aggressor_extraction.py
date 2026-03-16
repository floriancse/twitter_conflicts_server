"""
Extraction of (actor, action, target, objective) quadruplets from conflict summaries via local LLM (Ollama).
"""

import json
from openai import OpenAI
import psycopg2
import os
from dotenv import load_dotenv

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

SQL_GET_MIL_TWEETS = """
    SELECT
        TWEET_ID,
        CREATED_AT::DATE,
        SUMMARY_TEXT,
        nominatim_query,
        ST_X (GEOM) AS LON,
        ST_Y (GEOM) AS LAT
    FROM
        TWEETS
    WHERE
        CONFLICT_TYPOLOGY = 'MIL'
        AND GEOM IS NOT NULL
        AND SUMMARY_TEXT IS NOT NULL
        AND CREATED_AT >= NOW() - INTERVAL '24 hours'
    ORDER BY
        CREATED_AT DESC
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
    liste_formatee = ", ".join(f'"{p}"' for p in countries)
    return f"""You are an OSINT analyst. Respond ONLY in JSON.
        From a conflict summary, extract WHO did WHAT to WHOM and WHAT WAS TARGETED.

        IMPORTANT: actor and target MUST be chosen EXACTLY (copy-paste) from this list of countries:
        {liste_formatee}

        ━━━ RULE 1 — ACTOR = the country whose military/personnel performs the action ━━━
        - The actor is determined by WHO operates/controls the forces, NOT by equipment origin.
        - If Ukraine uses French Mirage jets → actor = "Ukraine" (Ukraine operates them)
        - If France uses its own Rafales → actor = "France"
        - Equipment brand, manufacturer, or supplier country is NEVER the actor.
        - Military units, drones, missiles = never actor or target. Always use the controlling country.
        - ONLY extract if the action is carried out by an OFFICIAL state military or government-directed forces.
        - Independent militias, rebel groups, or terrorist organizations acting WITHOUT explicit state command → actor = null.
        - "Hezbollah fires rockets" → actor = null (unless text explicitly states Iranian/Syrian command).
        - "Iran-directed Houthis attack" → actor = "Iran" ONLY if the text explicitly states state command.
        - When in doubt between state vs. non-state actor → prefer null over a wrong attribution.

        ━━━ RULE 1b — EQUIPMENT NATIONALITY AS FALLBACK ACTOR ━━━
        - If the text mentions equipment/weapon NATIONALITY but does NOT explicitly name the operating country,
          infer the actor from WHO POSSESSES that equipment in the conflict context.
        - "Russian 2S3 Akatsiya destroyed by Ukrainian FPV drone" → the FPV drone is operated by Ukraine → actor = "Ukraine"
        - "Ukrainian T-72 destroyed by Russian Kornet" → actor = "Russia" (Russia operates the Kornet)
        - Ask yourself: "Which country fields/uses this weapon in this conflict?" → that country is the actor.
        - Apply this rule ONLY when no explicit actor country is named AND the equipment nationality clearly maps
          to a single country in the conflict.
        - If the equipment could belong to multiple countries → prefer null.

        ━━━ RULE 2 — TARGET = the ENEMY/THREAT physically struck, not the ally being protected ━━━
        - In a DEFENSIVE mission: target = the aggressor/attacker being repelled, NOT the ally being defended.
        - "France defends UAE against Iran" → actor="France", target="Iran"  ✓  (NOT target="UAE")
        - "France intercepts Iranian drones headed to UAE" → actor="France", target="Iran"  ✓
        - "US Patriot batteries protect Poland from Russian missiles" → actor="United States", target="Russia"  ✓
        - Keywords signaling defense: defend, protect, intercept, escort, shield, shoot down, destroy inbound
        → In all these cases, target = the threatening country

        ━━━ RULE 3 — TARGET = THE COUNTRY PHYSICALLY STRUCK, not where consequences are felt ━━━
        - The target MUST be the country on whose territory the strike/attack physically lands.
        - A country treating casualties, providing logistics, or feeling downstream effects is NOT the target.
        - "Iranian strikes hit U.S. bases in Iraq" → target = "Iraq" (Iraq is where the strike lands) ✓
        - "Hospital in Germany treats casualties from Iranian strikes" → target = null (Germany was NOT struck) ✓
        - "Iranian missiles hit U.S. bases in Qatar" → target = "Qatar" ✓
        - If the text ONLY mentions secondary effects in a country (treating wounded, supply strain, evacuations, refugees)
          WITHOUT that country being physically attacked → target = null.
        - Ask yourself: "Was a weapon physically detonated ON this country's territory?" If no → not the target.

        ━━━ RULE 4 — NATIONALITY → COUNTRY MAPPING ━━━
        - Ukrainian, Ukrainians, Ukrainian Air Force → "Ukraine"
        - Russian, Russians → "Russia"
        - Israeli, Israelis → "Israel"
        - Iranian, Iranians → "Iran"
        - British, UK → "United Kingdom"
        - American, US → "United States"
        - French → "France"
        - Emirati, UAE → "United Arab Emirates"
        (apply common sense for any other nationality)

        ━━━ RULE 5 — EDGE CASES ━━━
        - Passive voice: identify the real actor (e.g. "base was struck by Iran" → actor="Iran")
        - Joint action: pick the first/most prominent country named
        - Multiple targets: pick the primary enemy/threat (the one physically struck)
        - No match possible in the list → null
        - Never output a city, military unit, or anything not in the list

        ━━━ RULE 6 — OBJECTIVE = WHAT WAS PHYSICALLY STRUCK OR TARGETED ━━━
        - Describe the physical target of the attack: military base, airfield, power grid,
          naval vessel, convoy, ammunition depot, civilian infrastructure, etc.
        - Be concise, 2-5 words max (e.g. "military airfield", "power grid", "naval base")
        - If the text mentions no specific target → null
        - Never repeat the actor or target country name here

        ━━━ EXAMPLES ━━━
        ✓ "Ukrainian Air Force uses French Mirage 2000 for strikes on Russian positions"
        → actor="Ukraine", action="offensive strikes", target="Russia", objective="military positions"

        ✓ "French Rafales conduct combat missions in UAE to defend against Iranian attacks"
        → actor="France", action="defensive combat missions", target="Iran", objective=null

        ✓ "Hospital in Germany treats casualties from Iranian strikes on U.S. targets in the region"
        → actor="Iran", action=null, target=null, objective=null  (Germany not physically struck)

        ✓ "Hezbollah fires rockets into northern Israel"
        → actor=null, action=null, target=null, objective=null  (non-state actor, no explicit state command)

        ✓ "Russian 2S3 Akatsiya self-propelled gun was destroyed by Ukrainian FPV drone"
        → actor="Ukraine", action="destruction of enemy equipment", target="Russia", objective="self-propelled artillery"

        ✓ "Iranian ballistic missiles strike Israeli airbase in the Negev desert"
        → actor="Iran", action="ballistic missile strike", target="Israel", objective="military airbase"

        Output format (strict JSON, no markdown, no extra text):
        {{
        "actor": "string | null",
        "action": "string | null",
        "target": "string | null",
        "objective": "string | null"
        }}"""

def fetch_aggressor_data(cur):
    """Fetches recent military tweets, the country/capital dictionary, and already processed actions."""
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


def keep_first_entity(value) -> str | None:
    """
    Normalizes the LLM response by keeping only a single entity.
    Handles cases where the LLM returns a list or a comma-separated string.
    """
    if not value:
        return None
    if isinstance(value, list):
        return value[0].strip() if value else None
    if "," in value:
        return value.split(",")[0].strip()
    return value.strip()


def extract_triplet(summary_text: str, countries: list[str]) -> dict | None:
    """Sends a summary to the LLM and returns the extracted (actor, action, target, objective) quadruplet."""
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
        print(f"LLM extraction error: {e}")
        return None


def generate_aggressor():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        tweets, country_dict, already_processed = fetch_aggressor_data(cur)
        countries = list(country_dict.keys())

        for row in tweets:
            tweet_id, date, summary, loc_name, lon_tweet, lat_tweet = row

            if tweet_id in already_processed:
                continue

            result = extract_triplet(summary, countries)
            if not result:
                continue

            aggressor = keep_first_entity(result.get("actor"))
            action    = result.get("action")
            target    = keep_first_entity(result.get("target"))
            objective = result.get("objective")

            if not country_dict.get(aggressor):
                continue

            aggressor_coords = country_dict.get(aggressor)
            target_coords    = country_dict.get(target)

            aggressor_geom = None
            if aggressor_coords:
                lon, lat = aggressor_coords[1]
                aggressor_geom = f"SRID=4326;POINT({lon} {lat})"

            target_geom = f"SRID=4326;POINT({lon_tweet} {lat_tweet})"

            print(f"{aggressor} {aggressor_coords}  --[{action}]--> {target} {target_coords} | objective: {objective}")

            try:
                cur.execute(
                    """
                    INSERT INTO MILITARY_ACTIONS
                        (TWEET_ID, AGGRESSOR, TARGET, ACTION, OBJECTIVE, AGGRESSOR_GEOM, TARGET_GEOM)
                    VALUES
                        (%s, %s, %s, %s, %s, ST_GeomFromEWKT(%s), ST_GeomFromEWKT(%s))
                    ON CONFLICT (TWEET_ID) DO UPDATE SET
                        AGGRESSOR      = EXCLUDED.AGGRESSOR,
                        TARGET         = EXCLUDED.TARGET,
                        ACTION         = EXCLUDED.ACTION,
                        OBJECTIVE      = EXCLUDED.OBJECTIVE,
                        AGGRESSOR_GEOM = EXCLUDED.AGGRESSOR_GEOM,
                        TARGET_GEOM    = EXCLUDED.TARGET_GEOM
                    """,
                    (tweet_id, aggressor, target, action, objective, aggressor_geom, target_geom),
                )
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"Insert error for tweet {tweet_id}: {e}")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    generate_aggressor()