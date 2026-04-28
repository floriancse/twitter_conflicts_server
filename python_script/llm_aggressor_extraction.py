"""
Extraction of (actor, weapon_type, target, objective) quadruplets from conflict summaries via local LLM (Ollama).
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
        T.TWEET_ID,
        T.CREATED_AT::DATE,
        T.TEXT,
        T.NOMINATIM_QUERY,
        ST_X (T.GEOM) AS LON,
        ST_Y (T.GEOM) AS LAT
    FROM
        TWEETS T
        LEFT JOIN MILITARY_ACTIONS MA ON T.TWEET_ID = MA.TWEET_ID
    WHERE
        T.CONFLICT_TYPOLOGY = 'MIL'
        AND T.GEOM IS NOT NULL
        AND T.SUMMARY_TEXT IS NOT NULL
        AND T.CREATED_AT >= NOW() - INTERVAL '24 hours'
        AND MA.TWEET_ID IS NULL
        AND IS_DUPLICATE = 'false'
    ORDER BY
        T.CREATED_AT DESC
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

client = OpenAI(
    base_url="http://localhost:11434/v1",
    api_key="ollama",
)

# Closed list of allowed weapon types
WEAPON_TYPES = [
    "Drones",
    "Missiles",
    "Explosives",
    "Air Defence",
    "Military Aviation",
    "Artillery & Armour",
    "Small Arms"
]

ARMED_GROUPS = [
    'JNIM', 'ISWAP', 'Boko Haram', 'ADF', 'Al-Shabaab',
    'Hezbollah', 'Hamas', 'Wagner', 'Africa Corps',
    'Taliban', 'ISIS', 'Al-Qaeda', 'Houthis', 'PKK',
    'TTP', 'Lashkar-e-Taiba', 'HTS',
]

def build_system_prompt(countries: list[str]) -> str:
    country_list = ", ".join(f'"{c}"' for c in countries)
    weapon_list  = ", ".join(f'"{w}"' for w in WEAPON_TYPES)
    group_list   = ", ".join(f'"{g}"' for g in ARMED_GROUPS)

    return f"""You are an OSINT analyst. Extract exactly one JSON object from the conflict summary.

        Respond ONLY with a raw JSON object — no markdown, no commentary, no extra keys:
        {{"actor": "...", "weapon_type": "...", "target": "...", "objective": "..."}}

        ━━━ RULE 1 — NON-KINETIC EVENTS ━━━
        If no weapon physically impacts a target (deployments, alerts, captures, troop movements,
        negotiations, announcements, flight restrictions without confirmed strike):
        → {{"actor": null, "weapon_type": null, "target": null, "objective": null}}

        ━━━ RULE 2 — TARGET (where does the weapon physically land?) ━━━
        Target = the country whose territory, vessel, or personnel absorbs the impact.

        Soil rule: target = the country whose SOIL the weapon hits, regardless of who owns the
        asset struck. A US base hit in Syria → target = "Syria". An Iranian EW system destroyed
        in Syria → target = "Syria". The nationality of the asset does NOT override the soil rule.

        Territory overrides (occupied land treated as the occupying power):
        - Crimea, Donetsk, Luhansk, Zaporizhzhia, Kherson → target = "Russia"
        - Oryol, Belgorod, Kursk, Bryansk → target = "Russia"
        - Occupied Luhansk, occupied Donetsk → target = "Russia"

        Vessel rule:
        - Military vessel → target = country of that vessel (flag state)
        - Commercial/cargo vessel → target = flag state only if in the allowed countries list, else null
        - "Shadow fleet" tankers without clear flag → target = null

        Multi-target rule: when a single event clearly strikes multiple countries simultaneously,
        pick the FIRST country mentioned or the one with the most significant damage described.
        Do not return a list; return a single target.

        Interception rule (CRITICAL — read carefully):
        When a weapon is SHOT DOWN, INTERCEPTED, or DESTROYED in flight:
        - actor = country/group that OWNS the weapon being destroyed
        - target = country where the weapon was destroyed (the airspace/soil it fell on)
        - The defending force is NOT the actor.
        Example: "US MQ-9 shot down an Iranian drone over Iran" → actor = "Iran" (Iran's drone was
        destroyed), target = "Iran" (it fell on Iranian soil). Wait — the US fired the weapon, so
        actor = "United States of America", target = "Iran" (where the Iranian drone was destroyed).
        Example: "Ukrainian crew shot down a Russian Shahed over Odesa" → actor = "Russia" (Russian
        weapon), target = "Ukraine" (it fell on Ukrainian soil). The Ukrainian crew are defenders,
        not actors.
        Example: "Russian pilot downed two Iranian Shaheds" → actor = null (Russia defending, not
        listed as aggressor), target = "Iran" (Iranian weapon destroyed).
        Example: "Iranian defenses shot down a US JASSM over Markazi" → actor = "United States of
        America" (US weapon), target = "Iran" (where it fell).

        Friendly fire rule:
        A weapon that malfunctions and hits its own side → actor = that country, target = that country.

        ━━━ RULE 3 — ACTOR (who physically fires or operates the weapon?) ━━━
        Actor = the entity whose forces physically launch, fire, or pilot the weapon.
        NOT the defending force. NOT the country providing the weapon.

        State militaries → exact country name from the allowed list.
        Armed groups → exact name from the armed groups list below. Do NOT return null when a named
        group is clearly identified as the one firing.
        Unknown / passive voice / "was struck" / unnamed attacker → actor = null.
        Proxy / unnamed militias ("pro-Iranian groups", "Iran-backed militias", unnamed brigades) → actor = null.

        Common triggers:
        - "Hezbollah fired / launched / struck" → actor = "Hezbollah"
        - "TTP ambushed / attacked" → actor = "TTP"
        - "JNIM placed IED / struck" → actor = "JNIM"
        - "ADF attacked / killed" → actor = "ADF"
        - "Houthis launched / fired" → actor = "Houthis"
        - "RSF attacked" → actor = null (RSF not in the armed groups list)
        - "Guardians of the Blood Brigades attacked" → actor = null (not in the list)
        - "Baloch Liberation Army attacked" → actor = null (not in the list)

        ━━━ RULE 4 — WEAPON_TYPE ━━━
        Pick the single best match:
        {weapon_list}

        ━━━ RULE 5 — OBJECTIVE ━━━
        2–6 words max. The physical thing struck (e.g. "oil terminal", "radar system").
        Return null if nothing specific is mentioned.

        ━━━ RULE 6 — UNCERTAINTY ━━━
        - "Reportedly", "allegedly", "suspected", "possibly", "claimed" → still extract.
        - Fully unconfirmed / pure rumor → all null.

        ━━━ VALID VALUES ━━━
        actor and target → exactly one value from the lists below, or null:
        Countries: {country_list}
        Armed groups (actor only): {group_list}
        weapon_type → exactly one of: {weapon_list}

        ━━━ EXAMPLES ━━━

        "Guardians of the Blood Brigades attacked Qasrak US military base in Hasakah, Syria using Shahed-101 drones."
        → {{"actor": null, "weapon_type": "Drone", "target": "Syria", "objective": "military base"}}

        "IDF destroyed an Iranian Cobra V8 EW system and anti-aircraft guns in Syria."
        → {{"actor": "Israel", "weapon_type": "Bombing / airstrike", "target": "Syria", "objective": "electronic warfare system"}}

        "IRGC launched Arash-2 drones and ballistic missiles against a vessel and US bases in UAE and Kuwait."
        → {{"actor": "Iran", "weapon_type": "Drone", "target": "United Arab Emirates", "objective": "military base"}}

        "Ukrainian crew shot down a Russian Shahed-136 over Odesa with automatic weapons."
        → {{"actor": "Russia", "weapon_type": "Drone", "target": "Ukraine", "objective": "drone"}}

        "Russian pilot from BULAVA unit used a STING interceptor drone to down two Iranian-made Shaheds."
        → {{"actor": null, "weapon_type": "Drone", "target": "Iran", "objective": null}}

        "US MQ-9 Reaper shot down an Iranian Mohajer-6 drone over Iran."
        → {{"actor": "United States of America", "weapon_type": "Drone", "target": "Iran", "objective": "Mohajer-6 drone"}}

        "Iranian defenses intercepted a US AGM-158 JASSM cruise missile over Markazi Province."
        → {{"actor": "United States of America", "weapon_type": "Ballistic missile", "target": "Iran", "objective": null}}

        "Hezbollah FPV drones struck two IDF Merkava tanks in Southern Lebanon."
        → {{"actor": "Hezbollah", "weapon_type": "Drone", "target": "Lebanon", "objective": "armored vehicles"}}

        "Hezbollah carried out artillery attacks against IDF troops in Khiam using a KS-19 and D-30 howitzer."
        → {{"actor": "Hezbollah", "weapon_type": "Gunfire / small arms", "target": "Lebanon", "objective": "IDF troops"}}

        "Hezbollah missile struck a British warship 70 miles off Lebanon's coast."
        → {{"actor": "Hezbollah", "weapon_type": "Ballistic missile", "target": "United Kingdom", "objective": "warship"}}

        "A malfunctioning Bahraini Patriot interceptor struck the BAPCO oil facility in Sitra, Bahrain."
        → {{"actor": "Bahrain", "weapon_type": "Ballistic missile", "target": "Bahrain", "objective": "oil facility"}}

        "TTP ambushed Pakistani Army soldiers in North Waziristan using a PKM machine gun."
        → {{"actor": "TTP", "weapon_type": "Gunfire / small arms", "target": "Pakistan", "objective": "military patrol"}}

        "JNIM targeted a Malian army vehicle with an IED near Tonka."
        → {{"actor": "JNIM", "weapon_type": "Mine", "target": "Mali", "objective": "army vehicle"}}

        "Ukrainian drones sank a Russian cargo ship in the Sea of Azov."
        → {{"actor": "Ukraine", "weapon_type": "Drone", "target": "Russia", "objective": "cargo ship"}}

        "Pro-Iranian armed groups attacked US diplomatic sites in Baghdad."
        → {{"actor": null, "weapon_type": "Unidentified weapon", "target": "Iraq", "objective": "diplomatic facilities"}}

        "Ukrainian air defenses shot down 260 of 286 Russian drones overnight."
        → {{"actor": null, "weapon_type": null, "target": null, "objective": null}}
        """

def fetch_aggressor_data(cur):
    """Fetches recent military tweets, the country/capital dictionary, and already processed actions."""
    cur.execute(SQL_GET_MIL_TWEETS)
    tweets = cur.fetchall()

    cur.execute(SQL_GET_CAPITALS)
    country_dict = {
        row[0]: [row[1], (row[2], row[3])]
        for row in cur.fetchall()
    }

    return tweets, country_dict


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


def sanitize_weapon_type(value: str | None) -> str | None:
    """
    Validates the weapon_type returned by the LLM against the closed list.
    Falls back to 'Unidentified weapon' if the value is not in the allowed list.
    Returns null only if the LLM explicitly returned null/None.
    """
    if value is None:
        return None
    normalized = value.strip().lower()
    for allowed in WEAPON_TYPES:
        if normalized == allowed.lower():
            return allowed  # return canonical casing
    # Value was non-null but not in list → treat as Unidentified weapon
    return "Unidentified weapon"


def extract_quadruplet(summary: str, countries: list[str]) -> dict | None:
    """Sends a summary to the LLM and returns the extracted (actor, weapon_type, target, objective) quadruplet."""
    try:
        
        response = client.chat.completions.create(
            model="mistral-small:24b",
            messages=[
                {"role": "system", "content": build_system_prompt(countries)},
                {"role": "user", "content": summary},
            ],
            response_format={"type": "json_object"},
            temperature=0.0,
            max_tokens=512,
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
        tweets, country_dict = fetch_aggressor_data(cur)
        countries = list(country_dict.keys())

        for row in tweets:
            tweet_id, date, summary, loc_name, lon_tweet, lat_tweet = row
            result = extract_quadruplet(summary, countries)
            
            if not result:
                continue

            aggressor   = keep_first_entity(result.get("actor"))
            weapon_type = sanitize_weapon_type(result.get("weapon_type"))
            target      = keep_first_entity(result.get("target"))
            objective   = result.get("objective")

            if weapon_type is None and target is None:
                continue

            aggressor_coords = country_dict.get(aggressor)
            target_coords    = country_dict.get(target)

            aggressor_geom = None
            if aggressor_coords:
                lon, lat = aggressor_coords[1]
                aggressor_geom = f"SRID=4326;POINT({lon} {lat})"

            target_geom = f"SRID=4326;POINT({lon_tweet} {lat_tweet})"

            print(f"{aggressor} --[{weapon_type}]--> {target} | {objective}")

            try:
                cur.execute(
                    """
                    INSERT INTO MILITARY_ACTIONS
                        (TWEET_ID, AGGRESSOR, TARGET, WEAPON_TYPE, OBJECTIVE, AGGRESSOR_GEOM, TARGET_GEOM)
                    VALUES
                        (%s, %s, %s, %s, %s, ST_GeomFromEWKT(%s), ST_GeomFromEWKT(%s))
                    ON CONFLICT (TWEET_ID) DO UPDATE SET
                        AGGRESSOR      = EXCLUDED.AGGRESSOR,
                        TARGET         = EXCLUDED.TARGET,
                        WEAPON_TYPE    = EXCLUDED.WEAPON_TYPE,
                        OBJECTIVE      = EXCLUDED.OBJECTIVE,
                        AGGRESSOR_GEOM = EXCLUDED.AGGRESSOR_GEOM,
                        TARGET_GEOM    = EXCLUDED.TARGET_GEOM
                    """,
                    (tweet_id, aggressor, target, weapon_type, objective,
                     aggressor_geom, target_geom),
                )
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"  Insert error for tweet {tweet_id}: {e}")

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    generate_aggressor()