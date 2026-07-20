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
    "Naval Weapons"
]

# Closed list of allowed objective (target) typologies
OBJECTIVE_TYPES = [
    "Military",
    "Industrial",
    "Energy",
    "Transport & Infrastructure",
    "Communications",
    #"Residential",
    "Civilian",
    "Government & Institutional",
    #"Healthcare",
    #"Education",
    #"Humanitarian",
    #"Leisure & Hospitality",
    "Unidentified/Other",
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
    objtype_list = ", ".join(f'"{o}"' for o in OBJECTIVE_TYPES)

    return f"""You are an OSINT analyst. Extract exactly one JSON object from the conflict summary.

    Respond ONLY with a raw JSON object — no markdown, no commentary, no extra keys:
    {{"actor": "...", "weapon_type": "...", "target": "...", "objective": "...", "objective_type": "..."}}

    ━━━ RULE 1 — NON-KINETIC EVENTS ━━━
    No confirmed physical impact (deployments, alerts, captures, troop movements, negotiations,
    announcements, flight restrictions, overflight/in-transit sightings with no reported hit,
    raw drone/missile counts with no reported impact) → all fields null, including objective_type.

    ━━━ RULE 2 — ACTOR (who physically fires/operates the weapon?) ━━━
    NOT the defending force. NOT the country supplying the weapon.
    - State militaries → exact country name from the countries list.
    - Named armed groups → exact name from the armed groups list. Never null when explicitly named.
    - Unknown/unnamed attacker, passive voice ("was struck"), unnamed proxy/militia ("pro-Iranian
      groups") → actor = null.
    - "USF" = Unmanned Systems Forces of Ukraine → Ukraine. "IRGC" → Iran. Wagner/Africa Corps → Russia.

    ━━━ RULE 3 — TARGET (whose territory absorbs the impact?) ━━━
    Soil rule: target = country whose SOIL is hit, regardless of asset ownership (a US base hit
    in Iraq → target = "Iraq").
    Occupied territory (Crimea, Donetsk, Luhansk, Zaporizhzhia, Kherson oblasts; Oryol, Belgorod,
    Kursk, Bryansk) → target = "Russia". Government-controlled Ukraine → target = "Ukraine".
    Vessel: military vessel → flag state/operator; commercial vessel → flag state if in countries
    list, else null; unclear "shadow fleet" tankers → null.
    Multi-target: pick the country with the most significant damage. Single string, never a list.

    ━━━ RULE 4 — INTERCEPTION (read carefully) ━━━
    When a weapon is SHOT DOWN/INTERCEPTED/DESTROYED in flight: actor = who LAUNCHED it,
    target = where it fell. The intercepting/defending force is NEVER the actor.
    Example: "Ukrainian crew shot down a Russian Shahed over Odesa" → actor = "Russia"
    (launched it), target = "Ukraine" (fell there) — even though Ukraine did the shooting.
    Friendly fire: a weapon that malfunctions and hits its own side → actor = target = that country.

    ━━━ RULE 5 — WEAPON_TYPE ━━━
    Pick the single closest match from this list: {weapon_list}
    - Drones: FPV, loitering munitions (Shahed, Geran, Lancet), USVs, UAVs.
    - Missiles: cruise/ballistic/guided missiles (Kh-series, JASSM, LMUR), anti-tank missiles.
    - Explosives: IEDs, rockets (107mm, Grad), RPGs, MANPADS, grenades, mortars, suicide vests.
    - Air Defence: ground-based SAM (Tor, Patriot, Buk) as the weapon destroying a target — not
      when it is the target being hit.
    - Military Aviation: fixed-wing airstrikes, helicopter gunship attacks.
    - Artillery & Armour: howitzers, tank cannons, self-propelled guns, field artillery.
    - Small Arms: rifles, machine guns, sniper fire, handguns.
    Unclear but aerial → "Drones"; unclear otherwise → "Explosives"; unidentifiable → null.

    ━━━ RULE 6 — OBJECTIVE (what physical asset was struck?) ━━━
    2–6 words, specific when possible ("oil refinery", not "infrastructure"/"facility"). Null if
    no specific target is mentioned.

    ━━━ RULE 6bis — OBJECTIVE_TYPE ━━━
    Exactly one category from: {objtype_list}
    - Military: barracks, checkpoints, army positions, armored vehicles, artillery, command posts,
      military bases/aircraft/ships/vehicles, weapon/ammo depots, fortifications, personnel.
    - Industrial: refineries, factories, defense-industrial/semiconductor plants.
    - Energy: substations, power plants, grid/pylons, oil storage/terminals (not refining).
    - Transport & Infrastructure: bridges, roads, railways, ports, airfields/airports, logistics hubs.
    - Communications: comms/telecom hubs, radio/radar installations (the installation itself, not
      a weapon system).
    - Civilian: any clearly civilian target — houses, apartments, residential areas/neighborhoods
      (even generic, no proper name needed); hospitals, clinics, schools, universities; hotels,
      sports clubs, restaurants, entertainment venues; humanitarian sites (Red Cross, aid depots);
      or a specific named civilian object (car, minibus, gas station, street, market).
    - Government & Institutional: government buildings, administrative offices, border posts.
    - Unidentified/Other: objective is null, too vague (bare city/region/"urban area" with no
      indication of what was hit), or fits no category above. Do not default to "Civilian" for
      vague locations — only use it when the text clearly signals civilian character.
    If objective is null, objective_type must also be null.

    ━━━ RULE 7 — UNCERTAINTY ━━━
    "Reportedly/allegedly/suspected/possibly/claimed" → still extract. Pure rumor, no detail → all null.

    ━━━ VALID VALUES ━━━
    actor → country from list OR armed group from list, or null.
    target → country from list, or null.
    weapon_type / objective_type → exactly one value from their list, or null.
    Countries: {country_list}
    Armed groups (actor only): {group_list}

    ━━━ EXAMPLES ━━━

    # 1. Standard strike
    "Ukrainian attack drones struck an oil pipeline pumping station in Perm, Russia, setting it ablaze."
    → {{"actor": "Ukraine", "weapon_type": "Drones", "target": "Russia", "objective": "oil pumping station", "objective_type": "Energy"}}

    # 2. Occupied territory → target = "Russia"
    "Ukrainian FP-2 drones destroyed both transformers at the 220 kV substation in Alchevsk, Luhansk region."
    → {{"actor": "Ukraine", "weapon_type": "Drones", "target": "Russia", "objective": "power substation transformers", "objective_type": "Energy"}}

    # 3. Interception — defending force is NOT the actor
    "Ukrainian soldier fired a MANPADS from the street in Dnipro, intercepting a Russian UAV."
    → {{"actor": "Russia", "weapon_type": "Drones", "target": "Ukraine", "objective": "UAV", "objective_type": "Military"}}

    # 4. Named armed group
    "JNIM/FLA coalition attacked the city of Gourma-Rharous and its military camp in Mali."
    → {{"actor": "JNIM", "weapon_type": "Small Arms", "target": "Mali", "objective": "military camp", "objective_type": "Military"}}

    # 5. Actor = null — unlisted group
    "Islamic Resistance FPV drone armed with a PG-7VR tandem-HEAT warhead struck a communication tower at the US Victoria Base in Baghdad."
    → {{"actor": null, "weapon_type": "Drones", "target": "Iraq", "objective": "communication tower", "objective_type": "Communications"}}

    # 6. Actor = null — unknown attacker, vague target → "Unidentified/Other"
    "An unknown group used 107mm Type 63 rockets to strike the city of Quetta in Balochistan, Pakistan."
    → {{"actor": null, "weapon_type": "Explosives", "target": "Pakistan", "objective": "urban area", "objective_type": "Unidentified/Other"}}

    # 7. Specific named civilian object → "Civilian"
    "An unknown group used 107mm Type 63 rockets to destroy a passenger minibus on Airport Road in Quetta, Pakistan."
    → {{"actor": null, "weapon_type": "Explosives", "target": "Pakistan", "objective": "passenger minibus", "objective_type": "Civilian"}}

    # 8. Generic but explicitly civilian-populated area → "Civilian" (no proper name needed)
    "A Russian Pantsir SAM system accidentally fired 30 mm cannon rounds into a residential neighborhood in Afipsky, Russia."
    → {{"actor": "Russia", "weapon_type": "Air Defence", "target": "Russia", "objective": "residential neighborhood", "objective_type": "Civilian"}}

    # 9. Overflight / in-progress, no confirmed impact → all null
    "A Ukrainian Flamingo cruise missile was spotted flying over Russia's Chuvash Republic, over 500 miles from the border."
    → {{"actor": null, "weapon_type": null, "target": null, "objective": null, "objective_type": null}}

    # 10. Friendly fire / accident
    "A Ukrainian military pick-up carrying ammunition detonated in Kharkiv, shattering windows in surrounding buildings."
    → {{"actor": "Ukraine", "weapon_type": "Explosives", "target": "Ukraine", "objective": "ammunition vehicle", "objective_type": "Military"}}

    # 11. Non-kinetic event → all null
    "Air raid sirens sounded across Kiryat Shmona, Israel, over a suspected drone attack from Lebanon."
    → {{"actor": null, "weapon_type": null, "target": null, "objective": null, "objective_type": null}}
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


def sanitize_objective_type(value: str | None) -> str | None:
    """
    Validates the objective_type returned by the LLM against the closed OBJECTIVE_TYPES list.
    Falls back to 'Unidentified/Other' if the value is not in the allowed list.
    Returns null only if the LLM explicitly returned null/None.
    """
    if value is None:
        return None
    normalized = value.strip().lower()
    for allowed in OBJECTIVE_TYPES:
        if normalized == allowed.lower():
            return allowed  # return canonical casing
    # Value was non-null but not in list → treat as Unidentified/Other
    return "Unidentified/Other"


def extract_quadruplet(summary: str, countries: list[str]) -> dict | None:
    """Sends a summary to the LLM and returns the extracted (actor, weapon_type, target, objective) quadruplet."""
    try:
        
        response = client.chat.completions.create(
            model="qwen36-fixed",
            messages=[
                {"role": "system", "content": build_system_prompt(countries)},
                {"role": "user", "content": summary},
            ],
            response_format={"type": "json_object"},
            temperature=0.0,
            max_tokens=4000,
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

            aggressor      = keep_first_entity(result.get("actor"))
            weapon_type    = sanitize_weapon_type(result.get("weapon_type"))
            target         = keep_first_entity(result.get("target"))
            objective      = result.get("objective")
            objective_type = sanitize_objective_type(result.get("objective_type"))

            if weapon_type is None and target is None:
                continue

            aggressor_coords = country_dict.get(aggressor)
            target_coords    = country_dict.get(target)

            aggressor_geom = None
            if aggressor_coords:
                lon, lat = aggressor_coords[1]
                aggressor_geom = f"SRID=4326;POINT({lon} {lat})"

            target_geom = f"SRID=4326;POINT({lon_tweet} {lat_tweet})"

            print(f"{aggressor} --[{weapon_type}]--> {target} | {objective} ({objective_type})")

            try:
                cur.execute(
                    """
                    INSERT INTO MILITARY_ACTIONS
                        (TWEET_ID, AGGRESSOR, TARGET, WEAPON_TYPE, OBJECTIVE, OBJECTIVE_TYPE, AGGRESSOR_GEOM, TARGET_GEOM)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, ST_GeomFromEWKT(%s), ST_GeomFromEWKT(%s))
                    ON CONFLICT (TWEET_ID) DO UPDATE SET
                        AGGRESSOR      = EXCLUDED.AGGRESSOR,
                        TARGET         = EXCLUDED.TARGET,
                        WEAPON_TYPE    = EXCLUDED.WEAPON_TYPE,
                        OBJECTIVE      = EXCLUDED.OBJECTIVE,
                        OBJECTIVE_TYPE = EXCLUDED.OBJECTIVE_TYPE,
                        AGGRESSOR_GEOM = EXCLUDED.AGGRESSOR_GEOM,
                        TARGET_GEOM    = EXCLUDED.TARGET_GEOM
                    """,
                    (tweet_id, aggressor, target, weapon_type, objective, objective_type,
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