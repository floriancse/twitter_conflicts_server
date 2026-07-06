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
    "Residential",
    "Civilian",
    "Government & Institutional",
    "Healthcare",
    "Education",
    "Humanitarian",
    "Leisure & Hospitality",
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
    If no weapon physically impacts a target (deployments, alerts, captures, troop movements,
    negotiations, announcements, flight restrictions without a confirmed strike):
    → {{"actor": null, "weapon_type": null, "target": null, "objective": null, "objective_type": null}}

    This also covers weapons merely observed in transit with no confirmed impact yet:
    "spotted flying over", "filmed heading toward", "targeting X right now" (with no reported
    hit), drones/missiles "diverting air traffic" without a confirmed strike, raw drone/missile
    counts from a source with no reported impact ("around 300 drones heading toward Russian
    regions"). These are in-progress or overflight events, not confirmed strikes → all fields
    null, including objective_type.

    ━━━ RULE 2 — ACTOR (who physically fires or operates the weapon?) ━━━
    Actor = the entity whose forces physically launch, fire, or pilot the weapon.
    NOT the defending force. NOT the country supplying the weapon.

    - State militaries → exact country name from the allowed countries list.
    - Named armed groups → exact name from the armed groups list. Never return null when the firing group is explicitly named.
    - Unknown attacker / passive voice ("was struck", "was destroyed") / unnamed attacker → actor = null.
    - Proxy forces or unnamed militias ("pro-Iranian groups", "Iran-backed militias") → actor = null.

    Common triggers:
    - "Hezbollah fired / launched / struck" → actor = "Hezbollah"
    - "TTP ambushed / attacked" → actor = "TTP"
    - "JNIM placed IED / struck" → actor = "JNIM"
    - "Houthis launched / fired" → actor = "Houthis"
    - "Guardians of Blood Brigades attacked" → actor = null (not in the list)
    - "RSF attacked" → actor = null (not in the list)

    ━━━ RULE 3 — TARGET (on whose territory does the weapon physically land?) ━━━
    Target = the country whose soil, vessel, or airspace absorbs the kinetic impact.

    Soil rule: target = the country whose SOIL the weapon hits, regardless of who owns the
    asset struck. A US base hit in Iraq → target = "Iraq". An Iranian system destroyed in Syria → target = "Syria".

    Territory overrides (treat as belonging to the occupying/administering power):
    - Crimea, Donetsk, Luhansk, Zaporizhzhia, Kherson oblasts (Russian-occupied) → target = "Russia"
    - Oryol, Belgorod, Kursk, Bryansk oblasts (Russian territory) → target = "Russia"
    - Government-controlled Ukraine → target = "Ukraine"

    Vessel rule:
    - Military vessel → target = the vessel's flag state / operating country.
    - Commercial vessel → target = flag state only if it appears in the allowed countries list, else null.
    - "Shadow fleet" tankers without clear flag → target = null.

    Multi-target rule: when one event clearly strikes targets in multiple countries simultaneously,
    pick the country with the most significant damage described. Return a single string, never a list.

    ━━━ RULE 4 — INTERCEPTION RULE (read carefully) ━━━
    When a weapon is SHOT DOWN, INTERCEPTED, or DESTROYED in flight:
    - actor = the country or group that LAUNCHED the weapon being destroyed.
    - target = the country where the weapon was destroyed (the soil or airspace it fell on).
    - The defending/intercepting force is NOT the actor.

    Examples:
    "Ukrainian crew shot down a Russian Shahed over Odesa."
    → actor = "Russia" (Russia launched the Shahed), target = "Ukraine" (it fell on Ukrainian soil).

    "Iranian defenses intercepted a US JASSM over Markazi Province."
    → actor = "United States of America" (US launched the missile), target = "Iran" (where it fell).

    "US MQ-9 shot down an Iranian Mohajer-6 drone over Iranian airspace."
    → actor = "United States of America" (US fired the weapon that destroyed the drone), target = "Iran".

    Friendly fire rule: a weapon that malfunctions and hits its own side → actor = that country, target = that country.

    ━━━ RULE 5 — WEAPON_TYPE ━━━
    Pick the single closest match from this list:
    {weapon_list}

    Mapping guidance:
    - "Drones": FPV drones, loitering munitions (Shahed, Geran, Lancet), USVs, UAVs, kamikaze drones.
    - "Missiles": cruise missiles, ballistic missiles, guided missiles (Kh-series, JASSM, LMUR), anti-tank missiles.
    - "Explosives": IEDs, rockets (107mm, Grad), RPGs, MANPADS, grenades, mortar rounds, suicide vests.
    - "Air Defence": ground-based SAM systems (Tor, Patriot, Buk) firing at aerial targets — use only when the air defence system itself is the weapon destroying a target (not when it is the target being hit).
    - "Military Aviation": fixed-wing aircraft airstrikes, helicopter gunship attacks.
    - "Artillery & Armour": howitzers, tank cannons, self-propelled guns, field artillery.
    - "Small Arms": rifles, machine guns, sniper fire, handguns.

    If the weapon is mentioned but does not clearly fit any category → "Drones" for aerial platforms, else "Explosives" as default. If completely unidentifiable → null.

    ━━━ RULE 6 — OBJECTIVE (what physical asset was struck?) ━━━
    2–6 words describing the physical thing struck. Be specific when the text allows it.
    Prefer: "oil refinery", "radar system", "ammunition depot", "armored vehicle", "military helicopter".
    Avoid generic terms like "infrastructure" or "facility" when more detail is available.
    Return null if no specific target is mentioned.

    ━━━ RULE 6bis — OBJECTIVE_TYPE (category of the struck asset) ━━━
    Classify the objective into exactly one category from this closed list:
    {objtype_list}

    Mapping guidance:
    - "Military": barracks, checkpoints, army positions, armored vehicles, artillery, command posts,
      military bases, aircraft/ships/vehicles belonging to armed forces, weapon/ammunition depots,
      fortifications, military personnel.
    - "Industrial": refineries, factories, manufacturing plants, industrial/production complexes
      (including defense-industrial plants like Titan-Barrikady, semiconductor plants).
    - "Energy": power substations, power plants, electricity pylons/grid, oil depots/terminals
      (storage, not refining).
    - "Transport & Infrastructure": bridges, roads, railways, ports/seaports, airfields/airports,
      logistics/supply routes, parking/transport hubs.
    - "Communications": satellite communication centers, telecom/comms hubs, radio/radar
      installations (when the installation itself, not a weapon system, is struck).
    - "Residential": houses, apartment buildings, residential complexes/districts.
    - "Civilian": a SPECIFIC, NAMED civilian object or site — a particular car/minibus/truck
      (not military), a named gas station, a named street or market, a specific identifiable
      public space. The text must point to one concrete civilian asset, not just a general area.
    - "Government & Institutional": government buildings, administrative offices, border posts.
    - "Healthcare": hospitals, clinics, medical facilities.
    - "Education": schools, universities.
    - "Humanitarian": aid warehouses, humanitarian organization sites (Red Cross, etc.).
    - "Leisure & Hospitality": hotels, sports clubs, swimming pools, restaurants, cafes, entertainment venues.
    - "Unidentified/Other": use when the objective is null, too vague, or fits no category above.

    ━━━ VAGUE / GENERIC TARGET GUARD (read carefully) ━━━
    Do NOT use "Civilian" as a default bucket. If the only thing mentioned is a city name, a
    region, "urban area", "multiple regions/targets", or an unspecified count of strikes with
    no named building/vehicle/site struck → objective = a short generic phrase (or null) and
    objective_type = "Unidentified/Other". Reserve "Civilian" strictly for a clearly named,
    specific civilian object or site, per the definition above.

    If objective is null, objective_type must also be null.

    ━━━ RULE 7 — UNCERTAINTY ━━━
    - "Reportedly", "allegedly", "suspected", "possibly", "claimed" → still extract.
    - Pure rumor with no detail → all null.

    ━━━ VALID VALUES ━━━
    actor → exactly one value from the countries list OR from the armed groups list, or null.
    target → exactly one value from the countries list, or null.
    weapon_type → exactly one value from the weapon types list, or null.
    objective_type → exactly one value from the objective types list, or null.
    Countries: {country_list}
    Armed groups (actor only): {group_list}

    ━━━ EXAMPLES ━━━

    # 1. Standard offensive strike — drone on energy infrastructure
    "Ukrainian attack drones struck an oil pipeline pumping station in Perm, Russia, setting it ablaze."
    → {{"actor": "Ukraine", "weapon_type": "Drones", "target": "Russia", "objective": "oil pumping station", "objective_type": "Energy"}}

    # 2. Standard offensive strike — aviation on enemy position
    "Israeli airstrike targeted a Hezbollah rocket launcher concealed inside a building in southern Lebanon."
    → {{"actor": "Israel", "weapon_type": "Military Aviation", "target": "Lebanon", "objective": "rocket launcher", "objective_type": "Military"}}

    # 3. Standard offensive strike — artillery
    "Russian artillery operators destroyed a Ukrainian 2S3 Akatsiya self-propelled howitzer in Zaporizhzhia Oblast."
    → {{"actor": "Russia", "weapon_type": "Artillery & Armour", "target": "Ukraine", "objective": "self-propelled howitzer", "objective_type": "Military"}}

    # 4. Occupied territory → target = "Russia"
    "Ukrainian FP-2 drones destroyed both transformers at the 220 kV substation in Alchevsk, Luhansk region."
    → {{"actor": "Ukraine", "weapon_type": "Drones", "target": "Russia", "objective": "power substation transformers", "objective_type": "Energy"}}

    # 5. Interception — the defending force is NOT the actor
    "Ukrainian soldier fired a MANPADS from the street in Dnipro, intercepting a Russian UAV."
    → {{"actor": "Russia", "weapon_type": "Drones", "target": "Ukraine", "objective": "UAV", "objective_type": "Military"}}

    # 6. Interception — same rule, air defense system as weapon
    "A Ukrainian Tor-M2 air defense system shot down a Russian Kh-101 cruise missile over Kyiv."
    → {{"actor": "Russia", "weapon_type": "Missiles", "target": "Ukraine", "objective": "cruise missile", "objective_type": "Military"}}

    # 7. Named armed group — Africa
    "JNIM/FLA coalition attacked the city of Gourma-Rharous and its military camp in Mali."
    → {{"actor": "JNIM", "weapon_type": "Small Arms", "target": "Mali", "objective": "military camp", "objective_type": "Military"}}

    # 8. Named armed group — Middle East, hybrid weapon
    "Hezbollah used a fiber-optic FPV kamikaze drone armed with a PG-7(L) anti-tank RPG warhead to strike an Israeli armored vehicle in southern Lebanon."
    → {{"actor": "Hezbollah", "weapon_type": "Drones", "target": "Lebanon", "objective": "armored vehicle", "objective_type": "Military"}}

    # 9. Named armed group — South Asia
    "Afghan Taliban fired a mortar shell into the border area of Angoor Adda, striking a house and injuring five Pakistanis."
    → {{"actor": "Taliban", "weapon_type": "Explosives", "target": "Pakistan", "objective": "residential building", "objective_type": "Residential"}}

    # 10. Actor = null — unlisted group
    "Islamic Resistance FPV drone armed with a PG-7VR tandem-HEAT warhead struck a communication tower at the US Victoria Base in Baghdad."
    → {{"actor": null, "weapon_type": "Drones", "target": "Iraq", "objective": "communication tower", "objective_type": "Communications"}}

    # 11. Actor = null — unknown attacker, vague target
    "An unknown group used 107mm Type 63 rockets to strike the city of Quetta in Balochistan, Pakistan."
    → {{"actor": null, "weapon_type": "Explosives", "target": "Pakistan", "objective": "urban area", "objective_type": "Unidentified/Other"}}

    # 11bis. Specific named civilian object → "Civilian"
    "An unknown group used 107mm Type 63 rockets to destroy a passenger minibus on Airport Road in Quetta, Pakistan."
    → {{"actor": null, "weapon_type": "Explosives", "target": "Pakistan", "objective": "passenger minibus", "objective_type": "Civilian"}}

    # 11ter. Overflight / in-progress, no confirmed impact → all null
    "A Ukrainian Flamingo cruise missile was spotted flying over Russia's Chuvash Republic, over 500 miles from the border."
    → {{"actor": null, "weapon_type": null, "target": null, "objective": null, "objective_type": null}}

    # 12. Friendly fire / accident
    "A Ukrainian military pick-up carrying ammunition detonated in Kharkiv, shattering windows in surrounding buildings."
    → {{"actor": "Ukraine", "weapon_type": "Explosives", "target": "Ukraine", "objective": "ammunition vehicle", "objective_type": "Military"}}

    # 13. Non-kinetic event → all null
    "Air raid sirens sounded across Kiryat Shmona, Israel, over a suspected drone attack from Lebanon."
    → {{"actor": null, "weapon_type": null, "target": null, "objective": null, "objective_type": null}}

    # 14. Industrial site (not military-owned)
    "New satellite imagery shows the aftermath of the Ukrainian cruise missile strike on Russia's Voronezh Semiconductor Devices Plant."
    → {{"actor": "Ukraine", "weapon_type": "Missiles", "target": "Russia", "objective": "semiconductor plant", "objective_type": "Industrial"}}

    # 15. Transport infrastructure
    "The road bridge on the H20 highway linking Mariupol and Donetsk collapsed after Ukrainian drone strikes."
    → {{"actor": "Ukraine", "weapon_type": "Drones", "target": "Russia", "objective": "road bridge", "objective_type": "Transport & Infrastructure"}}

    # 16. Leisure & Hospitality
    "A Russian drone strike hit a swimming pool inside a public fitness club in Zaporizhzhia."
    → {{"actor": "Russia", "weapon_type": "Drones", "target": "Ukraine", "objective": "swimming pool", "objective_type": "Leisure & Hospitality"}}

    # 17. Healthcare
    "A Russian attack damaged a hospital in Kherson, killing a doctor and injuring a nurse."
    → {{"actor": "Russia", "weapon_type": "Military Aviation", "target": "Ukraine", "objective": "hospital", "objective_type": "Healthcare"}}

    # 18. Humanitarian
    "A Russian strike destroyed a Ukrainian Red Cross warehouse in Kyiv, destroying humanitarian aid and equipment."
    → {{"actor": "Russia", "weapon_type": "Missiles", "target": "Ukraine", "objective": "Red Cross warehouse", "objective_type": "Humanitarian"}}
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