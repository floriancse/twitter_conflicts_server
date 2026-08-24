"""
Extraction of (actor, weapon_type, target, objective) quadruplets from conflict summaries via local LLM (Ollama).
"""

import json
from openai import OpenAI
import psycopg2
import os
from dotenv import load_dotenv
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
SELECT
	A.ENTITY_NAME,
	C.NAME,
	ROUND(ST_X (C.GEOM)::NUMERIC, 2) AS LON,
	ROUND(ST_Y (C.GEOM)::NUMERIC, 2) AS LAT
FROM
	WORLD_AREAS A
	LEFT JOIN WORLD_CAPITALS C ON ST_INTERSECTS (A.GEOM, C.GEOM)
WHERE
	C.GEOM IS NOT NULL
	AND ENTITY_TYPE = 'country'
"""

client = OpenAI(
    base_url="http://localhost:8081/v1",
    api_key="",
)

# Closed list of allowed weapon types
WEAPON_TYPES = [
    "Drones",
    "Missiles",
    "Explosives",
    "Air Defence",
    "Military Aviation",
    "Artillery & Armour",
    "Small Arms",
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
    'Lashkar-e-Taiba', 'HTS',
]

def build_system_prompt(countries: list[str]) -> str:
    country_list = ", ".join(f'"{c}"' for c in countries)
    weapon_list  = ", ".join(f'"{w}"' for w in WEAPON_TYPES)
    group_list   = ", ".join(f'"{g}"' for g in ARMED_GROUPS)
    objtype_list = ", ".join(f'"{o}"' for o in OBJECTIVE_TYPES)

    return f"""You are an OSINT analyst. You will receive a batch of conflict summaries, each
    labeled with an EVENT_ID (an opaque identifier — treat it as a label, not as data to
    interpret). For EACH summary, independently extract one quintuplet
    (actor, weapon_type, target, objective, objective_type) following the rules below.
    Judge every event ONLY on its own text — do not let one event's content influence another's.

    You MUST return exactly one result per EVENT_ID you were given, in the same order, with no
    omissions and no extra entries. Respond ONLY with a raw JSON object — no markdown, no
    commentary, no extra keys:
    {{"results": [
      {{"event_id": "<EVENT_ID as given>", "actor": "...", "weapon_type": "...", "target": "...", "objective": "...", "objective_type": "..."}}
    ]}}

    RULE 1 — NON-KINETIC: no confirmed physical impact (deployments, alerts, captures, troop
    movements, negotiations, announcements, flight restrictions, overflight/in-transit sightings
    with no reported hit, raw drone/missile counts with no impact) → all fields null.

    RULE 2 — ACTOR (who fires/operates the weapon, never the defender or the supplier country):
    state military → exact country name from the list; named armed group → exact name from the
    group list, never null when explicitly named; unknown/unnamed attacker, passive voice
    ("was struck"), unnamed proxy/militia → null.
    Aliases: "USF" (Unmanned Systems Forces, Ukraine) → Ukraine; "IRGC" → Iran; Wagner/Africa Corps → Russia. Talibans (TTP, Balochistan Liberation Front) → Afghanistan.

    RULE 3 — TARGET (whose territory absorbs the impact): soil rule — target = country whose SOIL
    is hit regardless of asset ownership (US base hit in Iraq → target "Iraq"). Occupied territory
    (Crimea, Donetsk, Luhansk, Zaporizhzhia, Kherson; Oryol, Belgorod, Kursk, Bryansk) → "Russia";
    government-controlled Ukraine → "Ukraine". Vessel: military → flag state/operator; commercial →
    flag state if in list else null; unclear "shadow fleet" → null. Multi-target → country with the
    most significant damage, single string never a list.

    RULE 4 — INTERCEPTION: when a weapon is shot down/intercepted/destroyed in flight, actor = who
    LAUNCHED it, target = where it FELL — the intercepting/defending force is never the actor
    (e.g. "Ukraine shot down a Russian Shahed over Odesa" → actor "Russia", target "Ukraine").
    Friendly fire (weapon malfunctions, hits own side) → actor = target = that country.

    RULE 4bis — ACTOR ≠ TARGET, UNLESS GENUINE FRIENDLY FIRE: actor and target must NOT be the
    same entity except in a confirmed friendly-fire/accident case (own weapon malfunctions, own
    munitions/vehicle detonates, own forces mistakenly hit — as in example 6). A state military
    conducting a deliberate strike against a NON-STATE ARMED GROUP on its own soil is NOT friendly
    fire, even though the impact occurs domestically: target = the named armed group from the
    group list if identifiable from context (named group, or clearly implied by region/known
    affiliation), otherwise null (do not fall back to the striking state's own name).
    Example: "Nigeria's air platforms killed suspected militants in Borno State, targeting groups
    near Aduwa and Buratai" → actor "Nigeria", target = the group if identifiable (e.g. "Boko
    Haram"/"ISWAP" per context) or null if not clearly named/implied — never "Nigeria".

    RULE 5 — WEAPON_TYPE, closest single match from: {weapon_list}.
    Drones: FPV/loitering munitions (Shahed, Geran, Lancet), USVs, UAVs. Missiles: cruise/ballistic/
    guided (Kh-series, JASSM, LMUR), anti-tank. Explosives: IEDs, rockets (107mm, Grad), RPGs,
    MANPADS, grenades, mortars, suicide vests. Air Defence: ground-based SAM (Tor, Patriot, Buk) as
    the weapon destroying a target, not when it's the target. Military Aviation: fixed-wing
    airstrikes, helicopter gunship. Artillery & Armour: howitzers, tank cannons, field artillery.
    Small Arms: rifles, machine guns, sniper fire, handguns.
    Unclear but aerial → "Drones"; unclear otherwise → "Explosives"; unidentifiable → null.

    RULE 6 — OBJECTIVE: 2–6 words, specific when possible ("oil refinery", not "infrastructure").
    Null if no specific target is mentioned.

    RULE 6bis — OBJECTIVE_TYPE, exactly one from: {objtype_list}.
    Military: barracks, checkpoints, positions, armored vehicles, artillery, command posts, bases/
    aircraft/ships/vehicles, depots, fortifications, personnel. Industrial: refineries, factories,
    defense-industrial/semiconductor plants. Energy: substations, power plants, grid/pylons, oil
    storage/terminals (not refining). Transport & Infrastructure: bridges, roads, railways, ports,
    airfields, logistics hubs. Communications: comms/telecom hubs, radio/radar installations
    (the installation itself, not a weapon system). Civilian: houses, apartments, residential
    areas/neighborhoods (generic, no proper name needed); hospitals, clinics, schools; hotels,
    restaurants, entertainment venues; humanitarian sites; or a specific named civilian object
    (car, minibus, gas station, market). Government & Institutional: government buildings,
    administrative offices, border posts. Unidentified/Other: objective is null, too vague (bare
    city/region/"urban area" with no indication of what was hit), or fits no category — do not
    default to "Civilian" for a vague location, only when the text clearly signals civilian
    character. If objective is null, objective_type must also be null.

    RULE 7 — UNCERTAINTY: "reportedly/allegedly/suspected/possibly/claimed" → still extract.
    Pure rumor with no detail → all null.

    VALID VALUES — actor: country from list OR armed group from list, or null. target: country
    from list OR armed group from list (only for a domestic counter-insurgency strike per RULE
    4bis), or null. weapon_type / objective_type: exactly one value from their list, or null.
    Countries: {country_list}
    Armed groups (actor or target per RULE 4bis): {group_list}

    EXAMPLES (each shown as a single labeled event; apply the same logic per EVENT_ID in your batch)

    1. Standard strike:
    EVENT_ID: ex1 — "Ukrainian attack drones struck an oil pipeline pumping station in Perm, Russia, setting it ablaze."
    → {{"event_id": "ex1", "actor": "Ukraine", "weapon_type": "Drones", "target": "Russia", "objective": "oil pumping station", "objective_type": "Energy"}}

    2. Occupied territory → target "Russia":
    EVENT_ID: ex2 — "Ukrainian FP-2 drones destroyed both transformers at the 220 kV substation in Alchevsk, Luhansk region."
    → {{"event_id": "ex2", "actor": "Ukraine", "weapon_type": "Drones", "target": "Russia", "objective": "power substation transformers", "objective_type": "Energy"}}

    3. Interception — defending force is NOT the actor:
    EVENT_ID: ex3 — "Ukrainian soldier fired a MANPADS from the street in Dnipro, intercepting a Russian UAV."
    → {{"event_id": "ex3", "actor": "Russia", "weapon_type": "Drones", "target": "Ukraine", "objective": "UAV", "objective_type": "Military"}}

    4. Actor null (unlisted group), vague target → "Unidentified/Other":
    EVENT_ID: ex4 — "An unknown group used 107mm Type 63 rockets to strike the city of Quetta in Balochistan, Pakistan."
    → {{"event_id": "ex4", "actor": null, "weapon_type": "Explosives", "target": "Pakistan", "objective": "urban area", "objective_type": "Unidentified/Other"}}

    5. Generic but explicitly civilian-populated area → "Civilian" (contrast with #4, no proper name needed):
    EVENT_ID: ex5 — "A Russian Pantsir SAM system accidentally fired 30 mm cannon rounds into a residential neighborhood in Afipsky, Russia."
    → {{"event_id": "ex5", "actor": "Russia", "weapon_type": "Air Defence", "target": "Russia", "objective": "residential neighborhood", "objective_type": "Civilian"}}

    6. Friendly fire / accident:
    EVENT_ID: ex6 — "A Ukrainian military pick-up carrying ammunition detonated in Kharkiv, shattering windows in surrounding buildings."
    → {{"event_id": "ex6", "actor": "Ukraine", "weapon_type": "Explosives", "target": "Ukraine", "objective": "ammunition vehicle", "objective_type": "Military"}}

    6bis. Domestic counter-insurgency strike — NOT friendly fire, target = the armed group, not the striking state:
    EVENT_ID: ex6bis — "Nigeria's Operation Hadin Kai air platforms killed about 38 suspected militants in precision strikes on militant concentrations in Borno State, targeting Boko Haram groups tracked near Aduwa and Buratai."
    → {{"event_id": "ex6bis", "actor": "Nigeria", "weapon_type": "Military Aviation", "target": "Boko Haram", "objective": "militant concentrations", "objective_type": "Military"}}

    7. Overflight / no confirmed impact → all null:
    EVENT_ID: ex7 — "A Ukrainian Flamingo cruise missile was spotted flying over Russia's Chuvash Republic, over 500 miles from the border."
    → {{"event_id": "ex7", "actor": null, "weapon_type": null, "target": null, "objective": null, "objective_type": null}}
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


# Nombre de résumés envoyés par appel LLM. Le system prompt (règles + listes pays/
# groupes/armes) coûte le même prix qu'il traite 1 ou N événements : plus ce chiffre
# est haut, moins on paie de fois ce prompt. 10 reste prudent ici car la tâche a 5
# champs et des règles fines par événement (contrairement à une simple catégorisation).
BATCH_SIZE = 5


def chunked(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def extract_quadruplets_batch(batch: list[tuple[str, str]], countries: list[str]) -> dict[str, dict]:
    """batch: list of (event_id, summary). Returns {event_id: quadruplet_dict}."""
    user_content = "\n\n".join(
        f"EVENT_ID: {event_id}\nTEXT: {summary}"
        for event_id, summary in batch
    )

    try:
        response = client.chat.completions.create(
            model="gemma-4-26B-A4B",
            messages=[
                {"role": "system", "content": build_system_prompt(countries)},
                {"role": "user", "content": user_content},
            ],
            response_format={"type": "json_object"},
            temperature=0.0,
            max_tokens=700 * len(batch) + 200,
        )
        track(response)
        raw = response.choices[0].message.content.strip()
        parsed = json.loads(raw)
        results = parsed.get("results", [])
    except Exception as e:
        print(f"LLM extraction error (batch of {len(batch)}): {e}")
        return {}

    output: dict[str, dict] = {}
    for entry in results:
        event_id = entry.get("event_id")
        if event_id is None:
            continue
        output[str(event_id)] = entry

    return output


def generate_aggressor():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        tweets, country_dict = fetch_aggressor_data(cur)
        countries = list(country_dict.keys())

        for batch in chunked(tweets, BATCH_SIZE):
            # event_id = tweet_id en str : garantit une correspondance fiable même
            # si tweet_id est un grand entier (bigint Twitter).
            llm_batch = [(str(row[0]), row[2]) for row in batch]
            results = extract_quadruplets_batch(llm_batch, countries)

            for row in batch:
                tweet_id, date, summary, loc_name, lon_tweet, lat_tweet = row
                result = results.get(str(tweet_id))

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
    print(token_tracker.summary())