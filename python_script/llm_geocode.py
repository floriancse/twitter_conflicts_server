"""
Module d'extraction d'événements géopolitiques via LLM (Ollama local)
=============================================================
"""

import json
from openai import OpenAI
from datetime import datetime
from token_tracker import track
import token_tracker

client = OpenAI(base_url="http://localhost:8081/v1", api_key="")

# Nombre de tweets envoyés par appel LLM dans le mode batch. Le prompt système
# (règles + schéma) coûte le même prix qu'il traite 1 ou N tweets : plus ce
# chiffre est haut, moins on paie de fois ce prompt. On reste prudent (8) car
# chaque tweet peut produire plusieurs événements avec un JSON de sortie
# verbeux, et le serveur local tourne avec --ctx-size 8192 seulement.
BATCH_SIZE = 5

RULES_BODY = """Extract concrete geopolitical events from tweets.
 
Return {"events": []} ONLY if: no concrete event/action is described (pure metadata, single words, retweet headers), OR no actor can be identified, OR the content is satire/a joke/a question with no factual claim.
An implicit location is NOT a reason to skip an event — infer it (see GEOLOCATION).
 
EXTRACT: attacks, strikes, explosions, ship seizures, military movements, deployments, political declarations, threats, sanctions, arms transfers, drone operations, airspace incidents.
SKIP: pure metadata ("Source:", "Thread:", "Breaking:") with no actual content.
 
GEOLOCATION
nominatim_query MUST be "City, Country" or "Region, Country" ONLY — never a facility/unit name or a preposition ("near", "at"). Put facility/installation names in summary_text only. Resolution order (use the first that applies):
1. Named city/town  → "Donetsk, Ukraine"
2. Named region     → "Khuzestan, Iran"
3. Country as an explicit LOCATION marker ("in Russia", "over Iran", "inside Ukraine"), use country full name (e.g. Democratic Republic of the Congo not DRC) → that country's capital, e.g. "Moscow, Russia". Do NOT trigger this from a nationality/origin adjective alone ("a Russian howitzer", "Ukrainian forces") — those describe WHO/WHAT, not WHERE.
4. Named sea/strait  → use the sea's own name as nominatim_query, no country attached and NO directional prefix (e.g. "Gulf of Oman", "Strait of Hormuz", "Black Sea", "Persian Gulf", "Arabian Sea"). If a direction (North/East/West/South, Northern/Eastern/etc.) is attached to the sea/strait, STRIP it — this overrides the DIRECTIONAL MODIFIERS section below, which does NOT apply to seas/straits.
   Negative example (do NOT do this): "North Arabian Sea" → nominatim_query = "North Arabian Sea". WRONG.
   Correct: "North Arabian Sea" → nominatim_query = "Arabian Sea". The direction can still inform lat/lon placement (shift the point toward the northern part of the sea) but must never appear in the nominatim_query string itself.
Example: "Ukraine destroyed a depot at the Tochmash plant near Donetsk airport" → nominatim_query = "Donetsk, Ukraine" (facility name dropped, confidence = "high").
If truly no location can be inferred (e.g. pure opinion, no actor/target location) → nominatim_query = null, lat/lon = null, confidence = "low". Note : attribute Crimea and its cities to Ukraine (e.g. Sevastopol, Ukraine).

ORIGIN vs TARGET
Some events describe an action FIRED/LAUNCHED/TAKING OFF FROM one place TOWARD/AT/AGAINST another
(e.g. "missiles launched from [base] in [origin] toward/at a target in [destination]", "jets took off
from [origin] to strike [destination]"). Whenever a preposition such as "toward", "at", "against",
"targeting", "aimed at", "heading to", "to strike" connects an origin to a target:
- nominatim_query MUST resolve to the TARGET/destination, never the origin/launch site — even if the
  origin is far more precise (a named base/city) than the target. The origin can still be named in
  summary_text (it's part of "actor/weapon"), it just never drives geolocation.
- If the target is only a bare country with no city/region given (e.g. "toward a base somewhere in
  Jordan"), resolve it per rule 3 (country capital, or the relevant part of the country if a
  directional modifier is present) — confidence "medium" at most, since the precise impact point is
  unknown.
- Exception: if no target is stated at all (pure launch/detection report, nothing fired "at/toward"
  anything), fall back to the origin location as the only available information.
Example: "IRGC launched ballistic missiles from its base in Yazd, Iran, toward a U.S. base in Jordan"
→ nominatim_query = "Amman, Jordan" (target country resolved to its capital), NOT "Yazd, Iran".
 
DIRECTIONAL MODIFIERS
This section applies ONLY to countries/regions (rule 3), NEVER to seas/straits (rule 4) — for seas/straits, always strip the direction per rule 4 above, no exceptions.
Watch for directional qualifiers attached to a location: "Middle East", "Eastern", "Northern", "Western", "Southern", "Central", "North", "South", "East", "West" (e.g. "eastern Ukraine", "northern Gaza", "southern Lebanon", "western Iran", "Middle East").
- KEEP the directional qualifier in nominatim_query when it is attached to a country/region and no more precise city/town is named — e.g. "eastern Ukraine" → nominatim_query = "Eastern Ukraine, Ukraine" (not just "Ukraine"), "southern Lebanon" → "Southern Lebanon, Lebanon", "Middle East" (as a standalone region, no country given) → "Middle East". Do NOT drop the directional word and fall back to the bare country name; that silently discards real location information.
- If a directional qualifier and a named city/region BOTH appear ("Kharkiv in eastern Ukraine"), the named city/region still wins per the resolution order above (nominatim_query = "Kharkiv, Ukraine") — the directional word is redundant and can be dropped in that case only.
- LAT/LON PLACEMENT: never just output the country's centroid or capital coordinates when a directional modifier is present. Shift your lat/lon estimate toward the stated compass zone of that country/region (e.g. "eastern Ukraine" → a point in Ukraine's eastern third, such as near Kharkiv/Luhansk oblasts, not Kyiv; "western Iran" → toward the Iran-Iraq border area, not Tehran; "northern Gaza" → toward Gaza City/Jabalia, not the strip's centroid; "Middle East" alone, with no country → pick a reasonable central point for the region, e.g. near the Gulf, and set confidence no higher than "medium").
- confidence for a directional-only location (no named city) is "medium" at most, since the point is an estimate, not a precise place.
 
Coordinates: if the tweet itself gives explicit decimal coordinates, use them exactly (no rounding), confidence = "explicit", and still fill nominatim_query from the nearest city/region. Otherwise, whenever nominatim_query is non-null, estimate lat/lon from your own knowledge — never leave lat/lon null if nominatim_query is set.
 
TYPOLOGY — apply the first matching rule:
MIL: a kinetic event that ALREADY HAPPENED (attack, bombing, strike, shooting, explosion, drone strike). Requires a past/present action verb ("struck", "destroyed", "fired"). Never for plans or future ops.
POL: statements, plans, threats, sanctions, negotiations, intel reports — anything NOT yet physical action. Use for "discussed / plan to / may / could / will".
MOVE: a confirmed physical repositioning already completed (deployment, ship/aircraft arrival, troop movement, arms delivery) but not combat.
OTHER: civilian incident, accident, humanitarian event, non-military seizure.
When in doubt between MIL and POL: planned/discussed → POL; already physically happened → MIL/MOVE.
 
TENSION SCORE (0-5, be conservative — most events score 1-3):
0 routine/admin · 1 minor local incident · 2 small skirmish/standard tactical event · 3 notable escalation risk (infrastructure attack, major deployment) · 4 major escalation (massive strike, large naval deployment, cross-border state attack) · 5 exceptional threat (war declaration, WMD use, attack on a nuclear power).
 
CONFIDENCE: "explicit" (coords given) · "high" (event+location unambiguous, named city) · "medium" (location inferred from facility/country name, or details partly unverified) · "low" (location implicit or claim speculative).
 
is_delayed: "false" if the event happened today/recently (default when date is unclear); "true" if it happened in the past (see TEMPORAL MARKING)."""

EVENT_OBJECT_SCHEMA = """{
      "summary_text": "1 concise sentence naming: (1) actor country/force, (2) weapon/means if identifiable, (3) target country/entity, (4) what was physically struck. E.g. 'Israeli Air Force conducted bombing airstrikes on Iranian ballistic missile infrastructure in Khuzestan, Iran.'",
      "typology": "MIL | POL | MOVE | OTHER",
      "strategic_importance": 1-5,
      "nominatim_query": "'City, Country' string, or null",
      "confidence": "explicit | high | medium | low",
      "lat": float or null,
      "lon": float or null,
      "is_delayed": "true | false"
    }"""

TEMPORAL_INSTRUCTIONS = """
 
TEMPORAL MARKING (mandatory):
Using today's date given above: if the tweet is a retrospective, commemoration, anniversary, "on this day", "X years/months ago", or otherwise clearly recalls an event from well before today (rough guide: >~30 days before today, or any explicit anniversary/retrospective framing regardless of how old) → it is HISTORICAL. Plain past-tense reporting of something from the last few days is NOT historical.
For HISTORICAL events, prefix summary_text with "[HISTORICAL EVENT - <date, as precisely as known, e.g. 'July 2024'>] " before the normal summary. If no date is identifiable, use "[HISTORICAL EVENT - date unclear]".
Example: a 2026 post marking "2nd anniversary of the Battle of Tinzaouatène (July 2024)" → summary_text starts with "[HISTORICAL EVENT - July 2024] "."""


def build_system_prompt(batch: bool = False) -> str:
    """Builds the system prompt with today's date injected, so the model can judge
    how recent or how distant an event is (e.g. anniversary/retrospective posts).
    batch=True switches the intro/output schema to the multi-tweet format used by
    extract_events_and_geoloc_batch; the extraction RULES_BODY itself never changes."""
    today_str = datetime.now().strftime("%Y-%m-%d")
    date_header = (
        f"Today's date is {today_str}. Use this date as your reference point whenever "
        f"you need to judge how recent, ongoing, or historical an event is.\n\n"
    )

    if batch:
        intro = """You are an OSINT analyst. Respond ONLY in English, ONLY valid JSON, no markdown.

You will receive a batch of tweets, each labeled with a TWEET_ID (an opaque identifier —
treat it as a label, not as data to interpret). Analyze EACH tweet INDEPENDENTLY: never let
one tweet's content influence the extraction for another.

"""
        output = f"""

OUTPUT — ALL FIELDS MANDATORY per event, JSON only. Return exactly one entry per TWEET_ID you
were given, in the same order, no omissions and no extra entries:
{{
  "results": [
    {{
      "tweet_id": "<TWEET_ID as given>",
      "events": [
        {EVENT_OBJECT_SCHEMA}
      ]
    }}
  ]
}}
If a given tweet has no extractable event, its "events" array must be []."""
    else:
        intro = "You are an OSINT analyst. Respond ONLY in English, ONLY valid JSON, no markdown.\n\n"
        output = f"""

OUTPUT — ALL FIELDS MANDATORY, JSON only:
{{
  "events": [
    {EVENT_OBJECT_SCHEMA}
  ]
}}
If no extractable event → return {{"events": []}}"""

    return date_header + intro + RULES_BODY + output + TEMPORAL_INSTRUCTIONS


def extract_events_and_geoloc(tweet_text: str) -> dict | None:
    """Version single-tweet : conservée pour usage standalone / tests. La boucle
    principale de feed.py utilise désormais extract_events_and_geoloc_batch."""
    try:
        response = client.chat.completions.create(
            model="gemma-4-26B-A4B",
            messages=[
                {"role": "system", "content": build_system_prompt(batch=False)},
                {
                    "role": "user",
                    "content": (
                        f"Analyze this tweet and return a JSON object. "
                        f"If no event is extractable, return {{\"events\": []}}.\n{tweet_text}"
                    ),
                },
            ],
            response_format={"type": "json_object"},
            temperature=0.0,
        )
        track(response)
        raw_content = response.choices[0].message.content.strip()

        if not raw_content:
            return {"events": []}

        return json.loads(raw_content)

    except Exception as e:
        print(str(e))
        return None


def chunked(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def extract_events_and_geoloc_batch(batch: list[tuple[str, str]]) -> dict[str, dict]:
    """batch: list of (tweet_id, tweet_text). Returns {tweet_id: {"events": [...]}}.
    A tweet_id absent from the returned dict means the LLM call or its parsing
    failed for the whole batch — callers should treat it as 'not processed yet'
    (the tweet stays out of the DB and will be retried on the next run)."""
    user_content = "\n\n".join(
        f"TWEET_ID: {tweet_id}\nTEXT: {tweet_text}"
        for tweet_id, tweet_text in batch
    )

    try:
        response = client.chat.completions.create(
            model="gemma-4-26B-A4B",
            messages=[
                {"role": "system", "content": build_system_prompt(batch=True)},
                {
                    "role": "user",
                    "content": (
                        "Analyze each of these tweets independently and return a JSON object "
                        f"per the schema above.\n\n{user_content}"
                    ),
                },
            ],
            response_format={"type": "json_object"},
            temperature=0.0,
        )
        track(response)
        raw_content = response.choices[0].message.content.strip()

        if not raw_content:
            return {}

        parsed = json.loads(raw_content)
        results = parsed.get("results", [])

    except Exception as e:
        print("LLM batch error:", str(e))
        return {}

    output: dict[str, dict] = {}
    for entry in results:
        tweet_id = entry.get("tweet_id")
        if tweet_id is None:
            continue
        output[str(tweet_id)] = {"events": entry.get("events", [])}

    return output


if __name__ == "__main__":
    print(extract_events_and_geoloc("""
BREAKING: The IRGC Aerospace Force has just launched three Khorramshahr-3 or 4 ballistic missiles from its Al-Qadir ballistic missile base in Yazd, central Iran, toward a U.S. military base somewhere in Jordan.

These ballistic missiles are equipped with cluster munitions and can present a particularly difficult interception challenge for the U.S. Army’s Patriot PAC-3 air-defense systems.

Their cluster-munition payloads can disperse submunitions across a wide area, potentially damaging multiple aircraft parked in the open on the aprons and ramps of an air base.

However, I doubt that many valuable aircraft remain at the targeted base, as the U.S. military has most likely evacuated or dispersed them in anticipation of further Iranian ballistic-missile attacks.

"""))
    print(token_tracker.summary())