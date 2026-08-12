"""
Module d'extraction d'événements géopolitiques via LLM (Ollama local)
=============================================================
"""

import json
from openai import OpenAI
from datetime import datetime

client = OpenAI(base_url="http://localhost:8081/v1", api_key="x")

SYSTEM_PROMPT_BASE = """You are an OSINT analyst. Respond ONLY in English, ONLY valid JSON, no markdown.
Extract concrete geopolitical events from tweets.
 
Return {"events": []} ONLY if: no concrete event/action is described (pure metadata, single words, retweet headers), OR no actor can be identified, OR the content is satire/a joke/a question with no factual claim.
An implicit location is NOT a reason to skip an event — infer it (see GEOLOCATION).
 
EXTRACT: attacks, strikes, explosions, ship seizures, military movements, deployments, political declarations, threats, sanctions, arms transfers, drone operations, airspace incidents.
SKIP: pure metadata ("Source:", "Thread:", "Breaking:") with no actual content.
 
GEOLOCATION
nominatim_query MUST be "City, Country" or "Region, Country" ONLY — never a facility/unit name or a preposition ("near", "at"). Put facility/installation names in summary_text only. Resolution order (use the first that applies):
1. Named city/town  → "Donetsk, Ukraine"
2. Named region     → "Khuzestan, Iran"
3. Country as an explicit LOCATION marker ("in Russia", "over Iran", "inside Ukraine") → that country's capital, e.g. "Moscow, Russia". Do NOT trigger this from a nationality/origin adjective alone ("a Russian howitzer", "Ukrainian forces") — those describe WHO/WHAT, not WHERE.
4. Named sea/strait  → use the sea's own name as nominatim_query, no country attached (e.g. "Gulf of Oman", "Strait of Hormuz", "Black Sea", "Persian Gulf").
Example: "Ukraine destroyed a depot at the Tochmash plant near Donetsk airport" → nominatim_query = "Donetsk, Ukraine" (facility name dropped, confidence = "high").
If truly no location can be inferred (e.g. pure opinion, no actor/target location) → nominatim_query = null, lat/lon = null, confidence = "low".
 
DIRECTIONAL MODIFIERS
Watch for directional qualifiers attached to a location: "Middle East", "Eastern", "Northern", "Western", "Southern", "Central", "North", "South", "East", "West" (e.g. "eastern Ukraine", "northern Gaza", "southern Lebanon", "western Iran", "Middle East").
- KEEP the directional qualifier in nominatim_query when it is attached to a country/region and no more precise city/town is named — e.g. "eastern Ukraine" → nominatim_query = "Eastern Ukraine" (not just "Ukraine"), "southern Lebanon" → "Southern Lebanon", "Middle East" (as a standalone region, no country given) → "Middle East". Do NOT drop the directional word and fall back to the bare country name; that silently discards real location information.
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
 
is_delayed: "false" if the event happened today/recently (default when date is unclear); "true" if it happened in the past (see TEMPORAL MARKING).
 
OUTPUT — ALL FIELDS MANDATORY, JSON only:
{
  "events": [
    {
      "summary_text": "1 concise sentence naming: (1) actor country/force, (2) weapon/means if identifiable, (3) target country/entity, (4) what was physically struck. E.g. 'Israeli Air Force conducted bombing airstrikes on Iranian ballistic missile infrastructure in Khuzestan, Iran.'",
      "typology": "MIL | POL | MOVE | OTHER",
      "strategic_importance": 1-5,
      "nominatim_query": "'City, Country' string, or null",
      "confidence": "explicit | high | medium | low",
      "lat": float or null,
      "lon": float or null,
      "is_delayed": "true | false"
    }
  ]
}
If no extractable event → return {"events": []}"""
 
TEMPORAL_INSTRUCTIONS = """
 
TEMPORAL MARKING (mandatory):
Using today's date given above: if the tweet is a retrospective, commemoration, anniversary, "on this day", "X years/months ago", or otherwise clearly recalls an event from well before today (rough guide: >~30 days before today, or any explicit anniversary/retrospective framing regardless of how old) → it is HISTORICAL. Plain past-tense reporting of something from the last few days is NOT historical.
For HISTORICAL events, prefix summary_text with "[HISTORICAL EVENT - <date, as precisely as known, e.g. 'July 2024'>] " before the normal summary. If no date is identifiable, use "[HISTORICAL EVENT - date unclear]".
Example: a 2026 post marking "2nd anniversary of the Battle of Tinzaouatène (July 2024)" → summary_text starts with "[HISTORICAL EVENT - July 2024] "."""


def build_system_prompt() -> str:
    """Builds the system prompt with today's date injected, so the model can judge
    how recent or how distant an event is (e.g. anniversary/retrospective posts)."""
    today_str = datetime.now().strftime("%Y-%m-%d")
    date_header = (
        f"Today's date is {today_str}. Use this date as your reference point whenever "
        f"you need to judge how recent, ongoing, or historical an event is.\n\n"
    )
    return date_header + SYSTEM_PROMPT_BASE + TEMPORAL_INSTRUCTIONS


def extract_events_and_geoloc(tweet_text: str) -> dict | None:
    try:
        response = client.chat.completions.create(
            model="qwen3.6-35b-a3b",
            messages=[
                {"role": "system", "content": build_system_prompt()},
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
            extra_body={"chat_template_kwargs": {"reasoning_effort": "low"}}
        )

        raw_content = response.choices[0].message.content.strip()
        
        if not raw_content:
            return {"events": []}

        return json.loads(raw_content)

    except Exception as e:
        print(str(e))
        return None

if __name__ == "__main__":
    print(extract_events_and_geoloc("""
North Korea fired an unidentified ballistic missile towards the Sea of Japan, landing outside the Japanese Exclusive Economic Zone.
"""))