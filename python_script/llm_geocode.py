"""
Module d'extraction d'événements géopolitiques via LLM (Ollama local)
=============================================================
"""

import json
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:11434/v1",
    api_key="ollama",  
)

SYSTEM_PROMPT = """You are an OSINT analyst. Respond ONLY in English. ALL fields must be in English.
Extract ONLY concrete physical events from tweets.

1. WHAT TO EXTRACT:
  EXTRACT: Attacks, ship seizures, military movements, incidents, violence, political declarations.
  SKIP: Purely informational tweets without events.

2. GEOLOCATION RULES:

  PRECISION HIERARCHY: installation/base/port > city > region > country > sea area > null
  Never use a higher level if a more specific one is explicitly mentioned.

  CRITICAL RULES:
  - Use ONLY locations explicitly named in the tweet.
  - Do NOT infer location from actor names (e.g. "Islamic State" ≠ Middle East).
  - "en route to X" or "heading to X" does NOT make X the event location.

  A) MARITIME: Use sea/ocean/strait/gulf name in English (e.g. "Caribbean Sea", "Strait of Hormuz").
     → location_type: "inferred", confidence: "medium", approximate centroid lat/lon.
     
  B) LAND - EXPLICIT (cities, bases, installations explicitly named):
     → location_name format (hierarchical, comma-separated, most specific first):
       - City/base/installation: "City, Region, Country" (e.g. "Volna, Krasnodar, Russia")
       - If no region known: "City, Country" (e.g. "Sevastopol, Ukraine")
       - Country only if nothing more specific: "Russia"
     → Disputed territories: use the internationally recognized country name.
       e.g. Sevastopol, Crimea → "Sevastopol, Ukraine" (not "Russia")
     → location_type: "explicit", confidence: "high", precise lat/lon.

  C) LAND - VAGUE (borders, remote areas, regions):
    → location_name format: "Descriptor, Country" or "Descriptor, Country1-Country2"
      e.g. "Eastern Ukraine" → "Eastern Ukraine, Ukraine"
            Iran-Pakistan border → "Iran-Pakistan Border Area, Iran-Pakistan"
            Iran-Azerbaijan border → "Iran-Azerbaijan Border Area, Iran-Azerbaijan"
    → location_type: "inferred", confidence: "medium", approximate centroid lat/lon.

  D) POLITICAL EVENTS: Geolocate at the capital of the country making the declaration.
     → location_type: "explicit", confidence: "high", precise lat/lon.

  E) COORDINATE PRECISION (mandatory):
   - Always 3 decimal places.
   - Use your own geographic knowledge to estimate coordinates.
   - For bilateral borders: compute the centroid of the actual shared border segment.
   - For obscure locations: use the nearest identifiable place you are confident about.
   - Only set lat/lon to null if the location is entirely unidentifiable geographically.
   - Do NOT interpolate from unrelated locations.

  F) UNKNOWN OR OBSCURE LOCATION:
   - If the location is a known place (city, base, installation): use precise coordinates.
   - If the location is obscure (small village, industrial site, military base not in common knowledge):
     return the coordinates of the nearest major city or region you are confident about,
     and set confidence to "medium" and location_type to "inferred".
   - Only set lat/lon to null if the location is entirely unidentifiable geographically.
   - For bilateral borders not listed above, compute the centroid of the shared border segment using the two countries' general geography — do NOT reuse a different border's coordinates

  3. TYPOLOGY:
    MIL: Attack, bombing, strike, shooting, combat, explosion
    POL: Political declaration, official announcement, defense budget, strategic intention
    MOVE: Naval/air deployment, ship/aircraft arrival or departure, surveillance flight, airspace restriction
    OTHER: Civilian seizure, non-military incident, accident

  4. TENSION SCORE (0-5) — evaluate the potential to generate or escalate geopolitical tension:
    0: No tension generated — purely routine, administrative, or logistical (e.g. scheduled exercise, budget vote)
    1: Minimal tension — minor local incident, routine patrol, low-stakes declaration
    2: Low tension — standard tactical event, small skirmish, minor NOTAM in a calm zone
    3: Moderate tension — notable event with escalation potential (infrastructure attack, NOTAM in a tension zone,
      significant deployment, airspace restriction near a conflict area)
    4: High tension — major escalation (massive strike, doctrine shift, diplomatic rupture, large naval deployment)
    5: Critical — exceptional event threatening regional or global stability (war declaration, WMD use, state collapse)

    Be conservative: most events score 1-3. Reserve 4-5 for genuinely exceptional events.

  5. OUTPUT FORMAT — ALL FIELDS MANDATORY:
{
  "events": [
    {
      "summary_text": "concise 1-sentence analytical summary, not a copy of the tweet",
      "typology": "MIL or POL or MOVE or OTHER",
      "strategic_importance": 1-5,
      "location_name": "English string or null",
      "location_type": "explicit or inferred or unknown",
      "confidence": "high or medium or low",
      "lat": null or float with exactly 3 decimal places,
      "lon": null or float with exactly 3 decimal places
    }
  ]
}

If no extractable event → return {"events": []}
location_name must ALWAYS be in English (Latin script only).
lat/lon must be null ONLY when location is truly unknown."""

def extract_events_and_geoloc(tweet_text: str) -> dict | None:
    try:
        response = client.chat.completions.create(
            model="richardyoung/qwen3-14b-abliterated:q5_k_m",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
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
            max_tokens=1024,
        )

        raw_content = response.choices[0].message.content.strip()

        if not raw_content:
            return {"events": []}

        return json.loads(raw_content)

    except Exception as e:
        print(str(e))
        return None