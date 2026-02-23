"""
Module d'extraction d'événements géopolitiques via LLM (Groq)
=============================================================
"""

import json
import os
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

client = Groq(api_key=os.environ.get("GROQ_API_KEY"))

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
            Iran-Pakistan border → "Saravan Border Area, Iran-Pakistan"
     → location_type: "inferred", confidence: "medium", approximate centroid lat/lon.

  D) POLITICAL EVENTS: Geolocate at the capital of the country making the declaration.
     → location_type: "explicit", confidence: "high", precise lat/lon.

  E) COORDINATE PRECISION (mandatory):
     - Always 3 decimal places.
     - Precise known locations: use accurate coordinates.
     - Approximate locations: use plausible centroid.
     Reference coordinates:
       "Kyiv, Ukraine"          → 50.450, 30.523
       "Taipei, Taiwan"         → 25.047, 121.543
       "RAF Mildenhall, UK"     → 52.361, 0.486
       "Pyongyang, North Korea" → 39.019, 125.754
       "Langley AFB, Virginia"  → 37.082, -76.360
       "Little Creek, Virginia" → 36.922, -76.181
       "Atlantic Ocean"         → 20.000, -35.000
       "South China Sea"        → 15.000, 114.000
       "Strait of Hormuz"       → 26.500, 56.500
       "Caribbean Sea"          → 15.000, -75.000
       "Eastern Ukraine"        → 48.500, 37.500
       "Eastern Poland"         → 52.000, 23.500
       "Iran-Pakistan border"   → 27.500, 62.000
       "Sahel region"           → 15.000, 5.000

  F) UNKNOWN LOCATION: If location cannot be determined:
     → location_name: null, location_type: "unknown", confidence: "low", lat: null, lon: null

3. TYPOLOGY:
  MIL: Attack, bombing, strike, shooting, combat, explosion
  POL: Political declaration, official announcement, defense budget, strategic intention
  MOVE: Naval/air deployment, ship/aircraft arrival or departure, surveillance flight
  OTHER: Civilian seizure, non-military incident, accident

4. IMPORTANCE (1-5, be conservative):
  1: Minor local incident, routine movement, routine declaration
  2: Standard tactical event (patrol, small strike, minor announcement)
  3: Notable event (infrastructure attack, multi-unit deployment, significant budget)
  4: Major strategic escalation (massive attack, doctrine change, diplomatic rupture)
  5: Exceptional global crisis (war declared, nuclear strike, regional collapse)

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
    """
    Analyse un tweet OSINT et extrait les événements géolocalisés via Groq.

    Args:
        tweet_text (str): Le texte du tweet à analyser

    Returns:
        dict | None: Dictionnaire JSON contenant la liste des événements, ou None en cas d'erreur
    """

    models_to_try = [
        "llama-3.3-70b-versatile",
        "qwen/qwen3-32b",              
    ]

    for model in models_to_try:
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": f"Analyze this tweet and return a JSON object:\n{tweet_text}"}
                ],
                response_format={"type": "json_object"},
                temperature=0.0,
                max_tokens=1024,
                top_p=1.0,
            )

            raw_content = response.choices[0].message.content.strip()
            if not raw_content:
                continue

            return json.loads(raw_content)

        except Exception as e:
            print(f"[INFO] Model {model} failed: {str(e)} → trying next model")
            continue

    return None