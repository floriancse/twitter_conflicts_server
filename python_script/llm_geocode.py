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

  2. GEOLOCATION:
    - Use ONLY locations explicitly named in the tweet.
    - Take the most specific location mentioned (installation > city > region > country > sea area).
    - If only a country is mentioned (no city or region), use its capital coordinates.
    - For sea areas: always verify mentally that the coordinates are surrounded by water, not on a coastline or land mass.
    - From location_name, provide your best lat/lon estimate.

    Sea area fallback coordinates (use ONLY when tweet names exactly one of these sea areas/regions and nothing more specific):
    "eastern mediterranean sea": (34.5, 32.0)
    "mediterranean sea":        (35.0, 18.0)
    "strait of hormuz":         (26.5, 56.5)
    "persian gulf":             (27.0, 51.0)
    "red sea":                  (20.0, 38.0)
    "black sea":                (43.0, 34.0)
    "south china sea":          (12.0, 113.0)
    "taiwan strait":            (24.5, 119.5)
    "gulf of aden":             (12.5, 47.0)
    "arabian sea":              (17.0, 65.0)
    "baltic sea":               (58.0, 19.0)
    "north sea":                (56.0, 3.0)
    "bering sea":               (58.0, -175.0)
    "english channel":          (50.5, 1.0)
    "strait of gibraltar":      (35.9, -5.4)

    When using a fallback: confidence = "medium" at best (unless the tweet is very clear).
     
  3. TYPOLOGY:
    MIL: Attack, bombing, strike, shooting, combat, explosion
    POL: Political declaration, official announcement, defense budget, strategic intention
    MOVE: Naval/air deployment, ship/aircraft arrival or departure, surveillance flight, airspace restriction
    OTHER: Civilian seizure, non-military incident, accident

  4. TENSION SCORE (0–5) — geopolitical escalation potential:
    0: Routine/administrative
    1: Minor local incident, routine patrol
    2: Small skirmish, standard tactical event
    3: Notable escalation risk (infrastructure attack, major deployment, airspace restriction in tension zone)
    4: Major escalation (massive strike, doctrine shift, large naval deployment)
    5: Exceptional threat to regional/global stability (war declaration, WMD use)
    
    Be conservative: most events score 1–3.

  5. OUTPUT FORMAT — ALL FIELDS MANDATORY:
    {
      "events": [
        {
          "summary_text": "concise 1-sentence analytical summary.",
          "typology": "MIL | POL | MOVE | OTHER",
          "strategic_importance": 1–5,
          "location_name": "Most specific English name, comma-separated (e.g. 'Kyiv, Ukraine')",
          "confidence": "high | medium | low",
          "lat": float or null,
          "lon": float or null
        }
      ]
    }

  If no extractable event → return {"events": []}"""

def extract_events_and_geoloc(tweet_text: str) -> dict | None:
    try:
        response = client.chat.completions.create(
            model="mistral-small:24b",
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