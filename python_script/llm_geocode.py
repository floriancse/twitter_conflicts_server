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
Extract concrete geopolitical events from tweets.

  0. MANDATORY PRE-CHECK — Run this BEFORE extracting anything:
    Ask yourself:
      a) Does this tweet describe a REAL event (attack, movement, incident, declaration, threat)?
      b) Is there an identifiable actor (country, group, military force, official)?
      c) Can a location be determined — either explicitly stated OR reasonably inferred from named entities (country, facility, city, region)?

    Return {"events": []} ONLY if ALL of the following are true:
      - No concrete event or action is described (e.g. pure metadata, single words, retweet headers)
      - OR no actor whatsoever can be identified
      - OR the content is clearly satirical, a joke, or a question with no factual claim

    DO NOT reject a tweet simply because the location is implicit — if a named facility, country, or entity implies a location, use it.

  1. WHAT TO EXTRACT:
    EXTRACT: Attacks, strikes, explosions, ship seizures, military movements, deployments, political declarations, threats, sanctions, arms transfers, drone operations, airspace incidents.
    SKIP: Pure social media metadata (e.g. "Source:", "Thread:", "Breaking:" with no content), tweets with zero factual claim.

  2. GEOLOCATION:
    Priority order (most specific wins):
      1. Named installation or facility (e.g. "Tochmash plant near Donetsk airport" → use facility coords)
      2. Named city or district
      3. Named region or province
      4. Named country → use its capital coordinates
      5. Named sea area → use fallback table below

    IMPLICIT LOCATION RULE: If a tweet names a country, facility, or well-known site without an explicit "in [place]" phrase, you MAY infer the location from that entity.
    Example: "Ukraine destroyed an ammunition depot at the Tochmash plant near Donetsk airport" → location_name = "Tochmash plant, Donetsk, Ukraine", confidence = "high".
    Example: "UAE struck an Iranian desalination plant" → location_name = "Iran" → use Tehran coords, confidence = "medium".

    For sea areas: always verify mentally that coordinates are surrounded by water.

    Sea area fallback coordinates:
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

    If truly no location can be determined even by inference → lat/lon = null, confidence = "low".
     
  3. TYPOLOGY:
    MIL: Attack, bombing, strike, shooting, combat, explosion, drone operation
    POL: Political declaration, official announcement, defense budget, strategic intention, threat, sanction
    MOVE: Naval/air deployment, ship/aircraft arrival or departure, surveillance flight, airspace restriction, arms transfer
    OTHER: Civilian seizure, non-military incident, accident

  4. TENSION SCORE (0–5) — geopolitical escalation potential:
    0: Routine/administrative
    1: Minor local incident, routine patrol
    2: Small skirmish, standard tactical event
    3: Notable escalation risk (infrastructure attack, major deployment, airspace restriction in tension zone)
    4: Major escalation (massive strike, doctrine shift, large naval deployment, cross-border attack between states)
    5: Exceptional threat to regional/global stability (war declaration, WMD use, attack on a nuclear power)
    
    Be conservative: most events score 1–3. Cross-border state-on-state strikes (e.g. UAE striking Iran) score 4–5.

  5. CONFIDENCE CALIBRATION:
    "high":   Location and event are explicit and unambiguous in the tweet text.
    "medium": Location is inferred from named entities (facility, country), or event details are partially unverified.
    "low":    Location is entirely implicit or event claim is speculative/unconfirmed.

  6. OUTPUT FORMAT — ALL FIELDS MANDATORY:
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