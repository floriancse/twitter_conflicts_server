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

    NOMINATIM_QUERY FORMAT RULE (mandatory):

        nominatim_query MUST strictly follow "City, Country" or "Region, Country".
        
        NEVER include: facility names, unit designations, prepositions ("near", "at"),
        or installation-level detail. Use the resolution ladder:
        1. Named city/town   → "Donetsk, Ukraine"
        2. Named region      → "Khuzestan, Iran"  
        3. Country capital   → "Moscow, Russia"
        4. Sea areas         → lat/lon from fallback table, nominatim_query = null

        The installation name belongs ONLY in summary_text, never in nominatim_query.

        Good: "Novokuybyshevsk, Russia"  ← event at the AVT-6 refinery unit
        Bad:  "Novokuybyshevsk refinery, Russia"
        Bad:  "AVT-6 unit Novokuybyshevsk, Russia"
        Bad:  "Tochmash plant near Donetsk airport, Ukraine"
        Good: "Donetsk, Ukraine"

    COORDINATES RULE:

        If the location is identified but coordinates, you MUST estimate the decimal coordinates based on your internal knowledge (e.g., center of the city/region or capital of the country).
        NEVER return null if a nominatim_query has been successfully identified.

    IMPLICIT LOCATION RULE: If a tweet names a country, facility, or well-known site without an explicit "in [place]" phrase, you MAY infer the location from that entity.
    Example: "Ukraine destroyed an ammunition depot at the Tochmach plant near Donetsk airport" → nominatim_query = "Donetsk, Ukraine", confidence = "high".

    If NO location can be determined even by inference (e.g. pure political opinion with no target/actor location) → lat/lon = null, confidence = "low".
    Note : - Attribute the Strait of Hormuz to Oman (e.g. Strait of Hormuz, Oman), 
           - Do not attribut Persian Gulf to any country (e.g. Persian Gulf) 

3. TYPOLOGY — apply the FIRST matching rule in order:
MIL: A kinetic event that has ALREADY HAPPENED: attack, bombing, strike, shooting, combat, explosion, drone operation.
     REQUIRES a past or present tense action verb ("destroyed", "struck", "exploded", "fired").
     NEVER use MIL for plans, discussions, intentions, deployments, or future operations.

POL: Any information, discussion, plan, declaration, or decision that has NOT yet resulted in physical action:
     political statements, official announcements, defense budget, strategic intentions, threats, sanctions,
     negotiations, intelligence reports, planned operations, arms deals not yet delivered.
     USE POL when the tweet describes what actors "discussed", "plan to", "consider", "may", "could", "will".

MOVE: A confirmed physical repositioning of military assets that has ALREADY OCCURRED:
      naval/air deployment, ship or aircraft arrival/departure, confirmed troop movement,
      surveillance flight, airspace restriction enforcement, confirmed arms delivery.
      MOVE = action confirmed, but not yet combat.

OTHER: Civilian seizure, non-military incident, accident, humanitarian event.

DECISION RULE — when in doubt between MIL and POL:
→ If the event is PLANNED, DISCUSSED, or POTENTIAL → POL
→ If the event ALREADY HAPPENED physically → MIL or MOVE

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
      "summary_text": "Concise 1-sentence analytical summary. MUST explicitly mention: (1) the actor country or force, (2) the weapon or means used if identifiable, (3) the target country or entity, (4) what was physically struck. Example: 'Israeli Air Force conducted bombing airstrikes on Iranian ballistic missile infrastructure in Khuzestan, Iran.'",
      "typology": "MIL | POL | MOVE | OTHER",
      "strategic_importance": 1–5,
      "nominatim_query": "Nominatim-ready query string (e.g. 'Donetsk, Ukraine')",
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
            model="qwen36-fixed",
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
            max_tokens=8192,
        )

        raw_content = response.choices[0].message.content.strip()

        if not raw_content:
            return {"events": []}

        return json.loads(raw_content)

    except Exception as e:
        print(str(e))
        return None