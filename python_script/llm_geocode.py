"""
Module d'extraction d'événements géopolitiques via LLM (version simplifiée)
===========================================================================

Prompt allégé pour éviter les hallucinations géographiques.
Focus sur l'extraction d'événements concrets uniquement.
"""

import ollama
import json

def extract_events_and_geoloc(tweet_text):
    """
    Analyse un tweet OSINT et extrait les événements géolocalisés via LLM.
    
    Args:
        tweet_text (str): Le texte du tweet à analyser
        
    Returns:
        dict: Dictionnaire JSON contenant la liste des événements extraits
    """
    
    prompt = f"""You are an OSINT analyst. Extract ONLY concrete physical events from this tweet.

    1. WHAT TO EXTRACT:
      ✓ Attacks (drone, missile, artillery)
      ✓ Ship seizures/boardings
      ✓ Military movements: naval/air deployments, patrols, tactical repositioning
      ✓ Incidents (collisions, fires, explosions)
      ✓ Violence (shootings, clashes)
      ✓ Political declarations (official announcements, defense budgets, laws, strategic intentions)
      
      ✗ DO NOT extract: purely informational tweets without events

    2. GEOLOCATION (strict rules by type):

      CRITICAL: Use ONLY locations explicitly named in the tweet
      Do NOT infer location from actor names (e.g., "Islamic State" does not mean Middle East)

      A) MARITIME EVENTS
          If tweet contains: "sea", "ocean", "waters", "strait", "gulf" + sea/ocean name
          → Place point AT SEA according to this table:
          
          Caribbean Sea / Caribbean           → 15.0, -75.0
          South China Sea                     → 10.0, 115.0
          East China Sea                      → 30.0, 125.0
          Black Sea                           → 43.0, 34.0
          Red Sea                             → 18.0, 38.0
          Mediterranean Sea / Mediterranean   → 35.0, 18.0
          Persian Gulf / Arabian Gulf         → 26.0, 52.0
          Gulf of Oman                        → 25.0, 58.0    
          Strait of Hormuz / Hormuz           → 26.6, 56.5    
          Gulf of Mexico                      → 25.0, -90.0
          Baltic Sea                          → 58.0, 20.0
          Pacific Ocean                       → 0.0, -140.0
          Atlantic Ocean                      → 25.0, -40.0
          Indian Ocean                        → -10.0, 75.0
          Japanese waters / Japanese maritime → 34.5, 138.5
          
          → location_type: "inferred", confidence: "medium"

      B) SHIPS IN MOVEMENT (arriving/departing/coming into/leaving)
          → Place point SLIGHTLY INLAND from the port to ensure polygon intersection:
          
          Little Creek, Virginia         → 36.92, -76.02  # Décalé vers terre
          Sydney, Australia              → -33.87, 151.21  # Décalé vers terre
          Norfolk, USA                   → 36.92, -76.08   # Décalé vers terre
          Portsmouth, UK                 → 50.82, -1.08    # Décalé vers terre
          
          → location_type: "inferred", confidence: "medium"

      C) LAND LOCATIONS (cities, bases, regions)
          → Use ONLY the geographic locations EXPLICITLY mentioned in the tweet
          → Do NOT infer locations based on actors/groups mentioned
          → If a city/region is mentioned, use your geographic knowledge:
      
          Examples:
          Volna, Krasnodar, Russia       → 45.35, 37.15
          Odesa, Ukraine                 → 46.48, 30.73
          Maputo, Mozambique             → -25.97, 32.58
          Pemba, Mozambique              → -12.97, 40.52
          RAF Mildenhall, UK             → 52.36, 0.49
          RAF Lakenheath, UK             → 52.41, 0.56
          Muwaffaq Salti AB, Jordan      → 31.97, 36.26
          Trapani, Italy                 → 38.02, 12.50
          Eastern Poland (zone)          → 51.0, 23.0
          Tehran, Iran                   → 35.70, 51.42
          Western Ukraine (zone)         → 49.5, 24.0
          Central Ukraine (zone)         → 49.0, 31.5
          Eastern Ukraine / Donbas       → 48.0, 38.0
          Zaporizhzhia region, Ukraine   → 47.5, 35.5
          Kherson region, Ukraine        → 46.5, 32.5
          Kharkiv, Ukraine               → 49.99, 36.23

          
          → location_type: "explicit", confidence: "high"

      D) COORDINATES PROVIDED IN TWEET
          If tweet already contains GPS coordinates (e.g., "35.733017, 51.494024")
          → Use them directly, confidence: "high"

    3. TYPOLOGY:
      - MIL: Attack, bombing, strike, shooting, combat, military explosion
      - POL: Political declaration, official announcement, defense budget, military law, strategic intention
      - MOVE: Naval/air deployment, patrol, tactical repositioning, military ship arrival/departure, surveillance flight
      - OTHER: Everything else (civilian seizure, non-military incident, accident)

    4. GEOLOCATION OF POLITICAL EVENTS (POL):
      → Geolocate at CAPITAL of country concerned by declaration
      → If multiple countries mentioned, choose country from which declaration originates
      
      Main capitals:
      Taiwan (Taipei)           → 25.03, 121.56
      Ukraine (Kyiv)            → 50.45, 30.52
      Russia (Moscow)           → 55.75, 37.62
      USA (Washington DC)       → 38.90, -77.04
      China (Beijing)           → 39.90, 116.40
      Iran (Tehran)             → 35.70, 51.42
      Israel (Jerusalem)        → 31.78, 35.22
      UK (London)               → 51.51, -0.13
      France (Paris)            → 48.86, 2.35
      Germany (Berlin)          → 52.52, 13.40
      Japan (Tokyo)             → 35.68, 139.65
      South Korea (Seoul)       → 37.57, 126.98
      North Korea (Pyongyang)   → 39.03, 125.75
      
      → location_type: "explicit", confidence: "high"

    5. IMPORTANCE (1-5) - BE CONSERVATIVE:
      1: Minor local incident, routine ship/aircraft movement, routine declaration
      2: Standard tactical event (patrol, small isolated strike, minor political announcement)
      3: Notable operational event (infrastructure attack, multi-unit deployment, significant defense budget)
      4: Major strategic escalation (massive coordinated attack, military doctrine change, diplomatic rupture)
      5: Exceptional global crisis (war declared, nuclear strike, regional collapse)
      
      By default, start at 1 and increase only if event clearly has strategic impact.

    JSON FORMAT (strict) - ALL FIELDS ARE MANDATORY - Only output ONE JSON about the main event :
    {{
      "events": [
        {{
          "event_summary": "short factual description in English",
          "typologie": "MIL or POL or MOVE or OTHER",
          "strategic_importance": 1-5,
          "main_location": "location name or 'Unknown' if not localizable",
          "location_type": "explicit or inferred or unknown",
          "latitude": decimal number or null,
          "longitude": decimal number or null,
          "confidence": "high or medium or low"
        }}
      ]
    }}

    ⚠️ The "confidence" field is MANDATORY in each event.
    ⚠️ CRITICAL RULE: If you cannot reliably locate the event:
      - main_location: "Unknown"
      - location_type: "unknown"
      - latitude: null
      - longitude: null
      - confidence: "low"
    ⚠️ NEVER use coordinates 0.0, 0.0 (Gulf of Guinea) as default!

    If no event → return {{"events":[]}}

    EXAMPLES:

    Tweet: "US special forces boarded and seized the Veronica III oil tanker in the Caribbean Sea."
    {{
      "events": [{{
        "event_summary": "US special forces seized oil tanker Veronica III in Caribbean Sea",
        "typologie": "OTHER",
        "strategic_importance": 3,
        "main_location": "Caribbean Sea",
        "location_type": "inferred",
        "latitude": 15.0,
        "longitude": -75.0,
        "confidence": "medium"
      }}]
    }}

    Tweet: "Taiwan will strengthen defence efforts, President Lai said. He is trying to pass a 40 billion defence bill."
    {{
      "events": [{{
        "event_summary": "President Lai announces defense strengthening with 40 billion defense bill under discussion",
        "typologie": "POL",
        "strategic_importance": 3,
        "main_location": "Taipei, Taiwan",
        "location_type": "explicit",
        "latitude": 25.03,
        "longitude": 121.56,
        "confidence": "high"
      }}]
    }}

    Tweet: "At least five KC-135R/T Stratotanker departing from RAF Mildenhall heading south, supporting six F-35A from Vermont ANG flying to Jordan."
    {{
      "events": [{{
        "event_summary": "Deployment of 5 KC-135 and 6 F-35A from RAF Mildenhall to Jordan",
        "typologie": "MOVE",
        "strategic_importance": 2,
        "main_location": "RAF Mildenhall to Jordan deployment",
        "location_type": "explicit",
        "latitude": 52.36,
        "longitude": 0.49,
        "confidence": "high"
      }}]
    }}

    Tweet: "Future USNS Point Loma (EPF-15) expeditionary fast transport coming into Little Creek, Virginia - February 14, 2026"
    {{
      "events": [{{
        "event_summary": "Expeditionary fast transport USNS Point Loma arriving at Little Creek",
        "typologie": "MOVE",
        "strategic_importance": 1,
        "main_location": "Little Creek waters, Virginia",
        "location_type": "inferred",
        "latitude": 36.90,
        "longitude": -76.00,
        "confidence": "medium"
      }}]
    }}

    Tweet: "NATO E-3A Sentry AEW&C Aircraft and Airbus A330 MRTT supporting F-35A are airborne over Eastern Poland."
    {{
      "events": [{{
        "event_summary": "NATO E-3A and A330 MRTT patrol supporting F-35A over Eastern Poland",
        "typologie": "MOVE",
        "strategic_importance": 2,
        "main_location": "Eastern Poland",
        "location_type": "inferred",
        "latitude": 51.0,
        "longitude": 23.0,
        "confidence": "medium"
      }}]
    }}

    Tweet: "Emergency services in Krasnodar said firefighters are battling a blaze in Volna village after a drone attack."
    {{
      "events": [{{
        "event_summary": "Drone attack on Volna village, fire in progress",
        "typologie": "MIL",
        "strategic_importance": 2,
        "main_location": "Volna, Krasnodar region, Russia",
        "location_type": "explicit",
        "latitude": 45.35,
        "longitude": 37.17,
        "confidence": "high"
      }}]
    }}

    Tweet: "CS Decisive cable-laying vessel coming into Sydney, Australia - February 13, 2026"
    {{
      "events": [{{
        "event_summary": "Cable-laying vessel CS Decisive arriving at Sydney",
        "typologie": "OTHER",
        "strategic_importance": 1,
        "main_location": "Sydney waters, Australia",
        "location_type": "inferred",
        "latitude": -33.85,
        "longitude": 151.25,
        "confidence": "medium"
      }}]
    }}

    Tweet: "It's likely that the first intercepted projectile was an RM-48U. The second interception appears to be a genuine Iskander-M missile."
    {{
      "events": [{{
        "event_summary": "Interception of two missiles: presumed RM-48U and confirmed Iskander-M",
        "typologie": "MIL",
        "strategic_importance": 2,
        "main_location": "Unknown",
        "location_type": "unknown",
        "latitude": null,
        "longitude": null,
        "confidence": "low"
      }}]
    }}

    TWEET TO ANALYZE:
    {tweet_text}
    """

    try:
        response = ollama.chat(
            model='richardyoung/qwen3-14b-abliterated:q5_k_m',
            messages=[{'role': 'user', 'content': prompt}],
            format='json',
            options={
                'temperature': 0.0,
                'num_ctx': 8192,
                'top_p': 0.9,
                'repeat_penalty': 1.1,
            }
        )
        
        raw_content = response['message']['content'].strip()
        return json.loads(raw_content)

    except json.JSONDecodeError as e:
        print("JSON parse error:", e)
        return None
    except Exception as e:
        print("Ollama error:", e)
        return None