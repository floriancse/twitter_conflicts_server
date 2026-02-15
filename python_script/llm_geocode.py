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
    
    prompt = f"""Tu es un analyste OSINT. Extrais UNIQUEMENT les événements concrets et physiques de ce tweet.

    RÈGLES D'EXTRACTION :

    1. QUOI EXTRAIRE :
      ✓ Attaques (drone, missile, artillerie)
      ✓ Saisies/arraisonnements de navires
      ✓ Mouvements militaires : déploiements navals/aériens, patrouilles, repositionnements tactiques
      ✓ Incidents (collisions, incendies, explosions)
      ✓ Violences (tirs, affrontements)
      ✓ Déclarations politiques (annonces officielles, budgets défense, lois, intentions stratégiques)
      
      ✗ NE PAS extraire : tweets purement informatifs sans événement

    2. GÉOLOCALISATION (règles strictes par type) :

      A) ÉVÉNEMENTS MARITIMES
          Si le tweet contient : "sea", "ocean", "waters", "strait" + un nom de mer/océan
          → Place le point EN MER selon cette table :
          
          Caribbean Sea / Caribbean           → 15.0, -75.0
          South China Sea                     → 10.0, 115.0
          East China Sea                      → 30.0, 125.0
          Black Sea                           → 43.0, 34.0
          Red Sea                             → 18.0, 38.0
          Mediterranean Sea / Mediterranean   → 35.0, 18.0
          Persian Gulf / Arabian Gulf         → 26.0, 52.0
          Gulf of Mexico                      → 25.0, -90.0
          Baltic Sea                          → 58.0, 20.0
          Pacific Ocean                       → 0.0, -140.0
          Atlantic Ocean                      → 25.0, -40.0
          Indian Ocean                        → -10.0, 75.0
          Japanese waters / Japanese maritime → 34.5, 138.5
          
          → location_type: "inferred", confidence: "medium"

      B) NAVIRES EN MOUVEMENT (arriving/departing/coming into/leaving)
          → Place le point dans les EAUX ADJACENTES au port mentionné :
          
          Little Creek, Virginia         → 36.90, -76.00
          Sydney, Australia              → -33.85, 151.25
          Norfolk, USA                   → 36.90, -76.05
          Portsmouth, UK                 → 50.80, -1.10
          
          → location_type: "inferred", confidence: "medium"

      C) LIEUX TERRESTRES (villes, bases, régions)
          → Utilise tes connaissances géographiques pour la ville/base/région :
          
          Volna, Krasnodar, Russia       → 45.35, 37.15
          Odesa, Ukraine                 → 46.48, 30.73
          RAF Mildenhall, UK             → 52.36, 0.49
          RAF Lakenheath, UK             → 52.41, 0.56
          Muwaffaq Salti AB, Jordan      → 31.97, 36.26
          Trapani, Italy                 → 38.02, 12.50
          Eastern Poland (zone)          → 51.0, 23.0
          Tehran, Iran                   → 35.70, 51.42
          
          → location_type: "explicit", confidence: "high"

      D) COORDONNÉES FOURNIES DANS LE TWEET
          Si le tweet contient déjà des coordonnées GPS (ex: "35.733017, 51.494024")
          → Utilise-les directement, confidence: "high"

    3. TYPOLOGIE :
      - MIL : Attaque, bombardement, frappe, tir, combat, explosion militaire
      - POL : Déclaration politique, annonce officielle, budget défense, loi militaire, intention stratégique
      - MOVE : Déploiement naval/aérien, patrouille, repositionnement tactique, arrivée/départ de navire militaire, vol de surveillance
      - OTHER : Tout le reste (saisie civile, incident non-militaire, accident)

    4. GÉOLOCALISATION DES ÉVÉNEMENTS POLITIQUES (POL) :
      → Géolocalise à la CAPITALE du pays concerné par la déclaration
      → Si plusieurs pays mentionnés, choisis le pays dont émane la déclaration
      
      Capitales principales :
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

    5. IMPORTANCE (1-5) :
      1: Incident mineur local, déclaration de routine
      2: Événement tactique (mouvement routine, petite frappe, annonce mineure)
      3: Événement opérationnel (attaque infrastructure, déploiement significatif, budget important)
      4: Escalade stratégique (attaque massive, changement doctrine, annonce majeure)
      5: Crise mondiale (guerre déclarée, frappe nucléaire, rupture diplomatique majeure)

    FORMAT JSON (strict) :
    {{
      "events": [
        {{
          "event_summary": "description factuelle courte",
          "typologie": "MIL ou POL ou MOVE ou OTHER",
          "strategic_importance": 1-5,
          "main_location": "nom du lieu",
          "location_type": "explicit ou inferred",
          "latitude": nombre décimal,
          "longitude": nombre décimal,
          "confidence": "high ou medium ou low"
        }}
      ]
    }}

    Si aucun événement → retourne {{"events":[]}}

    EXEMPLES :

    Tweet: "US special forces boarded and seized the Veronica III oil tanker in the Caribbean Sea."
    {{
      "events": [{{
        "event_summary": "Saisie du pétrolier Veronica III par forces spéciales US en Mer des Caraïbes",
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
        "event_summary": "Le président Lai annonce un renforcement de la défense avec un budget de 40 milliards en discussion",
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
        "event_summary": "Déploiement de 5 KC-135 et 6 F-35A depuis RAF Mildenhall vers la Jordanie",
        "typologie": "MOVE",
        "strategic_importance": 3,
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
        "event_summary": "Navire de transport expéditionnaire USNS Point Loma arrive à Little Creek",
        "typologie": "MOVE",
        "strategic_importance": 2,
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
        "event_summary": "Patrouille NATO E-3A et A330 MRTT en soutien de F-35A au-dessus de la Pologne orientale",
        "typologie": "MOVE",
        "strategic_importance": 3,
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
        "event_summary": "Attaque de drone sur le village de Volna, incendie en cours",
        "typologie": "MIL",
        "strategic_importance": 3,
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
        "event_summary": "Navire poseur de câbles CS Decisive arrive à Sydney",
        "typologie": "OTHER",
        "strategic_importance": 2,
        "main_location": "Sydney waters, Australia",
        "location_type": "inferred",
        "latitude": -33.85,
        "longitude": 151.25,
        "confidence": "medium"
      }}]
    }}

    TWEET À ANALYSER :
    {tweet_text}
    """

    try:
        response = ollama.chat(
            model='richardyoung/qwen3-14b-abliterated:q5_k_m',
            messages=[{'role': 'user', 'content': prompt}],
            format='json',
            options={
                'temperature': 0.0,
                'num_ctx': 4096,
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

