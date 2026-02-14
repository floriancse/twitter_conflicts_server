"""
Module d'extraction d'événements géopolitiques via LLM
======================================================

Ce module utilise un modèle de langage local (Ollama) pour analyser des tweets OSINT
et extraire automatiquement :
- La nature de l'événement (militaire ou autre)
- Sa localisation géographique (coordonnées GPS)
- Son importance stratégique (échelle 1-5)
- Le niveau de confiance de la géolocalisation

Le modèle utilisé : Qwen3-14B (version abliterated) pour une analyse factuelle sans censure.
"""

import ollama
import json

def extract_events_and_geoloc(tweet_text):
    """
    Analyse un tweet OSINT et extrait les événements géolocalisés via LLM.
    
    Args:
        tweet_text (str): Le texte du tweet à analyser
        
    Returns:
        dict: Dictionnaire JSON contenant la liste des événements extraits avec leurs métadonnées,
              ou None en cas d'erreur de parsing
              
    Format de retour:
        {
          "events": [
            {
              "event_summary": str,
              "typologie": "MIL" | "OTHER",
              "strategic_importance": 1-5,
              "main_location": str | null,
              "location_type": "explicit" | "inferred" | "unknown",
              "latitude": float | null,
              "longitude": float | null,
              "confidence": "high" | "medium" | "low"
            }
          ]
        }
    """
    
    # Construction du prompt structuré pour guider le LLM dans l'extraction
    prompt = f"""
    Tu es un analyste OSINT spécialisé en extraction d'événements géopolitiques et militaires à partir de tweets.

    RÈGLE D’OR (priorité absolue) :
    Dès qu’il y a le mot "waters", "maritime waters", "at sea", "in the sea", "naval", "warship", "navy", "fishing boat" + saisie/interception, "collision", "refueling", ou toute action clairement maritime → le point DOIT être placé **en mer**, même si un pays, une île ou un continent est mentionné juste avant ou après. Ne place jamais le point sur la terre ferme dans ce cas.

    Objectif :
    Extraire uniquement des informations factuelles, classifier l'événement et relier l'action à un lieu réel. Évaluer son importance stratégique.

    Catégories de Typologie (CHOISIR UNIQUEMENT PARMI) :
    - MIL : Seulement si le texte mentionne explicitement un bombardement, une frappe de missile/drone, ou un combat direct avéré (avec preuve ou source claire).
    - OTHER : Tout autre événement.

    Règles STRICTES de Géolocalisation (appliquer dans cet ordre de priorité) :

    1. Détection maritime (priorité absolue)
      Si le texte contient un des mots-clés suivants ou une situation navale claire :
      - maritime waters, territorial waters, in waters, at sea, naval, warship, navy, fishing boat seized/intercepted, collision, refueling, exercice naval, etc.
      → Toujours traiter comme localisation maritime.
      → Placement obligatoire : point **uniquement dans l’eau** (jamais sur terre).
      → Choisis un point représentatif dans la zone aquatique concernée, le plus proche possible de la région / pays nommé.

      Exemples de coordonnées à utiliser / s’inspirer :
      - Japanese / East China Sea maritime waters     → 34.5, 138.5
      - waters near South America                     → -15.0, -38.0 (Atlantique large du Brésil)
      - South China Sea                               → 10.0, 115.0
      - Black Sea                                     → 43.0, 34.0
      - Red Sea                                       → 18.0, 38.0
      - Philippine Sea                                → 20.0, 135.0
      - Mediterranean Sea (général)                   → 35.0, 18.0
      - Persian Gulf                                  → 26.0, 52.0
      - near Taiwan Strait                            → 24.0, 120.0

    2. Localisation terrestre explicite
      Seulement si AUCUN mot maritime n’est présent ET qu’un pays/ville/base est nommé clairement.
      → Point à l’intérieur des frontières du pays / de la ville.

    3. Cas "near [pays/continent]" sans indication maritime
      → Point terrestre central du pays/continent.

    - location_type : "inferred" dès qu’on utilise une zone maritime large ou une approximation.
    - confidence   : "medium" pour toute localisation maritime non précisément chiffrée (ex: pas de coordonnées exactes dans le tweet).

    Critères d’Importance Stratégique (Note de 1 à 5) :
    1 : Événement local/mineur (escarmouche isolée, déclaration de routine).
    2 : Événement tactique (mouvement de troupes local, frappe sur cible secondaire).
    3 : Événement opérationnel (perte d'infrastructure clé, changement de ligne de front, incident naval significatif).
    4 : Événement stratégique (changement de politique majeure, livraison d’armes lourdes, escalade régionale).
    5 : Événement critique mondial (déclaration de guerre, frappe nucléaire, chute d’un gouvernement).

    Exemples de sortie attendue :

    Tweet : "Japanese navy has seized a Chinese fishing boat which refused to stop for inspection in Japanese maritime waters-WSJ"
    Sortie :
    {{
      "events": [{{
        "event_summary": "La marine japonaise a saisi un bateau de pêche chinois qui refusait de s'arrêter dans les eaux maritimes japonaises",
        "typologie": "OTHER",
        "strategic_importance": 3,
        "main_location": "Japanese maritime waters",
        "location_type": "inferred",
        "latitude": 34.5,
        "longitude": 138.5,
        "confidence": "medium"
      }}]
    }}

    Tweet : "A U.S. warship and a Navy supply vessel collided during refueling in waters near South America-Reuters"
    Sortie :
    {{
      "events": [{{
        "event_summary": "Collision entre un navire de guerre américain et un navire ravitailleur de la Navy lors d'un ravitaillement en mer près de l'Amérique du Sud",
        "typologie": "OTHER",
        "strategic_importance": 3,
        "main_location": "waters near South America",
        "location_type": "inferred",
        "latitude": -15.0,
        "longitude": -38.0,
        "confidence": "medium"
      }}]
    }}

    Format JSON attendu (strict) :
    {{
      "events": [
        {{
          "event_summary": "description factuelle et concise",
          "typologie": "MIL | OTHER",
          "strategic_importance": 1 | 2 | 3 | 4 | 5,
          "main_location": "nom du lieu ou null",
          "location_type": "explicit | inferred | unknown",
          "latitude": float ou null,
          "longitude": float ou null,
          "confidence": "high | medium | low"
        }}
      ]
    }}

    IMPORTANT : Si aucune information fiable n'est disponible ou si le tweet n'est pas informatif, retourne : {{"events":[]}}

    Tweet à analyser :
    {tweet_text}
    """

    try:
        # Appel au modèle LLM local via Ollama
        response = ollama.chat(
            model='richardyoung/qwen3-14b-abliterated:q5_k_m',  # Modèle 14B quantifié (q5_k_m pour équilibre qualité/vitesse)
            messages=[{'role': 'user', 'content': prompt}],
            format='json',  # Force la sortie en JSON valide
            options={
                'temperature': 0.0,        # Température nulle pour des résultats déterministes et factuels
                'num_ctx': 4096,           # Fenêtre de contexte (tokens max)
                'top_p': 0.9,              # Nucleus sampling pour diversité contrôlée
                'repeat_penalty': 1.1,     # Pénalité légère contre les répétitions
            }
        )
        
        # Extraction et parsing de la réponse JSON
        raw_content = response['message']['content'].strip()
        return json.loads(raw_content)

    except json.JSONDecodeError as e:
        # Gestion des erreurs de parsing JSON (réponse mal formée du LLM)
        print("JSON parse error:", e)
        return None
    except Exception as e:
        # Gestion des autres erreurs (connexion Ollama, timeout, etc.)
        print("Ollama error:", e)
        return None