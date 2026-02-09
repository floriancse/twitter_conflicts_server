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

    Objectif :
    Extraire uniquement des informations factuelles, classifier l'événement et relier l'action à un lieu réel évaluer son importance stratégique.

    Catégories de Typologie (CHOISIR UNIQUEMENT PARMI) :
    - MIL : Seulement si le texte mentionne explicitement un bombardement, une frappe de missile/drone, ou un combat direct avéré (avec preuve ou source claire).
    - OTHER : Tout autre événement.

    Règles STRICTES de Géolocalisation :
    - N'invente jamais un lieu précis s'il n'est pas explicitement mentionné.
    - Si le lieu est nommé explicitement (ville, base) : "location_type": "explicit", "confidence": "high".
    - Si le texte décrit une opération dans une zone connue sans ville précise (ex : Middle East, Northern Atlantic): Choisis un point central représentatif, "location_type": "inferred", "confidence": "medium".
    - Si aucun lieu n'est identifiable : "location_type": "unknown", "latitude": null, "longitude": null.
    - Pour les localisations maritimes (océan, mer, golfe, canal, ou toute étendue d'eau mentionnée comme "over the Black Sea", "in the Atlantic Ocean", etc.) : Choisis toujours un point central dans les eaux, pas sur terre. Utilise des coordonnées approximatives au milieu de la zone aquatique concernée (ex : pour la Mer Noire, environ 43.0 latitude, 34.0 longitude). "location_type": "inferred" si non explicite, "confidence": "medium".
    - Pour les localisations terrestres connues (pays explicitement donné) : Choisis toujours un point dans le pays, à l'intérieur de ses frontières.

    Critères d'Importance Stratégique (Note de 1 à 5) :
        1 : Événement local/mineur (escarmouche isolée, déclaration de routine).
        2 : Événement tactique (mouvement de troupes local, frappe sur cible secondaire).
        3 : Événement opérationnel (perte d'infrastructure clé, changement de ligne de front).
        4 : Événement stratégique (changement de politique majeure, livraison d'armes lourdes, escalade régionale).
        5 : Événement critique mondial (déclaration de guerre, frappe nucléaire, chute d'un gouvernement).
        
    Format JSON attendu :
    {{
      "events": [
        {{
          "event_summary": "description factuelle et concise",
          "typologie": "MIL | OTHER",
          "strategic_importance": 1 | 2 | 3 | 4 | 5,
          "main_location": "nom du lieu ou null",
          "location_type": "explicit | inferred | unknown",
          "latitude": 0.0,
          "longitude": 0.0,
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