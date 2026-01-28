import ollama
import json

def extract_events_and_geoloc(tweet_text):
    # Liste des typologies pour référence (peut être mise en commentaire ou utilisée pour validation)
    # MIL-STRIKE, MIL-MOVEMENT, POL-DIPLO, INFRA-DAMAGE, INDUSTRIAL-DEF, CIV-IMPACT, OTHER
    
    prompt = f"""
    Tu es un analyste OSINT spécialisé en extraction d'événements géopolitiques et militaires à partir de tweets.

    Objectif :
    Extraire uniquement des informations factuelles, classifier l'événement et relier l'action à un lieu réel évaluer son importance stratégique.

    Catégories de Typologie (CHOISIR UNIQUEMENT PARMI) :
    - MIL : Seulement si le texte mentionne explicitement un bombardement, une frappe de missile/drone, ou un combat direct avéré (avec preuve ou source claire).
    - OTHER : Tout autre événement.

    Règles STRICTES de Géolocalisation :
    - N'invente jamais un lieu précis s'il n'est pas explicitement mentionné.
    - Si le lieu est nommé explicitement (ville, base, région) : "location_type": "explicit", "confidence": "high".
    - Si le texte décrit une opération dans une zone connue sans ville précise : Choisis un point central représentatif, "location_type": "inferred", "confidence": "medium".
    - Si aucun lieu n'est identifiable : "location_type": "unknown", "latitude": null, "longitude": null.

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
        # Nettoyage sécurisé pour ne garder que le JSON
        return json.loads(raw_content)

    except json.JSONDecodeError as e:
        print("JSON parse error:", e)
        return None
    except Exception as e:
        print("Ollama error:", e)
        return None