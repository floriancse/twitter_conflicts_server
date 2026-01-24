import ollama
import json

def extract_events_and_geoloc(tweet_text):
    prompt = f"""
    Tu es un analyste OSINT spécialisé en extraction d'événements géopolitiques et militaires à partir de tweets.

    Objectif :
    Extraire uniquement des informations factuelles et relier l'événement principal à un lieu réel identifiable sur une carte.

    Règles STRICTES :
    - N'invente jamais un lieu précis s'il n'est pas explicitement mentionné.
    - Les lieux explicitement cités (villes, usines, régions) sont utilisables comme main_location même pour des annonces de projets industriels, transferts d'équipements ou préparations de production dans un contexte défense.
    - Si le lieu est nommé explicitement et lié à l'action principale (production, lancement, partenariat), utilise "location_type": "explicit" et "confidence": "high".
    - Si plusieurs lieux sont mentionnés, choisis le ou les plus directement liés à l'action principale (ex. sites de production).

    Règle pour zones approximatives :
    - Si le texte décrit une opération ou un événement militaire dans une région connue sans préciser de ville ou base :
      - Choisis un point représentatif central de cette zone.
      - La latitude et longitude doivent correspondre à un endroit réel identifiable (ville, base, mer, etc.).
      - Indique "location_type": "inferred" et ajuste "confidence" en "medium".
    - Ne crée jamais de villes ou lieux fictifs.

    Format JSON attendu :
    {{
      "events": [
        {{
          "event_summary": "description factuelle et concise",
          "main_location": "nom du lieu ou null",
          "location_type": "explicit | inferred | unknown",
          "latitude": 0.0,
          "longitude": 0.0,
          "confidence": "high | medium | low"
        }}
      ]
    }}

    Consignes GPS :
    - Si location_type = "unknown", latitude et longitude doivent être null.
    - Si location_type = "inferred", la confiance ne peut pas être "high".

    IMPORTANT :
    Si aucune information fiable n'est disponible, retourne exactement :
    {{"events":[]}}

    Tweet :
    {tweet_text}
    """


    try:
        response = ollama.chat(
        model='richardyoung/qwen3-14b-abliterated:q5_k_m',  # ← ou qwen2.5:32b-instruct-q4_K_M
        messages=[{'role': 'user', 'content': prompt}],
        format='json',
        options={
            'temperature': 0.0,
            'num_ctx': 4096,
            'top_p': 0.9,          # ← petit bonus pour éviter les réponses trop "plates"
            'repeat_penalty': 1.1, # ← réduit les boucles si le tweet est répétitif
        }
    )
        
        raw_content = response['message']['content'].strip()
        return json.loads(raw_content.replace("```json","").replace("```",""))

    except json.JSONDecodeError as e:
        print("JSON parse error:", e)
        print("Raw response was:", repr(response['message']['content'][:400]))
        return None
    except Exception as e:
        print("Ollama error:", e)
        return None