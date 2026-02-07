"""
Parseur de flux RSS Nitter vers JSON
=====================================

Ce module convertit les flux RSS provenant d'une instance Nitter locale
en format JSON structuré pour faciliter le traitement ultérieur.

Fonctionnalités :
- Parsing XML depuis URL ou fichier local
- Nettoyage des balises HTML et CDATA
- Filtrage par auteur (pour isoler les tweets d'un utilisateur spécifique)
- Remplacement automatique des URLs localhost par x.com
- Extraction des URLs d'images

Utilisation :
    parse_to_json("http://localhost:8080/user/rss", "@username")
"""

import xml.etree.ElementTree as ET
import json
import re
import sys
import urllib.request

def extract_images(text):
    """
    Extrait toutes les URLs d'images depuis le texte HTML.
    
    Args:
        text (str): Texte HTML contenant potentiellement des balises <img>
        
    Returns:
        list: Liste des URLs d'images extraites
        
    Exemple:
        '<img src="http://localhost/pic/media.jpg" />' → ['http://x.com/pic/media.jpg']
    """
    if not text:
        return []
    
    # Extraction du contenu CDATA si présent
    text = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', text, flags=re.DOTALL)
    
    # Recherche de toutes les balises img et extraction du src
    img_pattern = r'<img[^>]+src=["\']([^"\']+)["\']'
    images = re.findall(img_pattern, text)
    
    # Remplacement de localhost par x.com dans les URLs
    images = [
        url.replace("%2F", "/")
        .replace("localhost:8080/pic/media", "pbs.twimg.com/media")
        .replace("localhost/pic/media", "pbs.twimg.com/media")
        .removesuffix(".png").removesuffix(".jpg").removesuffix(".jpeg").removesuffix(".webp")
        + "?format=jpg&name=medium"
        for url in images
    ]    
    return images

def clean_html(text):
    """
    Nettoie le texte en supprimant les balises HTML et normalisant les espaces.
    
    Args:
        text (str): Texte brut potentiellement contenant du HTML/CDATA
        
    Returns:
        str: Texte nettoyé sans balises HTML
        
    Transformations appliquées :
        1. Extraction du contenu CDATA
        2. Suppression de toutes les balises HTML
        3. Normalisation des espaces multiples
    """
    if not text:
        return ""
    # Extraction du contenu CDATA (format XML)
    text = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', text, flags=re.DOTALL)
    # Suppression de toutes les balises HTML
    text = re.sub(r'<[^>]+>', '', text)
    # Normalisation des espaces (multiples → simple)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def parse_to_json(user_url, user_name):
    """
    Parse un flux RSS Nitter et extrait les tweets d'un utilisateur spécifique.
    
    Args:
        user_url (str): URL du flux RSS à parser (peut être écrasée par sys.argv[1])
        user_name (str): Nom d'utilisateur Twitter (avec @) pour filtrer les tweets
        
    Returns:
        dict: Structure JSON contenant les métadonnées du feed et la liste des tweets
        
    Format de retour :
        {
          "feed": {
            "title": str,
            "link": str,
            "description": str
          },
          "tweets": [
            {
              "title": str,
              "date": str,
              "link": str,
              "id": str,
              "author": str,
              "description": str,
              "images": [str]  # Nouveau : liste des URLs d'images
            }
          ]
        }
    """
    # Détermination de la source (URL ou fichier)
    # Priorité à l'argument CLI si présent, sinon utilise user_url
    input_source = sys.argv[1] if len(sys.argv) > 1 else user_url

    # Récupération et parsing du XML
    if input_source.startswith('http://') or input_source.startswith('https://'):
        # Récupération depuis une URL (instance Nitter locale)
        with urllib.request.urlopen(input_source) as response:
            xml_data = response.read()
        root = ET.fromstring(xml_data)
    else:
        # Lecture depuis un fichier local
        tree = ET.parse(input_source)
        root = tree.getroot()

    # Extraction des éléments du flux RSS
    channel = root.find('channel')
    items = channel.findall('item')

    # Construction de la structure JSON de base
    data = {
        'feed': {
            'title': channel.find('title').text,
            'link': channel.find('link').text,
            'description': clean_html(channel.find('description').text)
        },
        'tweets': []
    }

    # Extraction et filtrage des tweets
    for item in items:
        # Récupération de l'auteur (namespace Dublin Core)
        creator = item.find('{http://purl.org/dc/elements/1.1/}creator').text
        
        # Filtrage : ne garder que les tweets de l'utilisateur cible
        if creator == user_name:
            # Récupération de la description brute pour extraire les images
            description_raw = item.find('description').text
            
            tweet = {
                # Nettoyage du titre et remplacement des URLs localhost
                'title': clean_html(item.find('title').text.replace("localhost:8080", "x.com").replace("localhost","x.com")),
                'date': item.find('pubDate').text,
                # Conversion des liens localhost vers x.com (URLs publiques)
                'link': item.find('link').text.replace("localhost","x.com"),
                'id': item.find('guid').text,  # GUID unique du tweet
                'author': item.find('{http://purl.org/dc/elements/1.1/}creator').text,
                # Nettoyage de la description complète
                'description': clean_html(description_raw.replace("localhost:8080", "x.com").replace("localhost","x.com")),
                # NOUVEAU : Extraction des URLs d'images
                'images': extract_images(description_raw)
            }
            data['tweets'].append(tweet)

    return data


# Exemple d'utilisation
if __name__ == "__main__":
    # Test avec votre exemple
    result = parse_to_json("http://localhost:8080/sentdefender/rss", "@sentdefender")
    print(json.dumps(result, indent=2, ensure_ascii=False))