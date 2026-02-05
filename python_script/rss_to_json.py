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

Utilisation :
    parse_to_json("http://localhost:8080/user/rss", "@username")
"""

import xml.etree.ElementTree as ET
import json
import re
import sys
import urllib.request

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
              "description": str
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
            tweet = {
                # Nettoyage du titre et remplacement des URLs localhost
                'title': clean_html(item.find('title').text.replace("localhost:8080", "x.com").replace("localhost","x.com")),
                'date': item.find('pubDate').text,
                # Conversion des liens localhost vers x.com (URLs publiques)
                'link': item.find('link').text.replace("localhost","x.com"),
                'id': item.find('guid').text,  # GUID unique du tweet
                'author': item.find('{http://purl.org/dc/elements/1.1/}creator').text,
                # Nettoyage de la description complète
                'description': clean_html(item.find('description').text.replace("localhost:8080", "x.com").replace("localhost","x.com"))
            }
            data['tweets'].append(tweet)

    return data