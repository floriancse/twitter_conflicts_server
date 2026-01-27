#!/usr/bin/env python3
"""
Convertit le flux RSS Nitter en JSON (depuis URL ou fichier)
"""

import xml.etree.ElementTree as ET
import json
import re
import sys
import urllib.request

def clean_html(text):
    """Enlève les balises HTML"""
    if not text:
        return ""
    text = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', text, flags=re.DOTALL)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

# Entrée (URL ou fichier)
def parse_to_json(user_url, user_name):
    input_source = sys.argv[1] if len(sys.argv) > 1 else user_url

    # Récupérer le XML
    if input_source.startswith('http://') or input_source.startswith('https://'):
        # Depuis URL
        with urllib.request.urlopen(input_source) as response:
            xml_data = response.read()
        root = ET.fromstring(xml_data)
    else:
        # Depuis fichier
        tree = ET.parse(input_source)
        root = tree.getroot()

    # Parser
    channel = root.find('channel')
    items = channel.findall('item')

    # Construire la structure JSON
    data = {
        'feed': {
            'title': channel.find('title').text,
            'link': channel.find('link').text,
            'description': clean_html(channel.find('description').text)
        },
        'tweets': []
    }

    # Ajouter chaque tweet
    for item in items:
        creator = item.find('{http://purl.org/dc/elements/1.1/}creator').text
        if creator == user_name:
            tweet = {
                'title': clean_html(item.find('title').text.replace("localhost:8080", "x.com").replace("localhost","x.com")),
                'date': item.find('pubDate').text,
                'link': item.find('link').text.replace("localhost","x.com"),
                'id': item.find('guid').text,
                'author': item.find('{http://purl.org/dc/elements/1.1/}creator').text,
                'description': clean_html(item.find('description').text.replace("localhost:8080", "x.com").replace("localhost","x.com"))
            }
            data['tweets'].append(tweet)

    return data