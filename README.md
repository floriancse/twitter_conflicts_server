# OSINT Observer

Système automatisé de collecte, géolocalisation et visualisation d'événements géopolitiques à partir de sources OSINT Twitter.

## Vue d'ensemble

Ce projet collecte des tweets de sources OSINT (Open Source Intelligence, ou Renseignement d’Origine Sources Ouvertes), extrait les informations géopolitiques et militaires via IA, géolocalise les événements et expose les données via une API REST. <br />
Démo live Open-Source (MapLibre GL JS) : https://floriancse.github.io/osint-observer/

## Fonctionnalités

- Scraping automatique de flux RSS Nitter
- Extraction d'événements via LLM local (Ollama + Qwen3-14B)
- Géolocalisation automatique avec niveau de confiance
- Classification typologique (MIL/OTHER) et importance stratégique (1-5)
- API REST avec export GeoJSON
- Filtrage par période, auteur, mots-clés

## Architecture

```
┌─────────────────┐
│  Sources OSINT  │ (@GeoConfirmed, @sentdefender, etc.)
└────────┬────────┘
         │ RSS Feed
         ▼
┌─────────────────┐
│  RSS Parser     │ (rss_to_json.py)
│                 │
└────────┬────────┘
         │ JSON
         ▼
┌─────────────────┐
│  LLM Qwen3      │ (llm_geocode.py)
│                 │
└────────┬────────┘
         │ (feed.py)
         ▼
┌─────────────────┐
│  PostgreSQL     │
│  + PostGIS      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  FastAPI        │ (API REST)
│  GeoJSON Export │
└─────────────────┘
```

## Technologies

- Python 3.x, FastAPI
- PostgreSQL + PostGIS
- Ollama + Qwen3-14B

## Pipeline de traitement

1. **Collecte RSS** : Parse les flux Nitter, nettoie le HTML
2. **Analyse LLM** : Extraction d'événements, classification, géolocalisation
3. **Insertion** : Stockage PostgreSQL avec géométrie PostGIS

### Classification LLM

- **MIL** : Événements militaires explicites (bombardements, frappes, combats)
- **OTHER** : Tous les autres événements

### Géolocalisation

- **explicit** : Lieu nommé précisément (confiance high)
- **inferred** : Zone approximative (confiance medium)
- **unknown** : Pas de lieu identifiable (null)

### Importance stratégique

1. Événement local/mineur
2. Événement tactique
3. Événement opérationnel
4. Événement stratégique
5. Événement critique mondial

## Sources OSINT

```python
sources = [
    "@GeoConfirmed",
    "@sentdefender",
    "@OSINTWarfare",
    "@Osinttechnical",
    "@Conflict_Radar",
    "@ACLEDINFO"
]
```
