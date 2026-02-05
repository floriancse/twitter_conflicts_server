# OSINT Twitter Conflicts Monitor

Système automatisé de collecte, géolocalisation et visualisation d'événements géopolitiques à partir de sources OSINT Twitter.

## Vue d'ensemble

Ce projet collecte des tweets de sources OSINT, extrait les informations géopolitiques et militaires via IA, géolocalise les événements et expose les données via une API REST.

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
│  Clean HTML     │
└────────┬────────┘
         │ JSON
         ▼
┌─────────────────┐
│  LLM Analyzer   │ (llm_geocode.py)
│  Ollama/Qwen3   │
└────────┬────────┘
         │ Structured Data
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
- psycopg2, python-dotenv

## Installation

### Prérequis

```bash
# PostgreSQL avec PostGIS
sudo apt install postgresql postgresql-contrib postgis

# Ollama
curl -fsSL https://ollama.com/install.sh | sh
ollama pull richardyoung/qwen3-14b-abliterated:q5_k_m
```

### Dépendances Python

```bash
git clone https://github.com/votre-username/osint-twitter-conflicts.git
cd osint-twitter-conflicts

python3 -m venv venv
source venv/bin/activate

pip install fastapi uvicorn psycopg2-binary python-dotenv ollama requests
```

### Base de données

```sql
CREATE DATABASE twitter_conflicts;
\c twitter_conflicts
CREATE EXTENSION postgis;

CREATE TABLE public.tweets (
    id SERIAL PRIMARY KEY,
    tweet_id VARCHAR(255) UNIQUE NOT NULL,
    date_published TIMESTAMP,
    url TEXT,
    author VARCHAR(255),
    body TEXT,
    accuracy VARCHAR(50),
    importance INTEGER,
    typology VARCHAR(50),
    geom GEOMETRY(Point, 4326)
);

CREATE INDEX idx_tweets_geom ON public.tweets USING GIST(geom);
```

## Configuration

Fichier `.env` :

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=twitter_conflicts
DB_USER=tw_user
DB_PASSWORD=votre_mot_de_passe
```

## Utilisation

```bash
# Démarrer l'API
uvicorn main:app --reload --port 8000

# Lancer le scraping
python scraper.py
```

Documentation API : `http://localhost:8000/docs`

## API Endpoints

**GET `/api/twitter_conflicts/tweets.geojson`**

Tweets géolocalisés en GeoJSON. Paramètres : `hours` (24), `q`, `authors`.

**GET `/api/twitter_conflicts/authors`**

Liste des auteurs. Paramètres : `hours` (720).

**GET `/api/twitter_conflicts/disputed_area.geojson`**

Zones disputées en GeoJSON.

**GET `/api/twitter_conflicts/important_tweets`**

Tweets avec importance ≥ 4. Paramètres : `hours` (24).

**GET `/api/twitter_conflicts/random_tweets`**

5 tweets aléatoires non géolocalisés.

**GET `/api/twitter_conflicts/last_tweet_date`**

Date du dernier tweet en base.

## Structure de données

### Table tweets

| Colonne | Type | Description |
|---------|------|-------------|
| id | SERIAL | Clé primaire |
| tweet_id | VARCHAR(255) | ID unique du tweet |
| date_published | TIMESTAMP | Date de publication |
| author | VARCHAR(255) | Auteur (@handle) |
| body | TEXT | Contenu du tweet |
| accuracy | VARCHAR(50) | Confiance (Haute/Moyenne/Basse) |
| importance | INTEGER | Importance stratégique (1-5) |
| typology | VARCHAR(50) | MIL ou OTHER |
| geom | GEOMETRY | Coordonnées géographiques |

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

## Licence

MIT

## Avertissement

Ce projet est destiné à la recherche et l'analyse OSINT. Les informations proviennent de sources publiques et doivent être vérifiées indépendamment.