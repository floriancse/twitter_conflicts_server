# OSINT Event Collection and Geolocation System

This project is a specialized OSINT (Open Source Intelligence) pipeline designed to collect, process, and geolocate conflict-related events from social media sources. It leverages Large Language Models (LLMs) and geolocation services to transform raw text into structured geospatial data stored in a PostgreSQL/PostGIS database.

Live demo available (React / MapLibre / FastAPI) : [https://floriancse.github.io/osint-observer/](https://floriancse.github.io/osint-observer/)

## Table of Contents

* System Overview
* Key Features
* Prerequisites
* Installation
* Configuration
* Technical Workflow
* Database Schema

---

## System Overview

The core script (feed.py) monitors a list of high-profile OSINT sources via an RSS-to-JSON gateway. It filters relevant data, extracts geographic entities, translates content into English, and performs automated threat assessment and duplicate detection.

## Key Features

* **Automated Collection**: Monitors over 30 specialized OSINT sources (e.g., @GeoConfirmed, @sentdefender, @NOELreports).
* **Dual Geolocation**: Uses LLMs for initial coordinate extraction and falls back to Nominatim for precise query-based geolocation.
* **Content Processing**: Automatically translates tweets to English and generates concise summaries of military actions.
* **Data Enrichment**:
    * **Duplicate Detection**: Flags redundant reports across different sources.
    * **Aggressor Extraction**: Identifies actors involved in military actions.
    * **Strategic Analysis**: Saves snapshots of global threat levels and maritime strait statuses.
* **PostGIS Integration**: Stores spatial data using the WKT format for mapping applications.

## Technical Workflow

1. **Ingestion**: Fetches RSS feeds from local gateway and converts them to JSON format.
2. **Filtering**: Excludes retweets, non-relevant updates, and IDs already present in the database.
3. **Extraction & Translation**:
    * The LLM extracts events, coordinates, typology, and strategic importance.
    * translate_to_english ensures all stored text is in English for consistency.
4. **Refinement**: Nominatim is queried to validate or improve geographic coordinates.
5. **Storage**: Data is inserted into the tweets table with geometry points (SRID 4326).
6. **Post-Processing**:
    * flag_duplicates(): Cleans the dataset for redundant information.
    * save_threat_snapshot(): Logs the current global situation.
    * generate_aggressor(): Populates actor-specific tables and conflict pairs.

## Database Schema

The script interacts with several tables to ensure data integrity:

| Table | Description |
| :--- | :--- |
| **tweets** | Core event data, original/translated text, and PostGIS geometry. |
| **tweet_images** | Links to media associated with specific reports. |
| **daily_conflict_pairs** | Aggregated daily view of interacting parties (Aggressor vs Target). |
| **military_actions** | Structured data regarding specific combat operations extracted by LLM. |

---