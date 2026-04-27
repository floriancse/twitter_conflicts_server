from openai import OpenAI
import psycopg2
import json
import os 
from dotenv import load_dotenv

# ==============================================================================
# CONFIGURATION
# ==============================================================================
load_dotenv()
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "twitter_conflicts"),
    "user":     os.getenv("DB_USER", "tw_user"),
    "password": os.getenv("DB_PASSWORD"),
    "sslmode":  os.getenv("DB_SSLMODE", "disable"),
}

client = OpenAI(
    base_url="http://localhost:11434/v1",
    api_key="ollama",
)

STRAITS = [
    {"id": 1,  "name": "Suez Canal",          "aliases": ["Suez Canal", "Suez"]},
    {"id": 2,  "name": "Panama Canal",         "aliases": ["Panama Canal", "Panama"]},
    {"id": 3,  "name": "Bosporus Strait",      "aliases": ["Bosporus", "Bosphorus", "Istanbul Strait"]},
    {"id": 4,  "name": "Bab el-Mandeb Strait", "aliases": ["Bab el-Mandeb", "Bab-el-Mandeb", "Bab al-Mandab", "Mandeb"]},
    {"id": 5,  "name": "Malacca Strait",       "aliases": ["Malacca", "Strait of Malacca"]},
    {"id": 6,  "name": "Strait of Hormuz",     "aliases": ["Hormuz", "Strait of Hormuz"]},
    {"id": 7,  "name": "Cape of Good Hope",    "aliases": ["Cape of Good Hope", "Good Hope"]},
    {"id": 8,  "name": "Gibraltar Strait",     "aliases": ["Gibraltar", "Strait of Gibraltar"]},
    {"id": 9,  "name": "Dover Strait",         "aliases": ["Dover", "Strait of Dover", "English Channel"]},
    {"id": 10, "name": "Oresund Strait",       "aliases": ["Oresund", "Øresund"]},
    {"id": 11, "name": "Taiwan Strait",        "aliases": ["Taiwan Strait", "Strait of Taiwan"]},
    {"id": 12, "name": "Korea Strait",         "aliases": ["Korea Strait", "Tsushima Strait"]},
    {"id": 13, "name": "Tsugaru Strait",       "aliases": ["Tsugaru"]},
    {"id": 14, "name": "Luzon Strait",         "aliases": ["Luzon Strait", "Luzon"]},
    {"id": 15, "name": "Lombok Strait",        "aliases": ["Lombok"]},
    {"id": 16, "name": "Ombai Strait",         "aliases": ["Ombai"]},
    {"id": 17, "name": "Bohai Strait",         "aliases": ["Bohai"]},
    {"id": 18, "name": "Torres Strait",        "aliases": ["Torres Strait", "Torres"]},
    {"id": 19, "name": "Sunda Strait",         "aliases": ["Sunda", "Strait of Sunda"]},
    {"id": 20, "name": "Makassar Strait",      "aliases": ["Makassar", "Macassar"]},
    {"id": 21, "name": "Magellan Strait",      "aliases": ["Magellan", "Strait of Magellan"]},
    {"id": 22, "name": "Yucatan Channel",      "aliases": ["Yucatan Channel", "Yucatan"]},
    {"id": 23, "name": "Windward Passage",     "aliases": ["Windward Passage"]},
    {"id": 24, "name": "Mona Passage",         "aliases": ["Mona Passage", "Mona"]},
    {"id": 25, "name": "Balabac Strait",       "aliases": ["Balabac"]},
    {"id": 26, "name": "Bering Strait",        "aliases": ["Bering", "Strait of Bering"]},
    {"id": 27, "name": "Mindoro Strait",       "aliases": ["Mindoro"]},
    {"id": 28, "name": "Kerch Strait",         "aliases": ["Kerch", "Strait of Kerch"]},
]

# ==============================================================================
# CONNEXION DB
# ==============================================================================

def get_db_connection():
    """Établit et retourne une connexion à la base de données PostgreSQL"""
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )

# ==============================================================================
# REQUÊTES
# ==============================================================================

def get_strait_tweets(cur, strait: dict, days: int = 7) -> list[dict]:
    """Récupère les tweets récents pour un détroit donné via ses aliases"""
    conditions = " OR ".join(["text ILIKE %s"] * len(strait["aliases"]))
    params = [f"%{a}%" for a in strait["aliases"]] + [days]

    cur.execute(f"""
        SELECT
            created_at,
            username,
            COALESCE(summary_text, LEFT(text, 200)) AS content,
            conflict_typology,
            importance_score
        FROM tweets
        WHERE ({conditions})
        AND created_at >= NOW() - INTERVAL '%s days' and IS_DUPLICATE = 'false'
        ORDER BY importance_score DESC, created_at DESC
        LIMIT 20
    """, params)

    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

# ==============================================================================
# INFÉRENCE LLM
# ==============================================================================

def infer_strait_status(tweets: list[dict], strait_name: str) -> dict:
    """Infère le statut opérationnel d'un détroit à partir des tweets récents"""
    if not tweets:
        return {
            "status": "UNKNOWN",
            "confidence": "low",
            "reason": "No recent activity found",
            "last_signal_at": None,
        }

    context = "\n".join([
        f"[{t['created_at'].strftime('%Y-%m-%d %H:%M')}] "
        f"{t['username']} (type={t['conflict_typology']}, score={t['importance_score']}): "
        f"{t['content']}"
        for t in tweets
    ])

    prompt = f"""You are a maritime intelligence analyst specialized in commercial shipping and civil maritime traffic.

    Your ONLY focus is to determine if civil commercial vessels (cargo ships, tankers, container ships, bulk carriers, etc.) can transit normally through the {strait_name}.

    Instructions:
    - Base your analysis EXCLUSIVELY on information related to civilian/commercial maritime traffic.
    - Completely ignore any mentions of military vessels, warships, naval forces, carrier strike groups, frigates, destroyers, submarines, or any state military activity.
    - Do not use military movements as evidence for the status of civil traffic.
    - If a tweet mentions only military activity and nothing about commercial shipping, treat it as irrelevant.
    - If the tweets show no clear information about commercial vessels, default to lower confidence.

    Tweets (ordered by importance then recency):
    {context}

    Respond ONLY with a valid JSON object, no explanation outside the JSON:
    {{
        "status": "OPEN" | "RESTRICTED" | "CLOSED",
        "confidence": "high" | "medium" | "low",
        "reason": "<one short sentence focused only on civil/commercial traffic>",
        "last_signal_at": "<ISO datetime of the most relevant tweet about civil traffic, or null>"
    }}"""

    response = client.chat.completions.create(
        model="mistral-small:24b",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1,
    )

    raw = response.choices[0].message.content.strip()
    if "```" in raw:
        raw = raw.split("```")[1].lstrip("json").strip()

    return json.loads(raw)

# ==============================================================================
# SCRIPT PRINCIPAL
# ==============================================================================

def save_strait_state():
    conn = get_db_connection()
    cur = conn.cursor()

    results = []
    for strait in STRAITS:
        tweets = get_strait_tweets(cur, strait, days=14)
        status = infer_strait_status(tweets, strait["name"])
        results.append({
            "id":          strait["id"],
            "name":        strait["name"],
            "tweet_count": len(tweets),
            **status,
        })
        print(f"[{strait['id']:02d}] {strait['name']:<25} → {status['status']:<12} ({status['confidence']}) — {status['reason']}")
        
        cur.execute("""
        INSERT INTO CHOKEPOINTS_STATE_HISTORY (
            SNAPSHOT_DATE, PORTNAME, STATUS, CONFIDENCE, REASON)
        VALUES (NOW(), %s, %s, %s, %s)
        ON CONFLICT (DATE(SNAPSHOT_DATE), PORTNAME)
        DO UPDATE SET
            SNAPSHOT_DATE = NOW(),
            STATUS        = EXCLUDED.STATUS,
            CONFIDENCE    = EXCLUDED.CONFIDENCE,
            REASON        = EXCLUDED.REASON
        """, (strait['name'], status['status'], status['confidence'], status['reason']))
        conn.commit()

    cur.close()
    conn.close()

if __name__ == "__main__":
    save_strait_state()