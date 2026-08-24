from openai import OpenAI
import psycopg2
import json
import os 
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from token_tracker import track
import token_tracker
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
    base_url="http://localhost:8081/v1",
    api_key="",
)

# If a strait has never been evaluated yet, look back this far for its first run.
FIRST_RUN_LOOKBACK_DAYS = 30

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

def get_last_snapshots(cur) -> dict:
    """Récupère le dernier snapshot (date + statut) pour chaque détroit déjà connu en base.
    Retourne {portname: {"snapshot_date":..., "status":..., "confidence":..., "reason":...}}.
    Un détroit absent du dict n'a jamais été évalué."""
    cur.execute("""
        SELECT DISTINCT ON (PORTNAME)
            PORTNAME, SNAPSHOT_DATE, STATUS, CONFIDENCE, REASON
        FROM CHOKEPOINTS_STATE_HISTORY
        ORDER BY PORTNAME, SNAPSHOT_DATE DESC
    """)
    return {
        row[0]: {
            "snapshot_date": row[1],
            "status":        row[2],
            "confidence":    row[3],
            "reason":        row[4],
        }
        for row in cur.fetchall()
    }


def has_new_signal(cur, strait: dict, since) -> bool:
    """Vérifie légèrement (sans ramener le contenu) s'il existe au moins un tweet
    lié à ce détroit posté après 'since'. Sert de garde avant d'appeler le LLM."""
    conditions = " OR ".join(["text ILIKE %s"] * len(strait["aliases"]))
    params = [f"%{a}%" for a in strait["aliases"]]
    query = f"""
        SELECT 1
        FROM tweets
        WHERE ({conditions})
        AND created_at > %s
        AND IS_DUPLICATE = 'false'
        LIMIT 1
    """
    cur.execute(query, params + [since])
    return cur.fetchone() is not None


def get_strait_tweets(cur, strait: dict, days: int = 60) -> list[dict]:
    """Récupère les tweets récents pour un détroit donné via ses aliases"""
    conditions = " OR ".join(["text ILIKE %s"] * len(strait["aliases"]))
    params = [f"%{a}%" for a in strait["aliases"]]
    query = f"""
        SELECT
            created_at,
            username,
            COALESCE(summary_text, LEFT(text, 200)) AS content,
            conflict_typology,
            importance_score
        FROM tweets
        WHERE ({conditions})
        AND created_at >= NOW() - INTERVAL '{days} days'
        AND IS_DUPLICATE = 'false'
        ORDER BY created_at DESC, importance_score DESC
        LIMIT 30
    """
    #print(query, params)
    cur.execute(query, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

# ==============================================================================
# INFÉRENCE LLM
# ==============================================================================

def infer_strait_status(tweets: list[dict], strait: dict) -> dict:
    """Infère le statut opérationnel d'un détroit à partir des tweets récents"""
    strait_name = strait["name"]

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

    # Extra instructions injected for straits where ambiguous signals are frequent
    STRAIT_SPECIFIC_INSTRUCTIONS = {
    "Strait of Hormuz": """
    HORMUZ-SPECIFIC RULES (apply strictly):

    1. CLOSURE CONFIRMATION:
    - Only assign RESTRICTED or CLOSED if there is an EXPLICIT, unambiguous signal:
        a confirmed closure order, a verified attack on a commercial vessel,
        an official blockade, or a formal navigation warning (NAVTEX/NOTAM/JMIC).
    - A single unverified source is insufficient; require corroboration.

    2. REOPENING CONFIRMATION:
    - Only revert to OPENED if there is an EXPLICIT operational confirmation
        that commercial vessels may transit freely and safely.
    - Diplomatic talks, negotiations, "progress" announcements, or MOU signings
        are NOT a reopening — require a concrete operational signal
        (e.g. vessels actually transiting normally, an official NOTAM lifting the closure).
    - A shadow fleet or unidentified vessel transit does NOT constitute
        evidence that the strait is open to all commercial traffic.

    3. MIXED OR AMBIGUOUS SIGNALS:
    - If the most recent signals are diplomatic/political (type=POL) but the last
        confirmed OPERATIONAL signal (type=MOVE or MIL with score >= 3) indicated
        a closure, maintain CLOSED or RESTRICTED with confidence 'medium'.
    - Do NOT default to OPENED when signals are mixed.
    - The fallback is: maintain the last confirmed operational status.

    4. SIGNAL PRIORITY (highest to lowest):
    - Vessel tracking data showing actual ship behavior (MOVE, score >= 3)
    - Official navigation warnings: JMIC, NAVTEX, NOTAM (POL, score >= 3)
    - Confirmed closure/opening orders from state actors (MIL/POL, score >= 4)
    - Diplomatic statements, negotiations, announcements (POL, any score) ← lowest weight

    5. PARTIAL / CAUTIOUS RESUMPTION AFTER A CLOSURE:
    - If a tweet reports a SPECIFIC, NAMED commercial vessel (with cargo type and/or
        destination) actually transiting the strait after a prior closure or attack,
        this is a genuine operational (MOVE-equivalent) signal, even if the tweet
        itself is sourced from news media rather than vessel-tracking data.
    - Do NOT dismiss this signal just because it is a single vessel, or because
        the report describes it as a "cautious", "first", or "test" resumption.
    - Such a signal is NOT sufficient to move status to OPENED (that still requires
        evidence of broad, normal commercial traffic resuming).
    - It IS sufficient to move status from CLOSED to RESTRICTED, with confidence
        'medium', with a reason noting that transit is resuming selectively /
        cautiously rather than normally.
    - Only remain at CLOSED if there is no evidence of any commercial vessel
        actually transiting since the closure began.
    """,
    }

    extra_instructions = STRAIT_SPECIFIC_INSTRUCTIONS.get(strait_name, "")

    prompt = f"""You are a maritime intelligence analyst specialized in commercial shipping and civil maritime traffic.

    Your ONLY focus is to determine if civil commercial vessels (cargo ships, tankers, container ships, bulk carriers, etc.) can transit normally through the {strait_name}.

    Instructions:
    - Your conclusion MUST reflect the state described in the MOST RECENT SIGNALS section above.
    - If the most recent signals indicate a closure or restriction, the status is CLOSED or RESTRICTED, even if older tweets mention a prior reopening.
    - A reopening deal announced days ago does NOT override a fresh closure announced today.
    - Base your analysis EXCLUSIVELY on information related to civilian/commercial maritime traffic.
    - Completely ignore any mentions of military vessels, warships, naval forces, or any state military activity UNLESS they directly impact civil transit (e.g. a blockade, a closure order, attacks on commercial ships).
    - If tweets contradict each other, the MOST RECENT SIGNALS always win.
    - Use "low" confidence only if the most recent signals contain no clear information about commercial traffic.
    - If a strait is closed to a specific country then the status is RESTRICTED, use the country name in the reason sentence.
    {extra_instructions}
    Tweets (ordered by recency then importance):
    {context}

    Respond ONLY with a valid JSON object, no explanation outside the JSON:
    {{
        "status": "OPENED" | "RESTRICTED" | "CLOSED",
        "confidence": "high" | "medium" | "low",
        "reason": "<one short sentence focused only on civil/commercial traffic>",
        "last_signal_at": "<ISO datetime of the most relevant tweet about civil traffic, or null>"
    }}"""

    response = client.chat.completions.create(
        model="gemma-4-26B-A4B",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        response_format={"type": "json_object"}
    )
    track(response)
    raw = response.choices[0].message.content.strip()

    if raw.startswith("```"):
        raw = raw.split("```")[1]
        
    if raw.startswith("json"):
        raw = raw[4:]

    raw = raw.strip()

    return json.loads(raw)

# ==============================================================================
# SCRIPT PRINCIPAL
# ==============================================================================

def save_strait_state():
    conn = get_db_connection()
    cur = conn.cursor()

    last_snapshots = get_last_snapshots(cur)
    default_since = datetime.now(timezone.utc) - timedelta(days=FIRST_RUN_LOOKBACK_DAYS)

    results = []
    skipped = 0
    carried_over = 0

    for strait in STRAITS:
        prev = last_snapshots.get(strait["name"])
        since = prev["snapshot_date"] if prev else default_since

        if not has_new_signal(cur, strait, since):
            if prev is None:
                # Jamais évalué et aucun signal depuis le lookback initial : rien à recopier.
                print(f"[{strait['id']:02d}] {strait['name']:<25} → SKIP (aucun historique, aucun signal depuis {since})")
                skipped += 1
                continue

            # Aucun nouveau signal : on recopie le dernier statut connu avec la date du jour.
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
            """, (strait["name"], prev["status"], prev["confidence"], prev["reason"]))
            conn.commit()

            print(f"[{strait['id']:02d}] {strait['name']:<25} → {prev['status']:<12} ({prev['confidence']}) — inchangé (aucun nouveau signal)")
            carried_over += 1
            continue

        tweets = get_strait_tweets(cur, strait, days=30)
        status = infer_strait_status(tweets, strait)
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

    print(f"\n{len(results)} détroit(s) réévalué(s), {carried_over} recopié(s) (aucun signal nouveau), {skipped} skippé(s) (aucun historique).")

    cur.close()
    conn.close()


if __name__ == "__main__":
    save_strait_state()
    print(token_tracker.summary())