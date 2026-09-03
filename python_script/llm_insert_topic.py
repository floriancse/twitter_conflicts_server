import json
from openai import OpenAI
import psycopg2
import os
from dotenv import load_dotenv
from token_tracker import track
import token_tracker
load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "twitter_conflicts"),
    "user":     os.getenv("DB_USER", "tw_user"),
    "password": os.getenv("DB_PASSWORD"),
}

# Nombre d'événements envoyés par appel LLM. Un system prompt (avec la liste des
# topics) coûte le même prix qu'il traite 1 ou 20 événements : plus ce chiffre
# est haut, moins on paie de fois ce prompt. 20 reste raisonnable pour ne pas
# risquer de tronquer la réponse JSON (max_tokens) ni la fenêtre de contexte.
BATCH_SIZE = 10

def get_db_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )


client = OpenAI(
    base_url="http://localhost:8081/v1",
    api_key="",
)

conn = get_db_connection()
cur = conn.cursor()

SQL_GET_TOPICS = """
SELECT
    TOPIC_ID,
	LABEL,
	COUNTRIES
FROM
	TOPICS
WHERE
	ACTIVE = TRUE
"""

SQL_GET_EVENTS = """
SELECT
	TWEET_ID,
	SUMMARY_TEXT
FROM
	TWEETS
WHERE
	CREATED_AT >= NOW() - INTERVAL '24 hours'
	AND FK_TOPIC IS NULL
	AND IS_DUPLICATE = 'false'
	AND GEOM IS NOT NULL
ORDER BY
	CREATED_AT DESC
"""

SQL_UPDATE_TOPICS = """
UPDATE TWEETS
SET FK_TOPIC = %s
WHERE TWEET_ID = %s
"""

cur.execute(SQL_GET_TOPICS)
topics = cur.fetchall()
topic_dict = {}

for topic_id, topic, country in topics:
    topic_dict[topic] = [topic_id, country]


def build_system_prompt(topic_dict: dict):
    return f"""
    You are an OSINT analyst. Your task is to assign each event in a batch to a topic.

    You will receive a numbered list of events, each prefixed with its EVENT_ID
    (an opaque identifier — treat it as a label, not as data to interpret).

    STRICT RULES (apply independently to EACH event):
    - Only assign a topic if the event mentions or relates to that conflict.
    - If the event is vague, a test, gibberish, or not clearly related to any topic, assign topic = null for it.
    - Do NOT guess or infer. If you are not 100% sure, assign topic = null.
    - A single number, word, or unrelated sentence must get topic = null.
    - If an event concerns Lebanon + Hezbollah and Israel only, assign "Hezbollah-Israel Front".
    - Judge each event ONLY on its own text. Do not let one event's topic influence another's.

    You MUST return exactly one assignment per EVENT_ID you were given, in the same order,
    with no omissions and no extra entries.

    Respond ONLY with a JSON object of this exact shape:
    {{
      "assignments": [
        {{"event_id": "<EVENT_ID as given>", "topic": "Iran-US War"}},
        {{"event_id": "<EVENT_ID as given>", "topic": null}}
      ]
    }}

    The topics are: {topic_dict}
    """


def extract_topics_batch(batch: list[tuple[str, str]]) -> dict[str, str | None]:
    """batch: list of (tweet_id, event_text). Returns {tweet_id: topic_or_None}."""
    user_content = "\n\n".join(
        f"EVENT_ID: {tweet_id}\nTEXT: {event_text}"
        for tweet_id, event_text in batch
    )

    response = client.chat.completions.create(
        model="gemma-4-26B-A4B",
        messages=[
            {"role": "system", "content": build_system_prompt(topic_dict)},
            {"role": "user", "content": user_content},
        ],
        response_format={"type": "json_object"},
        temperature=0.0,
    )
    track(response)
    raw = response.choices[0].message.content.strip()
    raw = raw.strip()

    if raw.startswith("```"):
        raw = raw.split("```")[1]
        raw = raw.strip()
        if raw.lower().startswith("json"):
            raw = raw[4:]

    raw = raw.strip()
    print(raw)
    try:
        parsed = json.loads(raw)
        assignments = parsed.get("assignments", [])
    except (json.JSONDecodeError, AttributeError):
        print(f"[WARN] Réponse LLM invalide pour ce batch, événements ignorés:\n{raw}")
        return {}

    result: dict[str, str | None] = {}
    for entry in assignments:
        event_id = entry.get("event_id")
        if event_id is None:
            continue
        result[str(event_id)] = entry.get("topic")

    return result


def chunked(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def insert_topics():
    cur.execute(SQL_GET_EVENTS)
    events = cur.fetchall()

    if not events:
        return

    for batch in chunked(events, BATCH_SIZE):
        assignments = extract_topics_batch([(str(tid), text) for tid, text in batch])

        # tweet_id peut être un bigint : on garde la correspondance via la version str
        batch_by_str_id = {str(tid): tid for tid, _ in batch}

        for str_id, topic_name in assignments.items():
            if not topic_name:
                continue
            if topic_name not in topic_dict:
                print(f"[WARN] Topic inconnu renvoyé par le LLM: {topic_name!r} (event {str_id})")
                continue

            tweet_id = batch_by_str_id.get(str_id)
            if tweet_id is None:
                print(f"[WARN] event_id {str_id!r} renvoyé par le LLM ne correspond à aucun tweet du batch")
                continue

            topic_id = topic_dict[topic_name][0]
            cur.execute(SQL_UPDATE_TOPICS, (topic_id, tweet_id))

        conn.commit()

if __name__ == "__main__":
    insert_topics()
    print(token_tracker.summary())