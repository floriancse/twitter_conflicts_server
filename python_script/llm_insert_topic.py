import json
from openai import OpenAI
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "twitter_conflicts"),
    "user":     os.getenv("DB_USER", "tw_user"),
    "password": os.getenv("DB_PASSWORD"),
}

def get_db_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )


client = OpenAI(
    base_url="http://localhost:11434/v1",
    api_key="ollama",
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
    You are an OSINT analyst. Your task is to assign events to a topic.
    
    STRICT RULES:
    - Only assign a topic if the event mentions or relates to that conflict.
    - If the event is vague, a test, gibberish, or not clearly related to any topic, you MUST return {{"topic": null}}.
    - Do NOT guess or infer. If you are not 100% sure, return {{"topic": null}}.
    - A single number, word, or unrelated sentence must return {{"topic": null}}.
    
    Respond ONLY with a JSON object: {{"topic": "Iran-US War"}} or {{"topic": null}}
    
    The topics are: {topic_dict}
    """


def extract_topic(event: str):
    response = client.chat.completions.create(
        model="qwen36-fixed",
        messages=[
            {"role": "system", "content": build_system_prompt(topic_dict)},
            {"role": "user", "content": event},
        ],
        response_format={"type": "json_object"},
        temperature=0.0,
        max_tokens=512,
    )

    raw = response.choices[0].message.content.strip()
    return json.loads(raw)

    
def insert_topics():
    cur.execute(SQL_GET_EVENTS)
    events = cur.fetchall()

    for tweet_id, event_text in events:
        topic_returned = extract_topic(event_text)
        if topic_returned["topic"]:
            topic_id = topic_dict[topic_returned["topic"]][0]
            cur.execute(SQL_UPDATE_TOPICS, (topic_id, tweet_id))
            conn.commit()

if __name__ == "__main__":
    insert_topics()