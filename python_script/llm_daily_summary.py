from openai import OpenAI
import psycopg2
import os
import re
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "twitter_conflicts"),
    "user":     os.getenv("DB_USER", "tw_user"),
    "password": os.getenv("DB_PASSWORD"),
}

client = OpenAI(
    base_url="http://localhost:11434/v1",
    api_key="ollama",
)

SYSTEM_PROMPT = """
You are an experienced investigative journalist specializing in conflict monitoring and geopolitical affairs.
Your task is to write a concise journalistic summary (1 sentence maximum) of the following events.

Guidelines:
- Use a clear, factual, and direct journalistic tone (inverted pyramid style: most important facts first)
- Highlight key actors, locations, and consequences
- Avoid speculation; stick strictly to the reported facts
- Write in the third person, as you would for a news wire dispatch"""


def get_db_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )


def summarize(text):
    try:
        response = client.chat.completions.create(
            model="qwen2.5:7b",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": text},
            ],
            temperature=0.7,
            top_p=0.8,
            max_tokens=512,
        )
        raw = response.choices[0].message.content.strip()
        return re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()

    except Exception as e:
        import traceback
        traceback.print_exc()
        return None


SQL_GET_EVENTS = """
SELECT TEXT, FK_TOPIC
FROM TWEETS
WHERE
    CREATED_AT::DATE = CURRENT_DATE - %s
    AND SUMMARY_TEXT IS NOT NULL
    AND FK_TOPIC IS NOT NULL
    AND IMPORTANCE_SCORE >= 4
"""

SQL_UPSERT_SUMMARY = """
INSERT INTO TOPIC_SUMMARIES (FK_TOPIC, CREATED_AT, SUMMARY)
VALUES (%s, CURRENT_DATE - %s, %s)
ON CONFLICT (FK_TOPIC, CREATED_AT)
DO UPDATE SET SUMMARY = EXCLUDED.SUMMARY
"""


def summarize_events():
    conn = get_db_connection()
    cur = conn.cursor()

    for i in range(30):
        cur.execute(SQL_GET_EVENTS, (i,))
        events = cur.fetchall()

        # Regroupe les tweets par topic
        events_dict = {}
        for text, topic_id in events:
            events_dict[topic_id] = []

        for text, topic_id in events:
            events_dict[topic_id].append(text)

        for topic_id, texts in events_dict.items():
            summary = summarize("\n".join(texts))
            if summary:
                cur.execute(SQL_UPSERT_SUMMARY, (topic_id,i, summary))
                conn.commit()

    cur.close()
    conn.close()


if __name__ == "__main__":
    summarize_events()