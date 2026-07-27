from openai import OpenAI
import psycopg2
import os
import json
import argparse
from dotenv import load_dotenv
from datetime import datetime


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

def build_system_prompt() -> str:
    today_str = datetime.today().strftime('%Y-%m-%d')
    return f"""
You are an experienced investigative journalist specializing in conflict monitoring and geopolitical affairs.
Your task is to identify and report on the single most significant or alarming development among the following events.

Guidelines:
- CRITICAL: Write in English only. Do not use Chinese, Arabic, French, or any other language.
- Select ONE standout event — the one with the greatest strategic, humanitarian, or escalatory significance.
- Write exactly 1 or 2 sentences — no more, no less. One sentence = one idea.
- Use a clear, factual, and direct journalistic tone (inverted pyramid style: most important facts first)
- Name the key actors, location, and spell out the concrete consequences or stakes
- Avoid speculation; stick strictly to the reported facts
- Write in the third person, as you would for a news wire dispatch
- Today's date is {today_str}. If the event you select actually took place before today (e.g. it is
  reported today but the facts happened days/months/years earlier, such as an anniversary, retrospective,
  or delayed report), you MUST contextualize it with the real event date — in BOTH the summary AND the title.
  Never let a past event read as if it happened today.

Return a JSON object with this exact structure:
{{
  "summary": "<1 or 2 sentence journalistic summary focused on the single most significant event>",
  "title": "<short, punchy headline>"
}}

Title rules:
- CRITICAL: Write in English only. Do not use Chinese, Arabic, or any other language.
- Keep it short and punchy — prioritize impact over completeness
- Frame the event as a turning point, threat, or crisis when the facts justify it
- Name the key actor and location (e.g. "Russia", "Ukraine", "Iran", "Israel", "Sudan")
- State the core action and what it puts at risk or triggers
- Active voice, past tense
- No hashtags, no quotes, no punctuation at the end
- MANDATORY TEMPORAL RULE: if the selected event did not happen today ({today_str}) — i.e. it is a past,
  historical, anniversary, or retrospective event — the title MUST include a clear date marker for when
  it actually happened (year, or month + year if known), e.g. "Mali: 2024 Ambush on FAMa Sparked Turkish
  Drone Strikes" or "Iran Nuclear Site Hit in June 2025 Strike, Fallout Continues". Do not omit the date
  from the title just because it appears in the summary — both must carry it independently.
- If the event genuinely happened today, no date marker is needed in the title.

Output ONLY the JSON object, nothing else."""


def get_db_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )


def _call_llm(user_content: str) -> dict | None:
    """Call the LLM and return a parsed JSON dict with 'summary' and 'title'."""
    try:
        response = client.chat.completions.create(
            model="qwen36-fixed",
            messages=[
                {"role": "system", "content": build_system_prompt()},
                {"role": "user", "content": user_content},
            ],
            response_format={"type": "json_object"},
            temperature=0,
            top_p=0.8,
            max_tokens=2000,
        )
        raw = response.choices[0].message.content.strip()
        print(raw)
        return json.loads(raw)

    except Exception:
        import traceback
        traceback.print_exc()
        return None


SQL_UPSERT_SUMMARY = """
INSERT INTO TOPIC_SUMMARIES (FK_TOPIC, CREATED_AT, SUMMARY, SUMMARY_TITLE)
VALUES (%s, %s, %s, %s)
ON CONFLICT (FK_TOPIC, CREATED_AT)
DO UPDATE SET
    SUMMARY = EXCLUDED.SUMMARY,
    SUMMARY_TITLE = EXCLUDED.SUMMARY_TITLE
"""

SQL_GET_EVENTS = """
SELECT SUMMARY_TEXT, FK_TOPIC, CREATED_AT::DATE
FROM TWEETS
WHERE
    CREATED_AT::DATE = CURRENT_DATE - %s
    AND SUMMARY_TEXT IS NOT NULL
    AND FK_TOPIC IS NOT NULL
    AND IMPORTANCE_SCORE >= 4
    AND IS_DUPLICATE = 'false' 
"""


def _process_events(events_dict: dict[int, list[str]], date_label, cur=None, conn=None):
    """
    Process a {topic_id: [texts]} dict.
    - If cur/conn provided → write to DB.
    - Always returns a list of result dicts for inspection.
    """
    results = []
    for topic_id, texts in events_dict.items():
        combined = "\n".join(texts)
        result = _call_llm(combined)

        if result is None:
            print(f"[WARN] LLM returned nothing for topic {topic_id}")
            continue

        summary = result.get("summary")
        title = result.get("title")

        if not summary:
            print(f"[WARN] Missing 'summary' field for topic {topic_id}")
            continue

        results.append({
            "topic_id": topic_id,
            "date": str(date_label),
            "summary": summary,
            "title": title,
        })

        if cur and conn:
            cur.execute(SQL_UPSERT_SUMMARY, (topic_id, date_label, summary, title))
            conn.commit()

    return results


def summarize_from_db():
    """Original mode: fetch events from DB, write summaries back to DB."""
    conn = get_db_connection()
    cur = conn.cursor()

    for i in range(1):
        print(f"[DB] Processing day offset: {i}")
        cur.execute(SQL_GET_EVENTS, (i,))
        rows = cur.fetchall()

        events_dict: dict[int, list[str]] = {}
        date_label = None
        for text, topic_id, date in rows:
            events_dict.setdefault(topic_id, []).append(text)
            if date_label is None:
                date_label = date

        if date_label is None:
            from datetime import date as dt_date, timedelta
            date_label = dt_date.today() - timedelta(days=i)

        _process_events(events_dict, date_label, cur=cur, conn=conn)

    cur.close()
    conn.close()


if __name__ == "__main__":
    summarize_from_db()