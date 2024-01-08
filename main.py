from phrases import phrases
import psycopg
import random
from bot_config import BOT_TOKEN, CHAT_ID
from db_config import DB_NAME, DB_USER, DB_PASS, DB_HOST
import requests


def get_phrase_id() -> int:
    conn = psycopg.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST
    )

    cursor = conn.cursor()

    cursor.execute("""
    WITH min_times_used AS (
      SELECT id, MIN(times_used) AS min_times
      FROM phrase_usage
      GROUP BY id
    )
    SELECT id
    FROM min_times_used
    ORDER BY RANDOM()
    LIMIT 1
    """)

    selected_id = cursor.fetchone()[0]

    cursor.execute("UPDATE phrase_usage SET times_used = times_used + 1 WHERE id = %s", (selected_id,))

    conn.commit()
    conn.close()

    return selected_id


def get_text() -> str:
    phrase_id = get_phrase_id()
    return phrases[phrase_id]


def main():
    url_wo_text = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage?chat_id={CHAT_ID}'
    text = get_text()
    url = f'{url_wo_text}&text={text}'
    requests.post(url)


if __name__ == '__main__':
    main()
