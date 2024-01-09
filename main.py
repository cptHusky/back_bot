from datetime import datetime, time, timedelta
from random import randint
from time import sleep

from psycopg import connect
from requests import post

from bot_config import BOT_TOKEN, CHAT_ID
from db_config import DB_HOST, DB_NAME, DB_PASS, DB_USER
from phrases import phrases

START_DATETIME = datetime.combine(datetime.now().date(), time(7, 0))
END_DATETIME = datetime.combine(datetime.now().date(), time(22, 0))
URL_WO_TEXT = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage?chat_id={CHAT_ID}'
ID_COLUMN_ID = 0


def get_sleep_time() -> int:
    now = datetime.now()

    if now < START_DATETIME:
        return int((START_DATETIME - now).total_seconds()) + randint(10 * 60, 25 * 60)
    elif now > END_DATETIME:
        next_day_start = START_DATETIME + timedelta(days=1)
        return int((next_day_start - now).total_seconds()) + randint(10 * 60, 25 * 60)
    else:
        return randint(40 * 60, 90 * 60)


def get_phrase_id() -> int:
    conn = connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST
    )

    cursor = conn.cursor()

    # noinspection SQLUnresolvedReferences, SqlNoDataSourceInspection, SqlResolve
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

    selected_id = cursor.fetchone()[ID_COLUMN_ID]

    # noinspection SqlNoDataSourceInspection, SqlResolve
    cursor.execute("UPDATE phrase_usage SET times_used = times_used + 1 WHERE id = %s", (selected_id,))

    conn.commit()
    conn.close()

    return selected_id


def get_text() -> str:
    phrase_id = get_phrase_id()
    return phrases[phrase_id]


def main():
    sleep_time = 0
    while True:
        sleep(sleep_time)
        text = get_text()
        url = f'{URL_WO_TEXT}&text={text}'
        post(url)
        sleep_time = get_sleep_time()


if __name__ == '__main__':
    main()
