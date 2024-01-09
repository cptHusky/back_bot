import logging
import os
from datetime import datetime, time, timedelta
from logging.handlers import TimedRotatingFileHandler
from random import randint
from time import sleep

from psycopg import DatabaseError, connect
from requests import post

from bot_config import BOT_TOKEN, CHAT_ID
from db_config import DB_HOST, DB_NAME, DB_PASS, DB_USER
from phrases import phrases

RETRY_TIMEOUT = 15
MAX_RETRIES = 10

START_TIME = time(7, 0)
END_TIME = time(22, 0)

URL_WO_TEXT = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage?chat_id={CHAT_ID}'

ID_COLUMN_ID = 0
DB_ERROR_TEXT = "У меня пока нет для тебя мотивирующей фразы, просто выпрями спину."


def get_sleep_time() -> int:
    next_message_delay = randint(40 * 60, 90 * 60)
    next_message_time = datetime.now() + timedelta(seconds=next_message_delay)
    if next_message_time.time() < START_TIME:
        if datetime.now().date() == next_message_time.date():
            next_message_time = (datetime.combine(datetime.now().date(), START_TIME) +
                                 timedelta(minutes=randint(2, 25)))
        else:
            next_message_time = (datetime.combine(datetime.now().date(), START_TIME) +
                                 timedelta(days=1, minutes=randint(2, 25)))

    elif next_message_time.time() > END_TIME:
        next_message_time = (datetime.combine(datetime.now().date(), START_TIME) +
                             timedelta(days=1, minutes=randint(2, 25)))

    else:
        return next_message_delay
    return int((next_message_time - datetime.now()).total_seconds())


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


def send_message(text: str) -> int:
    url = f'{URL_WO_TEXT}&text={text}'
    logging.info(f"POSTing: \"{url}\"")
    code = post(url).status_code
    logging.info(f"POST returned: {code}")
    return code


def main():
    logging.info("Bot started")
    sleep_time = get_sleep_time()
    while True:
        try:
            sleep(sleep_time)

            try:
                text = get_text()
            except DatabaseError as e:
                logging.error(f"Database error: {e}")
                text = DB_ERROR_TEXT

            code = send_message(text=text)
            if code == 200:
                logging.info(f"Message sent: \"{text}\"")
            else:
                logging.error(f"Request error, returned {code}")

                for attempt_number in range(MAX_RETRIES):
                    logging.info(f"Retry {attempt_number + 1} out of {MAX_RETRIES} in {RETRY_TIMEOUT} seconds...")
                    sleep(RETRY_TIMEOUT)
                    send_message(text=text)
                    if code == 200:
                        logging.info(f"Message sent after retry: \"{text}\"")
                        break
                    else:
                        logging.error(f"Retry {attempt_number + 1} failed, returned code {code}")

            sleep_time = get_sleep_time()
            next_message_time = datetime.now() + timedelta(seconds=sleep_time)
            logging.info(f"Next message will be sent at {next_message_time}")

        except Exception as e:
            logging.error(f"Unexpected: {e}")


def logger_init():
    log_directory = "logs"
    os.makedirs(log_directory, exist_ok=True)
    log_file = os.path.join(log_directory, "bot_log")

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    handler = TimedRotatingFileHandler(
        filename=log_file,
        when="midnight",
        interval=1
    )
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)


if __name__ == '__main__':
    logger_init()
    main()
