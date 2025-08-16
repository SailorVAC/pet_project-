import psycopg2
import json

DB_HOST = "localhost"  # имя сервиса из docker-compose
DB_NAME = "music_db"         # база для лайков
DB_USER = "music_user"
DB_PASSWORD = "music_pass"
DB_PORT = "5432"

TABLE_NAME = "raw_likes"
JSON_PATH = "data/raw/likes.json"


def get_conn():
    """
    Возвращает соединение с PostgreSQL и устанавливает кодировку UTF8
    """
    conn = psycopg2.connect(
        dbname=DB_NAME.strip(),
        user=DB_USER.strip(),
        password=DB_PASSWORD.strip(),
        host=DB_HOST.strip(),
        port=DB_PORT.strip()
    )
    conn.set_client_encoding('UTF8')
    return conn


def create_table():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    track_id TEXT PRIMARY KEY,
                    title TEXT,
                    artists TEXT[],
                    duration_ms INTEGER,
                    albums TEXT[]
                )
            """)
            conn.commit()
            print(f"Таблица '{TABLE_NAME}' создана или уже существует")


def load_data():
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    with get_conn() as conn:
        with conn.cursor() as cur:
            for record in data:
                cur.execute(f"""
                    INSERT INTO {TABLE_NAME} (track_id, title, artists, duration_ms, albums)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (track_id) DO NOTHING
                """, (
                    record.get("track_id"),
                    record.get("title"),
                    record.get("artists"),
                    record.get("duration_ms"),
                    record.get("albums")
                ))
            conn.commit()
            print(f"Загружено {len(data)} записей в таблицу '{TABLE_NAME}'")


def load_main():
    create_table()
    load_data()


if __name__ == "__main__":
    load_main()
