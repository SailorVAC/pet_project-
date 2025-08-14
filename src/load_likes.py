import psycopg2
import json

DB_NAME = "postgres"
DB_USER = "user"
DB_PASSWORD = "password"
DB_HOST = "localhost"
DB_PORT = "5432"

TABLE_NAME = "raw_likes"
JSON_PATH = "data/raw/likes.json"

def create_table():
    with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT) as conn:
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
        

    with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT) as conn:
        with conn.cursor() as cur:
            for record in data:
                cur.execute(f"""
                    INSERT INTO {TABLE_NAME} (track_id, title, artists, duration_min, albums)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (track_id) DO NOTHING
                """, (
                    record["track_id"],
                    record["title"],
                    record["artists"],
                    record["duration_ms"],
                    record["albums"]
                ))
            conn.commit()
            print(f"Загружено {len(data)} записей в таблицу '{TABLE_NAME}'")

if __name__ == "__main__":
    create_table()
    load_data()
