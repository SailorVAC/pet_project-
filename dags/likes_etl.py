from yandex_music import Client
import json
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

TOKEN = "мой токен"
DB_HOST = "postgres-music"  
DB_NAME = "music_db"
DB_USER = "music_user"
DB_PASSWORD = "music_pass"
DB_PORT = "5432"

TABLE_NAME = "raw_likes"
JSON_PATH = "/opt/airflow/data/raw/likes.json"  # путь внутри контейнера

def extract_main():    
    os.makedirs(os.path.dirname(JSON_PATH), exist_ok=True)
    client = Client(TOKEN).init()
    likes = client.users_likes_tracks()
    out = []

    for liked in likes:
        track = liked.fetch_track()
        record = {
            "track_id": track.track_id,
            "title": track.title,
            "artists": [artist.name for artist in track.artists],
            "duration_ms": int(track.duration_ms / 60000),
            "albums": [album.title for album in track.albums] if track.albums else [],
        }
        out.append(record)

    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"Скачано и сохранено {len(out)} треков в {JSON_PATH}")


def get_conn():
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


with DAG(
    dag_id="likes_etl",
    start_date=datetime(2025, 8, 14),
    schedule_interval='@daily',
    catchup=False,
    description="ETL DAG: extract likes from Yandex Music and load to DB"
) as dag:

    t1 = PythonOperator(
        task_id="extract_likes",
        python_callable=extract_main,
        execution_timeout=timedelta(minutes=10)
    )

    t2 = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )

    t3 = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    t1 >> t2 >> t3
