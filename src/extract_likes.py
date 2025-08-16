from yandex_music import Client
import json
import os

TOKEN = os.environ.get("YANDEX_TOKEN")  # берём токен из переменной окружения

def main():
    client = Client(TOKEN).init()
    likes = client.users_likes_tracks()
    out = []

    for liked in likes:
        track = liked.fetch_track()
        record = {
            "track_id": track.track_id,
            "title": track.title,
            "artists": [artist.name for artist in track.artists],
            "duration_ms": track.duration_ms / 60000,
            "albums": [album.title for album in track.albums] if track.albums else [],
        }
        out.append(record)

    # Сохраняем в JSON
    with open("/opt/airflow/src/data/raw/likes.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"Скачано и сохранено {len(out)} треков в data/raw/likes.json")

if __name__ == "__main__":
    main()
