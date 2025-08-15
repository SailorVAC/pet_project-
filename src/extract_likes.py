from yandex_music import Client
import json

# Вставь сюда свой токен для Yandex Music API
TOKEN = "y0__xDRi5SoARje-AYgg8idiBQR9lN0c2vVJrEtgTBhcqtUgFQRhA"

def main():
    client = Client(TOKEN).init()

    # Получаем лайки пользователя
    likes = client.users_likes_tracks()
    out = []

    for liked in likes:
        track = liked.fetch_track()
        record = {
            "track_id": track.track_id,
            "title": track.title,
            "artists": [artist.name for artist in track.artists],
            "duration_ms": track.duration_ms/60000,
            "albums": [album.title for album in track.albums] if track.albums else [],
        }
        out.append(record)

    # Сохраняем в JSON
    with open("data/raw/likes.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"Скачано и сохранено {len(out)} треков в data/raw/likes.json")

if __name__ == "__main__":
    main()