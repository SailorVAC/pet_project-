Yandex Music Likes ETL

Это мой pet-проект по автоматизации обработки данных о лайках с Yandex Music. Проект извлекает данные из JSON, загружает их в PostgreSQL и позволяет анализировать с помощью Airflow.

Технологии: Python, PostgreSQL, Docker, Airflow.

Функционал:

Создание таблицы для хранения лайков.

Загрузка данных из JSON.

Автоматизация через Airflow.

Запуск:

Поднять сервисы через docker compose up -d.

Инициализировать базу Airflow.

Запустить скрипт загрузки данных: python src/load_likes.py.