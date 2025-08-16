FROM apache/airflow:2.9.1

# Копируем requirements.txt внутрь образа
COPY requirements.txt .

# Ставим пакеты через airflow пользователя
USER airflow
RUN pip install --no-cache-dir -r requirements.txt
