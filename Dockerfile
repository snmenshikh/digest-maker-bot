# Базовый образ
FROM python:3.11-slim

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libxml2-dev libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

# Создаём рабочую директорию
WORKDIR /app

# Копируем зависимости
COPY requirements.txt /app/requirements.txt

# Устанавливаем зависимости Python
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код бота
COPY digest_maker_bot.py /app/digest_maker_bot.py

# Указываем переменные окружения (токен хранится в Portainer Env)
ENV PYTHONUNBUFFERED=1

# Точка входа
CMD ["python", "digest_maker_bot.py"]