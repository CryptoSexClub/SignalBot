FROM python:3.11-slim

# Установка системных зависимостей
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc build-essential libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Создадим рабочую директорию
WORKDIR /app

# Скопируем зависимости
COPY requirements.txt .

# Установим зависимости Python
RUN pip install --no-cache-dir -r requirements.txt

# Скопируем весь проект
COPY . .

# Создаем папку для логов
#RUN mkdir -p /logs

# (опционально) Явно укажем локаль, если будут проблемы с pandas
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV PYTHONUNBUFFERED=1

# Указываем Python искать модули в корне проекта
ENV PYTHONPATH="/app"

# Запуск бота
#CMD ["python", "run_signals.py"]

# Запуск обоих скриптов параллельно
#CMD ["sh", "-c", "python run_signals.py & python modules/run_funding_watcher.py --max-workers 10 & wait"]
CMD ["sh", "-c", " python run_funding_watcher.py --max-workers 10"]
