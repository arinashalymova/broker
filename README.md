# Python Message Broker

Простой, но эффективный брокер сообщений, реализованный на Python с использованием FastAPI.

документация сваггер после запуска располагается по адресу:
http://localhost:8000/api/v1/docs

## Возможности

- Паттерн Publisher/Subscriber
- Поддержка очередей сообщений (FIFO, LIFO)
- Публикация сообщений в топики/очереди
- Подписка на топики/очереди
- Обработка множественных подписчиков
- Персистентное хранение сообщений
- Восстановление после перезапуска
- Гарантия доставки сообщений

## Установка

```bash
# Клонирование репозитория
git clone https://github.com/yourusername/python-message-broker.git
cd python-message-broker

# Установка зависимостей
pip install -r requirements.txt

# Запуск брокера
python -m broker.server
