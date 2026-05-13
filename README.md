# SHA Message Broker

Собственный брокер сообщений с поддержкой pub/sub, персистентности и гарантий доставки, написанный на Python (FastAPI).

## Возможности

| Функция | Статус |
|---------|--------|
| Pub/Sub (топики) | ✅ |
| Несколько подписчиков на один канал | ✅ |
| Приоритеты сообщений (LOW / NORMAL / HIGH / CRITICAL) | ✅ |
| TTL (Time To Live) | ✅ |
| Dead Letter Queue (DLQ) | ✅ |
| Метрики (`/metrics`) | ✅ |
| **Персистентность (JSONL-лог, fsync)** | ✅ |
| **Восстановление после перезапуска** | ✅ |
| **Гарантия доставки at-least-once (ACK + retry)** | ✅ |
| **Дедупликация на стороне брокера** (producer dedup) | ✅ |
| **Дедупликация на стороне консьюмера** (seen-ids set) | ✅ |
| **Python SDK** (Publisher + Subscriber) | ✅ |
| WebSocket push-уведомления | ✅ |
| Docker / docker-compose | ✅ |

---

## Архитектура

```
┌─────────────────────────────────────────────────────────┐
│                    FastAPI HTTP + WS                     │
│  POST /topics/{name}/publish   → publish_to_topic()     │
│  POST /ack                     → broker.ack()           │
│  WS  /ws/{subscriber_id}       → subscribe + ack        │
└───────────────────┬─────────────────────────────────────┘
                    │
            ┌───────▼────────┐
            │  MessageBroker │   ProducerDedupCache (LRU)
            │                │   _restore_from_disk()
            │  topics{}      │   _retry_loop() every 0.5s
            │               │   _cleanup_loop() every 60s
            └──────┬────────┘
                   │
              ┌────▼────┐   Pub/Sub fan-out
              │ Topic   │
              │in_flight│  ACK timeout = 3s → retry same msg_id
              └────┬────┘
                   │
            ┌──────▼───────────────┐
    │     JsonlLogStorage        │
    │                            │
    │  data/topics/<n>/          │
    │    messages.jsonl          │  ← append-only, one msg per line, fsync
    │  data/offsets/<sub>/       │
    │    topic__<n>.json         │  ← highest ACKed offset per subscriber
    │  data/dedup/producer.json  │  ← client_message_id → server msg_id
    │  data/meta.json            │  ← topics/subscriptions registry
    └────────────────────────────┘
```

### Гарантии доставки

```
Publisher SDK                   Broker                  Subscriber SDK
     │                             │                          │
     │── POST /publish ──────────>│                          │
     │   (timeout 5s, retry x3)   │── WS push ─────────────>│
     │                             │   (in-flight entry)      │── callback()
     │<── {msg_id, offset} ───────│                          │── ACK ──────>│
     │                             │<── ACK ────────────────  │
     │                             │   (remove in-flight,     │
     │                             │    save offset to disk)  │
     │                             │                          │
     │                     if no ACK within 3s:               │
     │                             │── retry same msg_id ───>│
     │                             │   (subscriber deduplicates │
     │                             │    via seen_ids set)      │
```

---

## Быстрый старт

### Локально (без Docker)

```bash
cd broker
pip install -r requirements.txt

python run.py
# → http://localhost:8000/api/v1/docs
```

### Docker Compose (брокер + 3 микросервиса)

```bash
cd broker
docker-compose up --build

# Брокер:        http://localhost:8000/api/v1/docs
# Publisher:     docker logs sha-publisher -f
# Subscriber A:  docker logs sha-subscriber-a -f
# Subscriber B:  docker logs sha-subscriber-b -f
```

Для демонстрации перезапуска (данные сохраняются):

```bash
docker-compose restart broker
# Publisher и Subscriber переподключатся автоматически
# Subscriber-а получат не подтверждённые сообщения повторно
```

---

## Использование Python SDK

### Publisher

```python
from client import BrokerPublisher

pub = BrokerPublisher("http://localhost:8000/api/v1")

# Создать топик
pub.create_topic("orders")

# Опубликовать (auto-retry за 5 с, идемпотентно через client_message_id)
msg_id, offset = pub.publish_topic("orders", {"order_id": "abc", "qty": 3})
print(f"Sent: id={msg_id} offset={offset}")

```

### Subscriber

```python
from client import BrokerSubscriber

sub = BrokerSubscriber("http://localhost:8000", "my-service")
sub.subscribe("topic", "orders")

def handle(msg):
    print("Order:", msg["content"])
    # ACK отправляется автоматически после возврата из handle()

sub.consume(handle)   # блокирует; авто-переподключение при разрыве
```

Несколько сервисов на одном топике:

```python
# Запускаются независимо — каждый получает свою копию каждого сообщения
sub_a = BrokerSubscriber("http://localhost:8000", "service-a")
sub_b = BrokerSubscriber("http://localhost:8000", "service-b")
sub_a.subscribe("topic", "orders")
sub_b.subscribe("topic", "orders")
```

---

## REST API

| Метод | Путь | Описание |
|-------|------|----------|
| POST | `/topics` | Создать топик |
| GET | `/topics` | Список топиков |
| POST | `/topics/{name}/publish` | Опубликовать сообщение |
| DELETE | `/topics/{name}` | Удалить топик |
| POST | `/subscriptions` | Создать подписку |
| **POST** | **`/ack`** | **Подтвердить доставку** |
| GET | `/metrics` | Метрики брокера |
| GET | `/health` | Статус |
| GET | `/dlq` | Dead Letter Queue |
| WS | `/ws/{subscriber_id}` | WebSocket push |

Полная документация: `http://localhost:8000/api/v1/docs`

### Формат publish

```json
{
  "content": { "любые": "данные" },
  "priority": "NORMAL",
  "ttl_seconds": 300,
  "client_message_id": "uuid-для-идемпотентности"
}
```

Ответ:

```json
{
  "status": "published",
  "message_id": "...",
  "offset": 42,
  "deduplicated": false
}
```

### Формат /ack

```json
{
  "subscriber_id": "my-service",
  "channel_type": "topic",
  "channel_name": "orders",
  "message_id": "..."
}
```

### WebSocket протокол

```jsonc
// Клиент → Брокер
{"action": "subscribe", "channel_type": "topic", "channel_name": "orders"}
{"action": "ack", "message_id": "...", "channel_type": "topic", "channel_name": "orders"}
{"action": "ping"}

// Брокер → Клиент
{"id": "...", "offset": 42, "topic": "orders", "content": {...}, ...}
{"action": "subscribed", "channel_type": "topic", "channel_name": "orders"}
{"action": "pong"}
```

---

## Тестирование

```bash
# Демо рестарта и гарантий доставки (запустить при работающем брокере)
python examples/test_persistence.py

# Расширенное демо (приоритеты, TTL, DLQ, метрики)
python examples/enhanced_demo.py

# Базовый smoke-test
python examples/test_broker.py
```

---

## Структура файлов данных

```
data/
├── meta.json                     # реестр топиков/подписок
├── topics/
│   └── orders/
│       └── messages.jsonl        # append-only лог
├── offsets/
│   └── my-service/
│       └── topic__orders.json    # {"offset": 42}
└── dedup/
    └── producer.json             # {client_msg_id: server_msg_id}
```

---

## Критерии оценивания (самооценка)

| Критерий | Реализовано | Балл |
|----------|-------------|------|
| Pub/Sub + множественные подписчики | ✅ полностью | 3/3 |
| Персистентность + восстановление + at-least-once | ✅ полностью | 1/1 |
| Python SDK (Publisher + Subscriber, retry, ACK, reconnect) | ✅ полностью | 1/1 |
| Документация + схема | ✅ README + arch.txt | 1/1 |
| Docker / docker-compose | ✅ | +0.5 |
| TTL + DLQ + приоритеты + метрики | ✅ | +1.5 |
| **Итого** | | **≥ 8/10** |
