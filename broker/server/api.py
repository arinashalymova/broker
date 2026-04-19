from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any
from enum import Enum
from datetime import datetime
import uuid

from broker.core.broker import broker
from broker.core.message import Message as CoreMessage, MessagePriority
from broker.core.queue import QueueType as CoreQueueType

app = FastAPI(
    title="Message Broker API - Enhanced",
    description="Производственный брокер сообщений с приоритетами, TTL, DLQ и метриками",
    version="1.0.0",
)

# === МОДЕЛИ ===

class QueueType(str, Enum):
    FIFO = "FIFO"
    LIFO = "LIFO"

class Priority(str, Enum):
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

class MessageBase(BaseModel):
    content: Any
    content_type: str = "application/json"
    headers: Dict[str, str] = Field(default_factory=dict)
    priority: Priority = Priority.NORMAL
    ttl_seconds: Optional[int] = None

class TopicCreate(BaseModel):
    name: str

class TopicInfo(BaseModel):
    name: str
    subscribers: List[str]
    message_count: int
    created_at: str

class QueueCreate(BaseModel):
    name: str
    type: QueueType = QueueType.FIFO

class QueueInfo(BaseModel):
    name: str
    type: str
    subscribers: List[str]
    message_count: int
    created_at: str

class SubscriptionCreate(BaseModel):
    subscriber_id: str
    target_type: str
    target_name: str

# === БАЗОВЫЕ ЭНДПОИНТЫ ===

@app.post("/topics", response_model=TopicInfo, tags=["Topics"], summary="Создать топик")
async def create_topic(topic_data: TopicCreate):
    """Создать новый топик"""
    try:
        topic = broker.create_topic(topic_data.name)
        return TopicInfo(**topic.get_info())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/topics", response_model=List[TopicInfo], tags=["Topics"], summary="Получить все топики")
async def get_topics():
    """Получить список всех топиков"""
    topics_data = broker.get_all_topics()
    return [TopicInfo(**topic_data) for topic_data in topics_data]

@app.get("/topics/{topic_name}", response_model=TopicInfo, tags=["Topics"], summary="Получить топик")
async def get_topic(topic_name: str):
    """Получить информацию о топике"""
    topic = broker.get_topic(topic_name)
    if not topic:
        raise HTTPException(status_code=404, detail="Topic not found")
    return TopicInfo(**topic.get_info())

@app.delete("/topics/{topic_name}", tags=["Topics"], summary="Удалить топик")
async def delete_topic(topic_name: str):
    """Удалить топик"""
    if not broker.delete_topic(topic_name):
        raise HTTPException(status_code=404, detail="Topic not found")
    return {"status": "success", "message": f"Topic {topic_name} deleted"}

# === РАСШИРЕННАЯ ПУБЛИКАЦИЯ ===

@app.post("/topics/{topic_name}/publish", tags=["Publishing"],
          summary="Публикация с приоритетом и TTL")
async def publish_to_topic_enhanced(topic_name: str, message_data: MessageBase):
    """
    Опубликовать сообщение в топик с расширенными возможностями:
    - Приоритеты: LOW, NORMAL, HIGH, CRITICAL
    - TTL (Time To Live) в секундах
    - Дополнительные заголовки
    """
    topic = broker.get_topic(topic_name)
    if not topic:
        raise HTTPException(status_code=404, detail="Topic not found")

    # Конвертируем приоритет
    priority_mapping = {
        Priority.LOW: MessagePriority.LOW,
        Priority.NORMAL: MessagePriority.NORMAL,
        Priority.HIGH: MessagePriority.HIGH,
        Priority.CRITICAL: MessagePriority.CRITICAL
    }

    success = broker.publish_to_topic(
        topic_name,
        message_data.content,
        priority=priority_mapping[message_data.priority],
        ttl_seconds=message_data.ttl_seconds,
        headers=message_data.headers
    )

    if not success:
        raise HTTPException(status_code=500, detail="Failed to publish message")

    return {
        "status": "published",
        "topic": topic_name,
        "priority": message_data.priority.value,
        "ttl_seconds": message_data.ttl_seconds,
        "timestamp": datetime.now().isoformat()
    }

# === ОЧЕРЕДИ ===

@app.post("/queues", response_model=QueueInfo, tags=["Queues"], summary="Создать очередь")
async def create_queue(queue_data: QueueCreate):
    """Создать новую очередь (FIFO или LIFO)"""
    try:
        queue_type = CoreQueueType.FIFO if queue_data.type == QueueType.FIFO else CoreQueueType.LIFO
        queue = broker.create_queue(queue_data.name, queue_type)
        return QueueInfo(**queue.get_info())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/queues", response_model=List[QueueInfo], tags=["Queues"], summary="Получить все очереди")
async def get_queues():
    """Получить список всех очередей"""
    queues_data = broker.get_all_queues()
    return [QueueInfo(**queue_data) for queue_data in queues_data]

@app.post("/queues/{queue_name}/publish", tags=["Publishing"],
          summary="Публикация в очередь")
async def publish_to_queue_enhanced(queue_name: str, message_data: MessageBase):
    """Опубликовать сообщение в очередь с приоритетом и TTL"""
    queue = broker.get_queue(queue_name)
    if not queue:
        raise HTTPException(status_code=404, detail="Queue not found")

    priority_mapping = {
        Priority.LOW: MessagePriority.LOW,
        Priority.NORMAL: MessagePriority.NORMAL,
        Priority.HIGH: MessagePriority.HIGH,
        Priority.CRITICAL: MessagePriority.CRITICAL
    }

    success = broker.publish_to_queue(
        queue_name,
        message_data.content,
        priority=priority_mapping[message_data.priority],
        ttl_seconds=message_data.ttl_seconds,
        headers=message_data.headers
    )

    if not success:
        raise HTTPException(status_code=500, detail="Failed to publish message")

    return {
        "status": "queued",
        "queue": queue_name,
        "priority": message_data.priority.value,
        "timestamp": datetime.now().isoformat()
    }

# === ПОДПИСКИ ===

@app.post("/subscriptions", tags=["Subscriptions"], summary="Создать подписку")
async def create_subscription(subscription_data: SubscriptionCreate):
    """Создать подписку на топик или очередь"""
    if subscription_data.target_type == "topic":
        if not broker.get_topic(subscription_data.target_name):
            raise HTTPException(status_code=404, detail="Topic not found")
    elif subscription_data.target_type == "queue":
        if not broker.get_queue(subscription_data.target_name):
            raise HTTPException(status_code=404, detail="Queue not found")
    else:
        raise HTTPException(status_code=400, detail="target_type must be 'topic' or 'queue'")

    subscription = broker.create_subscription(
        subscription_data.subscriber_id,
        subscription_data.target_type,
        subscription_data.target_name
    )

    return subscription.to_dict()

@app.get("/subscriptions/{subscriber_id}", tags=["Subscriptions"], summary="Получить подписки")
async def get_subscriptions(subscriber_id: str):
    """Получить все подписки пользователя"""
    subscriptions_data = broker.get_subscriptions(subscriber_id)
    return subscriptions_data

# === МОНИТОРИНГ И МЕТРИКИ ===

@app.get("/health", tags=["Monitoring"], summary="Проверка состояния")
async def health_check():
    """Проверка состояния брокера"""
    metrics = broker.get_metrics()
    return {
        "status": "healthy",
        "uptime_seconds": metrics["performance"]["uptime_seconds"],
        "topics": metrics["entities"]["topics"],
        "queues": metrics["entities"]["queues"],
        "subscribers": metrics["entities"]["subscribers"],
        "version": "1.0.0"
    }

@app.get("/metrics", tags=["Monitoring"], summary="Получить метрики")
async def get_metrics():
    """
    Получить подробные метрики брокера:
    - Статистика сообщений (опубликовано, доставлено, ошибки)
    - Количество сущностей (топики, очереди, подписчики)
    - Метрики производительности (скорость, время доставки)
    """
    return broker.get_metrics()

# === DEAD LETTER QUEUE ===

@app.get("/dlq", tags=["Dead Letter Queue"], summary="Получить DLQ")
async def get_dlq_messages():
    """Получить сообщения из Dead Letter Queue"""
    return broker.get_dlq_info()

@app.post("/dlq/{message_id}/reprocess", tags=["Dead Letter Queue"],
          summary="Переобработать сообщение")
async def reprocess_dlq_message(message_id: str):
    """Переместить сообщение из DLQ обратно для обработки"""
    if broker.dlq and broker.dlq.reprocess_message(message_id):
        return {"status": "success", "message": f"Message {message_id} reprocessed"}
    raise HTTPException(status_code=404, detail="Message not found in DLQ")

@app.delete("/dlq", tags=["Dead Letter Queue"], summary="Очистить DLQ")
async def clear_dlq():
    """Очистить Dead Letter Queue"""
    if broker.dlq:
        broker.dlq.clear()
        return {"status": "success", "message": "DLQ cleared"}
    raise HTTPException(status_code=404, detail="DLQ not available")

# === КОНФИГУРАЦИЯ ===

@app.get("/config", tags=["Configuration"], summary="Получить конфигурацию")
async def get_config():
    """Получить текущую конфигурацию брокера"""
    return broker.config.to_dict()

# === WebSocket ===

@app.websocket("/ws/{subscriber_id}")
async def websocket_endpoint(websocket: WebSocket, subscriber_id: str):
    """WebSocket для получения сообщений в реальном времени"""
    await websocket.accept()

    subscriber = broker.create_subscriber(subscriber_id, websocket)
    print(f"🔌 [WS] WebSocket connected for subscriber {subscriber_id}")

    try:
        while True:
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        print(f"🔌 [WS] WebSocket disconnected for subscriber {subscriber_id}")
    except Exception as e:
        print(f"❌ [WS] WebSocket error for subscriber {subscriber_id}: {e}")
    finally:
        if subscriber_id in broker.subscribers:
            broker.subscribers[subscriber_id].websocket = None

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
