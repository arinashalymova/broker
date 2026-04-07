from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, BackgroundTasks, Query, Path
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any
from enum import Enum
from datetime import datetime
import uuid

from broker.core.broker import broker
from broker.core.message import Message as CoreMessage
from broker.core.queue import QueueType as CoreQueueType

app = FastAPI(
    title="Message Broker API",
    description="API для взаимодействия с брокером сообщений",
    version="0.1.0",
)

class QueueType(str, Enum):
    FIFO = "FIFO"
    LIFO = "LIFO"

class MessageBase(BaseModel):
    content: Any
    content_type: str = "application/json"
    headers: Dict[str, str] = Field(default_factory=dict)

class Message(BaseModel):
    id: str
    content: Any
    content_type: str
    headers: Dict[str, str]
    topic: Optional[str] = None
    queue: Optional[str] = None
    timestamp: str
    status: str

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

class SubscriptionInfo(BaseModel):
    id: str
    subscriber_id: str
    target_type: str
    target_name: str
    created_at: str

# === API для работы с топиками ===
@app.post("/topics", response_model=TopicInfo, tags=["Topics"])
async def create_topic(topic_data: TopicCreate):
    """Создать новый топик"""
    try:
        topic = broker.create_topic(topic_data.name)
        return TopicInfo(**topic.get_info())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/topics", response_model=List[TopicInfo], tags=["Topics"])
async def get_topics():
    """Получить список всех топиков"""
    topics_data = broker.get_all_topics()
    return [TopicInfo(**topic_data) for topic_data in topics_data]

@app.get("/topics/{topic_name}", response_model=TopicInfo, tags=["Topics"])
async def get_topic(topic_name: str):
    """Получить информацию о топике"""
    topic = broker.get_topic(topic_name)
    if not topic:
        raise HTTPException(status_code=404, detail="Topic not found")
    return TopicInfo(**topic.get_info())

@app.delete("/topics/{topic_name}", tags=["Topics"])
async def delete_topic(topic_name: str):
    """Удалить топик"""
    if not broker.delete_topic(topic_name):
        raise HTTPException(status_code=404, detail="Topic not found")
    return {"status": "success", "message": f"Topic {topic_name} deleted"}

# === API для публикации сообщений ===
@app.post("/topics/{topic_name}/publish", response_model=Message, tags=["Publishing"])
async def publish_to_topic(topic_name: str, message_data: MessageBase):
    """Опубликовать сообщение в топик"""
    topic = broker.get_topic(topic_name)
    if not topic:
        raise HTTPException(status_code=404, detail="Topic not found")

    message = CoreMessage(
        content=message_data.content,
        content_type=message_data.content_type,
        headers=message_data.headers
    )

    if broker.publish_to_topic(topic_name, message):
        return Message(
            id=message.id,
            content=message.content,
            content_type=message.content_type,
            headers=message.headers,
            topic=message.topic,
            timestamp=message.timestamp.isoformat(),
            status=message.status
        )
    else:
        raise HTTPException(status_code=500, detail="Failed to publish message")

# === API для работы с очередями ===
@app.post("/queues", response_model=QueueInfo, tags=["Queues"])
async def create_queue(queue_data: QueueCreate):
    """Создать новую очередь"""
    try:
        queue_type = CoreQueueType.FIFO if queue_data.type == QueueType.FIFO else CoreQueueType.LIFO
        queue = broker.create_queue(queue_data.name, queue_type)
        return QueueInfo(**queue.get_info())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/queues", response_model=List[QueueInfo], tags=["Queues"])
async def get_queues():
    """Получить список всех очередей"""
    queues_data = broker.get_all_queues()
    return [QueueInfo(**queue_data) for queue_data in queues_data]

@app.get("/queues/{queue_name}", response_model=QueueInfo, tags=["Queues"])
async def get_queue(queue_name: str):
    """Получить информацию об очереди"""
    queue = broker.get_queue(queue_name)
    if not queue:
        raise HTTPException(status_code=404, detail="Queue not found")
    return QueueInfo(**queue.get_info())

@app.post("/queues/{queue_name}/publish", response_model=Message, tags=["Publishing"])
async def publish_to_queue(queue_name: str, message_data: MessageBase):
    """Опубликовать сообщение в очередь"""
    queue = broker.get_queue(queue_name)
    if not queue:
        raise HTTPException(status_code=404, detail="Queue not found")

    message = CoreMessage(
        content=message_data.content,
        content_type=message_data.content_type,
        headers=message_data.headers
    )

    if broker.publish_to_queue(queue_name, message):
        return Message(
            id=message.id,
            content=message.content,
            content_type=message.content_type,
            headers=message.headers,
            queue=message.queue,
            timestamp=message.timestamp.isoformat(),
            status=message.status
        )
    else:
        raise HTTPException(status_code=500, detail="Failed to publish message")

# === API для подписок ===
@app.post("/subscriptions", response_model=SubscriptionInfo, tags=["Subscriptions"])
async def create_subscription(subscription_data: SubscriptionCreate):
    """Создать подписку"""
    # Проверяем существование цели подписки
    if subscription_data.target_type == "topic":
        if not broker.get_topic(subscription_data.target_name):
            raise HTTPException(status_code=404, detail="Topic not found")
    elif subscription_data.target_type == "queue":
        if not broker.get_queue(subscription_data.target_name):
            raise HTTPException(status_code=404, detail="Queue not found")
    else:
        raise HTTPException(status_code=400, detail="Invalid target_type. Must be 'topic' or 'queue'")

    subscription = broker.create_subscription(
        subscription_data.subscriber_id,
        subscription_data.target_type,
        subscription_data.target_name
    )

    return SubscriptionInfo(**subscription.to_dict())

@app.get("/subscriptions/{subscriber_id}", response_model=List[SubscriptionInfo], tags=["Subscriptions"])
async def get_subscriptions(subscriber_id: str):
    """Получить подписки пользователя"""
    subscriptions_data = broker.get_subscriptions(subscriber_id)
    return [SubscriptionInfo(**sub_data) for sub_data in subscriptions_data]

# === WebSocket для получения сообщений ===
@app.websocket("/ws/{subscriber_id}")
async def websocket_endpoint(websocket: WebSocket, subscriber_id: str):
    """WebSocket соединение для получения сообщений"""
    await websocket.accept()

    # Создаем или получаем подписчика с WebSocket
    subscriber = broker.create_subscriber(subscriber_id, websocket)

    try:
        while True:
            # Ждем сообщения от клиента (для поддержания соединения)
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        # Клиент отключился
        print(f"WebSocket disconnected for subscriber {subscriber_id}")
    except Exception as e:
        print(f"WebSocket error for subscriber {subscriber_id}: {e}")
    finally:
        # Убираем WebSocket из подписчика
        if subscriber_id in broker.subscribers:
            broker.subscribers[subscriber_id].websocket = None

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
