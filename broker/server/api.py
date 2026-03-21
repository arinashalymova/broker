from fastapi import FastAPI, HTTPException, WebSocket, Depends, BackgroundTasks, Query, Path
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any
from enum import Enum
from datetime import datetime
import uuid

app = FastAPI(
    title="Message Broker API",
    description="API для взаимодействия с брокером сообщений",
    version="0.1.0",
)

# Модели данных для Swagger
class QueueType(str, Enum):
    FIFO = "FIFO"
    LIFO = "LIFO"

class MessageBase(BaseModel):
    content: Any
    content_type: str = "application/json"
    headers: Dict[str, str] = Field(default_factory=dict)

class Message(MessageBase):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    topic: Optional[str] = None
    queue: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.now)
    status: str = "pending"

class TopicCreate(BaseModel):
    name: str

class TopicInfo(BaseModel):
    name: str
    subscribers: List[str] = Field(default_factory=list)
    message_count: int = 0
    created_at: datetime = Field(default_factory=datetime.now)

class QueueCreate(BaseModel):
    name: str
    type: QueueType = QueueType.FIFO

class QueueInfo(BaseModel):
    name: str
    type: QueueType
    subscribers: List[str] = Field(default_factory=list)
    message_count: int = 0
    created_at: datetime = Field(default_factory=datetime.now)

class SubscriptionCreate(BaseModel):
    subscriber_id: str
    target_type: str  # "topic" или "queue"
    target_name: str

class SubscriptionInfo(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    subscriber_id: str
    target_type: str
    target_name: str
    created_at: datetime = Field(default_factory=datetime.now)
    last_message_id: Optional[str] = None

# API для работы с топиками
@app.post("/topics", response_model=TopicInfo, tags=["Topics"], summary="Создать новый топик")
async def create_topic(topic: TopicCreate):
    """
    Создает новый топик для публикации сообщений.

    - **name**: Уникальное имя топика
    """
    return {"name": topic.name, "created_at": datetime.now()}

@app.get("/topics", response_model=List[TopicInfo], tags=["Topics"], summary="Получить список всех топиков")
async def get_topics():
    """
    Возвращает список всех доступных топиков.
    """
    return [{"name": "example-topic", "subscribers": [], "message_count": 0, "created_at": datetime.now()}]

@app.get("/topics/{topic_name}", response_model=TopicInfo, tags=["Topics"], summary="Получить информацию о топике")
async def get_topic(topic_name: str = Path(..., description="Имя топика")):
    """
    Возвращает информацию о конкретном топике.

    - **topic_name**: Имя топика
    """
    return {"name": topic_name, "subscribers": [], "message_count": 0, "created_at": datetime.now()}

@app.delete("/topics/{topic_name}", tags=["Topics"], summary="Удалить топик")
async def delete_topic(topic_name: str = Path(..., description="Имя топика")):
    """
    Удаляет указанный топик.

    - **topic_name**: Имя топика для удаления
    """
    return {"status": "success", "message": f"Topic {topic_name} deleted"}

# API для публикации сообщений
@app.post("/topics/{topic_name}/publish", response_model=Message, tags=["Publishing"], summary="Опубликовать сообщение в топик")
async def publish_to_topic(
        message: MessageBase,
        topic_name: str = Path(..., description="Имя топика для публикации"),
        background_tasks: BackgroundTasks = None
):
    """
    Публикует сообщение в указанный топик.

    - **topic_name**: Имя топика
    - **message**: Содержимое и метаданные сообщения
    """
    msg_id = str(uuid.uuid4())
    return {
        "id": msg_id,
        "topic": topic_name,
        "content": message.content,
        "content_type": message.content_type,
        "headers": message.headers,
        "timestamp": datetime.now(),
        "status": "delivered"
    }

# API для работы с очередями
@app.post("/queues", response_model=QueueInfo, tags=["Queues"], summary="Создать новую очередь")
async def create_queue(queue: QueueCreate):
    """
    Создает новую очередь для сообщений.

    - **name**: Уникальное имя очереди
    - **type**: Тип очереди (FIFO - первым пришел, первым ушел; LIFO - последним пришел, первым ушел)
    """
    return {
        "name": queue.name,
        "type": queue.type,
        "subscribers": [],
        "message_count": 0,
        "created_at": datetime.now()
    }

@app.get("/queues", response_model=List[QueueInfo], tags=["Queues"], summary="Получить список всех очередей")
async def get_queues():
    """
    Возвращает список всех доступных очередей.
    """
    return [{
        "name": "example-queue",
        "type": "FIFO",
        "subscribers": [],
        "message_count": 0,
        "created_at": datetime.now()
    }]

# API для подписок
@app.post("/subscriptions", response_model=SubscriptionInfo, tags=["Subscriptions"], summary="Создать подписку")
async def create_subscription(subscription: SubscriptionCreate):
    """
    Создает новую подписку на топик или очередь.

    - **subscriber_id**: Идентификатор подписчика
    - **target_type**: Тип цели (topic или queue)
    - **target_name**: Имя топика или очереди
    """
    return {
        "id": str(uuid.uuid4()),
        "subscriber_id": subscription.subscriber_id,
        "target_type": subscription.target_type,
        "target_name": subscription.target_name,
        "created_at": datetime.now()
    }

@app.get("/subscriptions/{subscriber_id}", response_model=List[SubscriptionInfo],
         tags=["Subscriptions"], summary="Получить подписки пользователя")
async def get_subscriptions(subscriber_id: str = Path(..., description="ID подписчика")):
    """
    Возвращает список всех подписок конкретного подписчика.

    - **subscriber_id**: Идентификатор подписчика
    """
    return [{
        "id": str(uuid.uuid4()),
        "subscriber_id": subscriber_id,
        "target_type": "topic",
        "target_name": "example-topic",
        "created_at": datetime.now()
    }]

# WebSocket для получения сообщений
@app.websocket("/ws/{subscriber_id}")
async def websocket_endpoint(websocket: WebSocket, subscriber_id: str):
    """
    WebSocket соединение для получения сообщений в реальном времени.
    """
    await websocket.accept()
    try:
        while True:
            # Здесь будет логика отправки сообщений подписчику
            await websocket.send_json({"message": "Test message", "timestamp": str(datetime.now())})
            # В реальном коде нужно будет дожидаться появления новых сообщений
            await websocket.receive_text()  # Блокирующий вызов для поддержания соединения
    except:
        await websocket.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
