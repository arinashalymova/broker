from typing import Optional, Callable, Any
from datetime import datetime
from .message import Message
import uuid
import asyncio
from fastapi import WebSocket

class Subscriber:
    def __init__(self, subscriber_id: str, websocket: Optional[WebSocket] = None):
        self.id = subscriber_id
        self.websocket = websocket
        self.subscriptions = {}
        self.created_at = datetime.now()
        self.last_message_id: Optional[str] = None
        self.message_callback: Optional[Callable] = None

    def set_message_callback(self, callback: Callable[[Message], None]):
        """Установить callback для обработки сообщений"""
        self.message_callback = callback

    def deliver_message(self, message: Message):
        """Доставить сообщение подписчику"""
        self.last_message_id = message.id

        if self.websocket:
            # Отправляем через WebSocket
            asyncio.create_task(self._send_websocket_message(message))
        elif self.message_callback:
            # Вызываем callback
            self.message_callback(message)
        else:
            # Логируем для демонстрации
            print(f"Message delivered to subscriber {self.id}: {message.content}")

    async def _send_websocket_message(self, message: Message):
        """Отправить сообщение через WebSocket"""
        try:
            if self.websocket:
                await self.websocket.send_json(message.to_dict())
        except Exception as e:
            print(f"Error sending WebSocket message to {self.id}: {e}")

class Subscription:
    def __init__(self, subscriber_id: str, target_type: str, target_name: str):
        self.id = str(uuid.uuid4())
        self.subscriber_id = subscriber_id
        self.target_type = target_type  # "topic" or "queue"
        self.target_name = target_name
        self.created_at = datetime.now()

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "subscriber_id": self.subscriber_id,
            "target_type": self.target_type,
            "target_name": self.target_name,
            "created_at": self.created_at.isoformat()
        }
