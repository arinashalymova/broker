from typing import List, Dict, Optional
from datetime import datetime
from collections import deque
from .message import Message
import threading

class Topic:
    def __init__(self, name: str):
        self.name = name
        self.subscribers: Dict[str, 'Subscriber'] = {}
        self.messages: deque = deque()
        self.created_at = datetime.now()
        self._lock = threading.RLock()

    def add_subscriber(self, subscriber: 'Subscriber'):
        """Добавить подписчика к топику"""
        with self._lock:
            self.subscribers[subscriber.id] = subscriber

    def remove_subscriber(self, subscriber_id: str):
        """Удалить подписчика из топика"""
        with self._lock:
            self.subscribers.pop(subscriber_id, None)

    def publish_message(self, message: Message):
        """Опубликовать сообщение в топик"""
        with self._lock:
            message.topic = self.name
            message.status = "published"
            self.messages.append(message)

            # Отправляем сообщение всем подписчикам
            for subscriber in self.subscribers.values():
                try:
                    subscriber.deliver_message(message)
                except Exception as e:
                    print(f"Error delivering message to subscriber {subscriber.id}: {e}")

    def get_info(self) -> Dict:
        """Получить информацию о топике"""
        with self._lock:
            return {
                "name": self.name,
                "subscribers": list(self.subscribers.keys()),
                "message_count": len(self.messages),
                "created_at": self.created_at.isoformat()
            }
