from typing import List, Dict, Optional
from datetime import datetime
from collections import deque
from enum import Enum
from .message import Message
import threading

class QueueType(Enum):
    FIFO = "FIFO"  # First In, First Out
    LIFO = "LIFO"  # Last In, First Out

class Queue:
    def __init__(self, name: str, queue_type: QueueType = QueueType.FIFO):
        self.name = name
        self.type = queue_type
        self.subscribers: Dict[str, 'Subscriber'] = {}
        self.messages: deque = deque()
        self.created_at = datetime.now()
        self._lock = threading.RLock()
        self._current_subscriber_index = 0

    def add_subscriber(self, subscriber: 'Subscriber'):
        """Добавить подписчика к очереди"""
        with self._lock:
            self.subscribers[subscriber.id] = subscriber

    def remove_subscriber(self, subscriber_id: str):
        """Удалить подписчика из очереди"""
        with self._lock:
            self.subscribers.pop(subscriber_id, None)

    def publish_message(self, message: Message):
        """Опубликовать сообщение в очередь"""
        with self._lock:
            message.queue = self.name
            message.status = "queued"

            if self.type == QueueType.FIFO:
                self.messages.append(message)
            else:  # LIFO
                self.messages.appendleft(message)

            # Пытаемся доставить сообщение одному из подписчиков (round-robin)
            self._deliver_next_message()

    def _deliver_next_message(self):
        """Доставить следующее сообщение подписчику (round-robin)"""
        if not self.messages or not self.subscribers:
            return

        subscribers_list = list(self.subscribers.values())
        if not subscribers_list:
            return

        # Round-robin выбор подписчика
        subscriber = subscribers_list[self._current_subscriber_index % len(subscribers_list)]
        self._current_subscriber_index = (self._current_subscriber_index + 1) % len(subscribers_list)

        # Получаем сообщение из очереди
        if self.type == QueueType.FIFO:
            message = self.messages.popleft()
        else:  # LIFO
            message = self.messages.pop()

        try:
            message.status = "delivered"
            subscriber.deliver_message(message)
        except Exception as e:
            print(f"Error delivering message to subscriber {subscriber.id}: {e}")
            # Возвращаем сообщение в очередь при ошибке доставки
            if self.type == QueueType.FIFO:
                self.messages.appendleft(message)
            else:
                self.messages.append(message)

    def get_info(self) -> Dict:
        """Получить информацию об очереди"""
        with self._lock:
            return {
                "name": self.name,
                "type": self.type.value,
                "subscribers": list(self.subscribers.keys()),
                "message_count": len(self.messages),
                "created_at": self.created_at.isoformat()
            }
