from typing import List, Dict, Optional
from datetime import datetime
from collections import deque
from enum import Enum
from .message import Message, MessageStatus, MessagePriority
import threading
import time

class QueueType(Enum):
    FIFO = "FIFO"  # First In, First Out
    LIFO = "LIFO"  # Last In, First Out

class Queue:
    def __init__(self, name: str, queue_type: QueueType = QueueType.FIFO, config=None, metrics=None, dlq=None):
        self.name = name
        self.type = queue_type
        self.config = config
        self.metrics = metrics
        self.dlq = dlq
        self.subscribers: Dict[str, 'Subscriber'] = {}
        self.messages: deque = deque()
        self.created_at = datetime.now()
        self._lock = threading.RLock()
        self._current_subscriber_index = 0

    def add_subscriber(self, subscriber):
        """Добавить подписчика к очереди"""
        with self._lock:
            self.subscribers[subscriber.id] = subscriber
            print(f"👤 [QUEUE] Subscriber {subscriber.id} added to queue {self.name}")

    def remove_subscriber(self, subscriber_id: str):
        """Удалить подписчика из очереди"""
        with self._lock:
            if subscriber_id in self.subscribers:
                self.subscribers.pop(subscriber_id)
                print(f"👤 [QUEUE] Subscriber {subscriber_id} removed from queue {self.name}")

    def publish_message(self, message: Message):
        """Опубликовать сообщение в очередь с поддержкой приоритетов"""
        with self._lock:
            message.queue = self.name
            message.status = MessageStatus.PUBLISHED

            # Проверяем TTL
            if message.is_expired():
                message.status = MessageStatus.EXPIRED
                if self.metrics:
                    self.metrics.increment_expired()
                print(f"⏰ [QUEUE] Message {message.id} expired (TTL: {message.ttl_seconds}s)")
                return

            # Добавляем в очередь с учетом типа и приоритета
            if message.priority in [MessagePriority.HIGH, MessagePriority.CRITICAL]:
                # Высокий приоритет
                if self.type == QueueType.FIFO:
                    # В FIFO высокий приоритет добавляется в начало
                    self.messages.appendleft(message)
                else:  # LIFO
                    # В LIFO высокий приоритет тоже в начало (будет обработан первым)
                    self.messages.appendleft(message)
                print(f"🚀 [QUEUE] High priority message {message.id} queued in {self.name}")
            else:
                # Обычный приоритет
                if self.type == QueueType.FIFO:
                    self.messages.append(message)
                else:  # LIFO
                    self.messages.appendleft(message)
                print(f"📥 [QUEUE] Message {message.id} queued in {self.name} ({self.type.value})")

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
            start_time = time.time()
            message.status = MessageStatus.DELIVERED
            subscriber.deliver_message(message)
            delivery_time = (time.time() - start_time) * 1000

            if self.metrics:
                self.metrics.increment_delivered(delivery_time)

            print(f"✅ [QUEUE] Message {message.id} delivered to {subscriber.id}")

        except Exception as e:
            print(f"❌ [QUEUE] Error delivering to subscriber {subscriber.id}: {e}")
            message.mark_retry()
            message.status = MessageStatus.FAILED

            if self.metrics:
                self.metrics.increment_failed()

            # Возвращаем сообщение в очередь при ошибке доставки
            if message.can_retry():
                if self.type == QueueType.FIFO:
                    self.messages.appendleft(message)
                else:
                    self.messages.append(message)
            elif self.dlq:
                # Отправляем в DLQ если исчерпаны попытки
                self.dlq.add_message(message, f"delivery_failed_to_{subscriber.id}")
                if self.metrics:
                    self.metrics.increment_dlq()

    def cleanup_expired_messages(self):
        """Очистка истекших сообщений"""
        with self._lock:
            expired_count = 0
            messages_to_remove = []

            for msg in self.messages:
                if msg.is_expired():
                    messages_to_remove.append(msg)
                    msg.status = MessageStatus.EXPIRED
                    expired_count += 1

            for msg in messages_to_remove:
                self.messages.remove(msg)

            if expired_count > 0:
                print(f"🗑️ [QUEUE] Cleaned up {expired_count} expired messages from {self.name}")
                if self.metrics:
                    for _ in range(expired_count):
                        self.metrics.increment_expired()

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
