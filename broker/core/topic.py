from typing import List, Dict, Optional
from datetime import datetime
from collections import deque
from .message import Message, MessageStatus, MessagePriority
import threading
import time

class Topic:
    def __init__(self, name: str, config=None, metrics=None, dlq=None):
        self.name = name
        self.config = config
        self.metrics = metrics
        self.dlq = dlq
        self.subscribers: Dict[str, 'Subscriber'] = {}
        self.messages: deque = deque()
        self.created_at = datetime.now()
        self._lock = threading.RLock()

    def add_subscriber(self, subscriber: 'Subscriber'):
        """Добавить подписчика к топику"""
        with self._lock:
            self.subscribers[subscriber.id] = subscriber
            print(f"👤 [TOPIC] Subscriber {subscriber.id} added to topic {self.name}")

    def remove_subscriber(self, subscriber_id: str):
        """Удалить подписчика из топика"""
        with self._lock:
            if subscriber_id in self.subscribers:
                self.subscribers.pop(subscriber_id)
                print(f"👤 [TOPIC] Subscriber {subscriber_id} removed from topic {self.name}")

    def publish_message(self, message: Message):
        """Опубликовать сообщение с поддержкой приоритетов и TTL"""
        with self._lock:
            message.topic = self.name
            message.status = MessageStatus.PUBLISHED

            if message.is_expired():
                message.status = MessageStatus.EXPIRED
                if self.metrics:
                    self.metrics.increment_expired()
                print(f"⏰ [TOPIC] Message {message.id} expired (TTL: {message.ttl_seconds}s)")
                return

            if message.priority in [MessagePriority.HIGH, MessagePriority.CRITICAL]:
                inserted = False
                for i, existing_msg in enumerate(self.messages):
                    if existing_msg.priority.value < message.priority.value:
                        self.messages.insert(i, message)
                        inserted = True
                        break
                if not inserted:
                    self.messages.append(message)
                print(f"🚀 [TOPIC] High priority message {message.id} published to {self.name}")
            else:
                self.messages.append(message)
                print(f"📤 [TOPIC] Message {message.id} published to {self.name}")

            self._deliver_to_subscribers(message)

    def _deliver_to_subscribers(self, message: Message):
        """Доставить сообщение всем подписчикам"""
        delivery_count = 0

        for subscriber in self.subscribers.values():
            try:
                start_time = time.time()
                subscriber.deliver_message(message)
                delivery_time = (time.time() - start_time) * 1000

                delivery_count += 1
                if self.metrics:
                    self.metrics.increment_delivered(delivery_time)

            except Exception as e:
                print(f"❌ [TOPIC] Error delivering to subscriber {subscriber.id}: {e}")
                message.mark_retry()
                message.status = MessageStatus.FAILED

                if self.metrics:
                    self.metrics.increment_failed()

                if not message.can_retry() and self.dlq:
                    self.dlq.add_message(message, f"delivery_failed_to_{subscriber.id}")
                    if self.metrics:
                        self.metrics.increment_dlq()

        if delivery_count > 0:
            print(f"✅ [TOPIC] Message {message.id} delivered to {delivery_count} subscribers")

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
                print(f"🗑️ [TOPIC] Cleaned up {expired_count} expired messages from {self.name}")
                if self.metrics:
                    for _ in range(expired_count):
                        self.metrics.increment_expired()

    def get_info(self) -> Dict:
        """Получить информацию о топике"""
        with self._lock:
            return {
                "name": self.name,
                "subscribers": list(self.subscribers.keys()),
                "message_count": len(self.messages),
                "created_at": self.created_at.isoformat()
            }
