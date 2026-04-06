from typing import Dict, List, Optional
from .topic import Topic
from .queue import Queue, QueueType
from .message import Message
from .subscription import Subscriber, Subscription
import threading
from datetime import datetime

class MessageBroker:
    def __init__(self):
        self.topics: Dict[str, Topic] = {}
        self.queues: Dict[str, Queue] = {}
        self.subscribers: Dict[str, Subscriber] = {}
        self.subscriptions: Dict[str, Subscription] = {}
        self._lock = threading.RLock()

    # === Работа с топиками ===
    def create_topic(self, name: str) -> Topic:
        """Создать новый топик"""
        with self._lock:
            if name in self.topics:
                raise ValueError(f"Topic '{name}' already exists")

            topic = Topic(name)
            self.topics[name] = topic
            return topic

    def get_topic(self, name: str) -> Optional[Topic]:
        """Получить топик по имени"""
        return self.topics.get(name)

    def get_all_topics(self) -> List[Dict]:
        """Получить список всех топиков"""
        with self._lock:
            return [topic.get_info() for topic in self.topics.values()]

    def delete_topic(self, name: str) -> bool:
        """Удалить топик"""
        with self._lock:
            if name in self.topics:
                # Удаляем все подписки на этот топик
                subscriptions_to_remove = [
                    sub_id for sub_id, sub in self.subscriptions.items()
                    if sub.target_type == "topic" and sub.target_name == name
                ]
                for sub_id in subscriptions_to_remove:
                    del self.subscriptions[sub_id]

                del self.topics[name]
                return True
            return False

    # === Работа с очередями ===
    def create_queue(self, name: str, queue_type: QueueType = QueueType.FIFO) -> Queue:
        """Создать новую очередь"""
        with self._lock:
            if name in self.queues:
                raise ValueError(f"Queue '{name}' already exists")

            queue = Queue(name, queue_type)
            self.queues[name] = queue
            return queue

    def get_queue(self, name: str) -> Optional[Queue]:
        """Получить очередь по имени"""
        return self.queues.get(name)

    def get_all_queues(self) -> List[Dict]:
        """Получить список всех очередей"""
        with self._lock:
            return [queue.get_info() for queue in self.queues.values()]

    def delete_queue(self, name: str) -> bool:
        """Удалить очередь"""
        with self._lock:
            if name in self.queues:
                # Удаляем все подписки на эту очередь
                subscriptions_to_remove = [
                    sub_id for sub_id, sub in self.subscriptions.items()
                    if sub.target_type == "queue" and sub.target_name == name
                ]
                for sub_id in subscriptions_to_remove:
                    del self.subscriptions[sub_id]

                del self.queues[name]
                return True
            return False

    # === Публикация сообщений ===
    def publish_to_topic(self, topic_name: str, message: Message) -> bool:
        """Опубликовать сообщение в топик"""
        topic = self.get_topic(topic_name)
        if not topic:
            return False

        topic.publish_message(message)
        return True

    def publish_to_queue(self, queue_name: str, message: Message) -> bool:
        """Опубликовать сообщение в очередь"""
        queue = self.get_queue(queue_name)
        if not queue:
            return False

        queue.publish_message(message)
        return True

    # === Подписки ===
    def create_subscriber(self, subscriber_id: str, websocket=None) -> Subscriber:
        """Создать нового подписчика"""
        with self._lock:
            if subscriber_id in self.subscribers:
                return self.subscribers[subscriber_id]

            subscriber = Subscriber(subscriber_id, websocket)
            self.subscribers[subscriber_id] = subscriber
            return subscriber

    def create_subscription(self, subscriber_id: str, target_type: str, target_name: str) -> Subscription:
        """Создать подписку"""
        with self._lock:
            # Создаем подписчика если его нет
            subscriber = self.create_subscriber(subscriber_id)

            # Создаем подписку
            subscription = Subscription(subscriber_id, target_type, target_name)
            self.subscriptions[subscription.id] = subscription

            # Привязываем подписчика к топику/очереди
            if target_type == "topic":
                topic = self.get_topic(target_name)
                if topic:
                    topic.add_subscriber(subscriber)
            elif target_type == "queue":
                queue = self.get_queue(target_name)
                if queue:
                    queue.add_subscriber(subscriber)

            return subscription

    def get_subscriptions(self, subscriber_id: str) -> List[Dict]:
        """Получить подписки подписчика"""
        with self._lock:
            return [
                sub.to_dict() for sub in self.subscriptions.values()
                if sub.subscriber_id == subscriber_id
            ]

    def delete_subscription(self, subscription_id: str) -> bool:
        """Удалить подписку"""
        with self._lock:
            if subscription_id not in self.subscriptions:
                return False

            subscription = self.subscriptions[subscription_id]
            subscriber = self.subscribers.get(subscription.subscriber_id)

            if subscriber:
                # Удаляем подписчика из топика/очереди
                if subscription.target_type == "topic":
                    topic = self.get_topic(subscription.target_name)
                    if topic:
                        topic.remove_subscriber(subscription.subscriber_id)
                elif subscription.target_type == "queue":
                    queue = self.get_queue(subscription.target_name)
                    if queue:
                        queue.remove_subscriber(subscription.subscriber_id)

            del self.subscriptions[subscription_id]
            return True

# Глобальный экземпляр брокера
broker = MessageBroker()
