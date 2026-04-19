from typing import Dict, List, Optional
import threading
import time
from datetime import datetime

from .topic import Topic
from .queue import Queue, QueueType
from .message import Message, MessageStatus, MessagePriority
from .subscription import Subscriber, Subscription
from .dead_letter_queue import DeadLetterQueue
from .metrics import Metrics
from .config import BrokerConfig

class MessageBroker:
    def __init__(self, config: BrokerConfig = None):
        self.config = config or BrokerConfig()

        # Основные структуры данных
        self.topics: Dict[str, Topic] = {}
        self.queues: Dict[str, Queue] = {}
        self.subscribers: Dict[str, Subscriber] = {}
        self.subscriptions: Dict[str, Subscription] = {}

        # Дополнительные компоненты
        self.dlq = DeadLetterQueue(max_size=self.config.dlq_max_size) if self.config.enable_dlq else None
        self.metrics = Metrics()

        # Блокировки и фоновые задачи
        self._lock = threading.RLock()
        self._running = True

        # Запускаем фоновые задачи
        self._start_background_tasks()

        print(f"🚀 MessageBroker инициализирован:")
        print(f"   - DLQ: {'✅' if self.dlq else '❌'}")
        print(f"   - Max topics: {self.config.max_topics}")
        print(f"   - Default TTL: {self.config.default_message_ttl}")

    def _start_background_tasks(self):
        """Запустить фоновые задачи"""
        # Задача очистки истекших сообщений
        cleanup_thread = threading.Thread(target=self._cleanup_expired_messages, daemon=True)
        cleanup_thread.start()

        # Задача retry
        retry_thread = threading.Thread(target=self._retry_failed_messages, daemon=True)
        retry_thread.start()

    def _cleanup_expired_messages(self):
        """Фоновая задача очистки истекших сообщений"""
        while self._running:
            try:
                time.sleep(self.config.cleanup_interval)

                with self._lock:
                    # Очистка в топиках
                    for topic in self.topics.values():
                        topic.cleanup_expired_messages()

                    # Очистка в очередях
                    for queue in self.queues.values():
                        if hasattr(queue, 'cleanup_expired_messages'):
                            queue.cleanup_expired_messages()

            except Exception as e:
                print(f"❌ [CLEANUP] Error: {e}")
                time.sleep(60)

    def _retry_failed_messages(self):
        """Фоновая задача повторной доставки"""
        while self._running:
            try:
                time.sleep(self.config.retry_delay_seconds)

                with self._lock:
                    current_time = datetime.now()

                    # Проверяем сообщения в топиках
                    for topic in self.topics.values():
                        for msg in list(topic.messages):
                            if (msg.status == MessageStatus.FAILED and
                                    msg.can_retry() and
                                    msg.last_retry and
                                    (current_time - msg.last_retry).seconds >= self.config.retry_delay_seconds):

                                msg.mark_retry()
                                msg.status = MessageStatus.PENDING
                                print(f"🔄 [RETRY] Retrying message {msg.id} (attempt {msg.retry_count})")
                                topic._deliver_to_subscribers(msg)

            except Exception as e:
                print(f"❌ [RETRY] Error: {e}")
                time.sleep(60)

    # === Методы для топиков ===
    def create_topic(self, name: str) -> Topic:
        """Создать новый топик с проверками"""
        with self._lock:
            if len(self.topics) >= self.config.max_topics:
                raise ValueError(f"Maximum number of topics ({self.config.max_topics}) reached")

            if name in self.topics:
                raise ValueError(f"Topic '{name}' already exists")

            topic = Topic(name, self.config, self.metrics, self.dlq)
            self.topics[name] = topic
            self._update_metrics()
            print(f"📂 [BROKER] Topic '{name}' created")
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
                self._update_metrics()
                print(f"🗑️ [BROKER] Topic '{name}' deleted")
                return True
            return False

    # === Методы для очередей ===
    def create_queue(self, name: str, queue_type: QueueType = QueueType.FIFO) -> Queue:
        """Создать новую очередь с проверками"""
        with self._lock:
            if len(self.queues) >= self.config.max_queues:
                raise ValueError(f"Maximum number of queues ({self.config.max_queues}) reached")

            if name in self.queues:
                raise ValueError(f"Queue '{name}' already exists")

            queue = Queue(name, queue_type, self.config, self.metrics, self.dlq)
            self.queues[name] = queue
            self._update_metrics()
            print(f"📋 [BROKER] Queue '{name}' ({queue_type.value}) created")
            return queue

    def get_queue(self, name: str) -> Optional[Queue]:
        """Получить очередь по имени"""
        return self.queues.get(name)

    def get_all_queues(self) -> List[Dict]:
        """Получить список всех очередей"""
        with self._lock:
            return [queue.get_info() for queue in self.queues.values()]

    # === Публикация сообщений ===
    def publish_to_topic(self, topic_name: str, content,
                         priority: MessagePriority = MessagePriority.NORMAL,
                         ttl_seconds: int = None, headers: Dict[str, str] = None) -> bool:
        """Опубликовать сообщение в топик с расширенными параметрами"""
        topic = self.get_topic(topic_name)
        if not topic:
            print(f"❌ [BROKER] Topic '{topic_name}' not found")
            return False

        # Используем TTL по умолчанию если не указан
        if ttl_seconds is None:
            ttl_seconds = self.config.default_message_ttl

        message = Message(
            content=content,
            headers=headers or {},
            priority=priority,
            ttl_seconds=ttl_seconds,
            max_retries=self.config.max_retry_attempts
        )

        self.metrics.increment_published()
        topic.publish_message(message)
        return True

    def publish_to_queue(self, queue_name: str, content,
                         priority: MessagePriority = MessagePriority.NORMAL,
                         ttl_seconds: int = None, headers: Dict[str, str] = None) -> bool:
        """Опубликовать сообщение в очередь"""
        queue = self.get_queue(queue_name)
        if not queue:
            print(f"❌ [BROKER] Queue '{queue_name}' not found")
            return False

        message = Message(
            content=content,
            headers=headers or {},
            priority=priority,
            ttl_seconds=ttl_seconds,
            max_retries=self.config.max_retry_attempts
        )

        self.metrics.increment_published()
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
            self._update_metrics()
            print(f"👤 [BROKER] Subscriber '{subscriber_id}' created")
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

            print(f"📌 [BROKER] Subscription created: {subscriber_id} -> {target_type}:{target_name}")
            return subscription

    def get_subscriptions(self, subscriber_id: str) -> List[Dict]:
        """Получить подписки подписчика"""
        with self._lock:
            return [
                sub.to_dict() for sub in self.subscriptions.values()
                if sub.subscriber_id == subscriber_id
            ]

    # === Метрики и информация ===
    def get_metrics(self) -> Dict:
        """Получить метрики брокера"""
        self._update_metrics()
        return self.metrics.to_dict()

    def get_dlq_info(self) -> Dict:
        """Получить информацию о DLQ"""
        if self.dlq:
            return self.dlq.get_info()
        return {"error": "DLQ is disabled"}

    def _update_metrics(self):
        """Обновить счетчики в метриках"""
        self.metrics.update_counts(
            len(self.topics),
            len(self.queues),
            len(self.subscribers)
        )

    def shutdown(self):
        """Корректное завершение работы брокера"""
        print("🛑 [BROKER] Shutting down...")
        self._running = False

config = BrokerConfig()
broker = MessageBroker(config)
