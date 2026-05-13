import threading
import time
from collections import OrderedDict
from datetime import datetime
from typing import Dict, List, Optional

from .topic import Topic
from .message import Message, MessageStatus, MessagePriority
from .subscription import Subscriber, Subscription
from .dead_letter_queue import DeadLetterQueue
from .metrics import Metrics
from .config import BrokerConfig
from ..persistence.storage import JsonlLogStorage


class MessageBroker:
    def __init__(self, config: BrokerConfig = None, data_dir: str = "data"):
        self.config = config or BrokerConfig()

        self.topics: Dict[str, Topic] = {}
        self.subscribers: Dict[str, Subscriber] = {}
        self.subscriptions: Dict[str, Subscription] = {}

        self.dlq = (
            DeadLetterQueue(max_size=self.config.dlq_max_size)
            if self.config.enable_dlq
            else None
        )
        self.metrics = Metrics()

        # Persistent storage
        self.storage = JsonlLogStorage(data_dir=data_dir)

        # Producer dedup: client_message_id → server message_id (LRU, max 10 000 entries)
        self._producer_dedup: OrderedDict[str, str] = OrderedDict(
            self.storage.load_dedup_cache()
        )
        self._dedup_max = 10_000

        self._lock = threading.RLock()
        self._running = True

        # Restore state from disk before starting background tasks
        self._restore_from_disk()
        self._start_background_tasks()

        print(
            f"[BROKER] Initialized | DLQ: {bool(self.dlq)} | "
            f"topics: {len(self.topics)}"
        )

    # ── Background tasks ──────────────────────────────────────────────────────

    def _start_background_tasks(self):
        threading.Thread(
            target=self._cleanup_loop, daemon=True, name="broker-cleanup"
        ).start()
        threading.Thread(
            target=self._retry_loop, daemon=True, name="broker-retry"
        ).start()
        threading.Thread(
            target=self._dedup_persist_loop, daemon=True, name="broker-dedup-persist"
        ).start()

    def _cleanup_loop(self):
        while self._running:
            time.sleep(self.config.cleanup_interval)
            try:
                with self._lock:
                    for topic in self.topics.values():
                        topic.cleanup_expired_messages()
            except Exception as e:
                print(f"[BROKER] Cleanup error: {e}")

    def _retry_loop(self):
        """Check in-flight messages in every topic every 0.5 s."""
        while self._running:
            time.sleep(0.5)
            try:
                with self._lock:
                    for topic in self.topics.values():
                        topic.check_inflight_timeouts()
            except Exception as e:
                print(f"[BROKER] Retry loop error: {e}")

    def _dedup_persist_loop(self):
        """Persist dedup cache to disk every 30 s so it survives restarts."""
        while self._running:
            time.sleep(30)
            try:
                with self._lock:
                    self.storage.save_dedup_cache(
                        dict(self._producer_dedup), self._dedup_max
                    )
            except Exception as e:
                print(f"[BROKER] Dedup persist error: {e}")

    # ── Restore from disk ─────────────────────────────────────────────────────

    def _restore_from_disk(self):
        """Restore topics, subscriptions, and offsets from disk."""
        meta = self.storage.load_meta()

        # Restore topics
        for topic_info in meta.get("topics", []):
            name = topic_info if isinstance(topic_info, str) else topic_info.get("name")
            if name and name not in self.topics:
                topic = self._make_topic(name)
                topic.load_from_storage()
                self.topics[name] = topic
                print(f"[BROKER] Restored topic '{name}'")

        # Restore subscribers and subscriptions
        for sub_info in meta.get("subscriptions", []):
            sub_id = sub_info.get("subscriber_id")
            t_type = sub_info.get("target_type")
            t_name = sub_info.get("target_name")
            if not (sub_id and t_type and t_name):
                continue

            if sub_id not in self.subscribers:
                self.subscribers[sub_id] = Subscriber(sub_id)

            sub = Subscription(sub_id, t_type, t_name)
            sub.id = sub_info.get("id", sub.id)
            self.subscriptions[sub.id] = sub

            subscriber = self.subscribers[sub_id]
            if t_type == "topic":
                topic = self.topics.get(t_name)
                if topic:
                    # replay=False on restore; replay happens when subscriber reconnects via WS
                    topic.add_subscriber(subscriber, replay=False)

        print(
            f"[BROKER] Restore complete: {len(self.topics)} topics, "
            f"{len(self.subscriptions)} subscriptions"
        )

    def _save_meta(self):
        """Persist current topics/subscriptions to meta.json."""
        meta = {
            "topics": [{"name": name} for name in self.topics],
            "subscriptions": [s.to_dict() for s in self.subscriptions.values()],
        }
        self.storage.save_meta(meta)

    # ── Producer dedup ────────────────────────────────────────────────────────

    def _check_dedup(self, client_message_id: Optional[str]) -> Optional[str]:
        """Return existing server message_id if this client_message_id was seen before."""
        if not client_message_id:
            return None
        return self._producer_dedup.get(client_message_id)

    def _register_dedup(self, client_message_id: Optional[str], server_message_id: str):
        if not client_message_id:
            return
        if client_message_id in self._producer_dedup:
            self._producer_dedup.move_to_end(client_message_id)
        else:
            self._producer_dedup[client_message_id] = server_message_id
            if len(self._producer_dedup) > self._dedup_max:
                self._producer_dedup.popitem(last=False)

    # ── Topics ────────────────────────────────────────────────────────────────

    def _make_topic(self, name: str) -> Topic:
        return Topic(
            name,
            storage=self.storage,
            config=self.config,
            metrics=self.metrics,
            dlq=self.dlq,
        )

    def create_topic(self, name: str) -> Topic:
        with self._lock:
            if len(self.topics) >= self.config.max_topics:
                raise ValueError(f"Max topics ({self.config.max_topics}) reached")
            if name in self.topics:
                raise ValueError(f"Topic '{name}' already exists")
            topic = self._make_topic(name)
            self.topics[name] = topic
            self._update_metrics()
            self._save_meta()
            print(f"[BROKER] Topic '{name}' created")
            return topic

    def get_topic(self, name: str) -> Optional[Topic]:
        return self.topics.get(name)

    def get_all_topics(self) -> List[Dict]:
        with self._lock:
            return [t.get_info() for t in self.topics.values()]

    def delete_topic(self, name: str) -> bool:
        with self._lock:
            if name not in self.topics:
                return False
            subs = [
                sid
                for sid, s in self.subscriptions.items()
                if s.target_type == "topic" and s.target_name == name
            ]
            for sid in subs:
                del self.subscriptions[sid]
            del self.topics[name]
            self._update_metrics()
            self._save_meta()
            print(f"[BROKER] Topic '{name}' deleted")
            return True

    # ── Publish ───────────────────────────────────────────────────────────────

    def publish_to_topic(
        self,
        topic_name: str,
        content,
        priority: MessagePriority = MessagePriority.NORMAL,
        ttl_seconds: int = None,
        headers: Dict[str, str] = None,
        client_message_id: Optional[str] = None,
    ) -> Optional[Dict]:
        """Returns {"message_id": ..., "offset": ...} or None on error."""
        topic = self.get_topic(topic_name)
        if not topic:
            return None

        # Producer dedup check
        existing_id = self._check_dedup(client_message_id)
        if existing_id:
            print(f"[BROKER] Dedup: client_message_id={client_message_id} already published as {existing_id}")
            return {"message_id": existing_id, "offset": None, "deduplicated": True}

        if ttl_seconds is None:
            ttl_seconds = self.config.default_message_ttl

        message = Message(
            content=content,
            headers=headers or {},
            priority=priority,
            ttl_seconds=ttl_seconds,
            max_retries=self.config.max_retry_attempts,
            client_message_id=client_message_id,
        )

        topic.publish_message(message)
        self._register_dedup(client_message_id, message.id)
        return {"message_id": message.id, "offset": message.offset, "deduplicated": False}

    # ── Subscriptions ─────────────────────────────────────────────────────────

    def create_subscriber(self, subscriber_id: str, websocket=None) -> Subscriber:
        with self._lock:
            if subscriber_id in self.subscribers:
                sub = self.subscribers[subscriber_id]
                if websocket:
                    sub.websocket = websocket
                return sub
            subscriber = Subscriber(subscriber_id, websocket)
            self.subscribers[subscriber_id] = subscriber
            self._update_metrics()
            return subscriber

    def create_subscription(
        self, subscriber_id: str, target_type: str, target_name: str
    ) -> Subscription:
        with self._lock:
            subscriber = self.create_subscriber(subscriber_id)

            if target_type != "topic":
                raise ValueError("Only topic subscriptions are supported")

            subscription = Subscription(subscriber_id, target_type, target_name)
            self.subscriptions[subscription.id] = subscription

            if target_type == "topic":
                topic = self.get_topic(target_name)
                if topic:
                    topic.add_subscriber(subscriber, replay=True)

            self._save_meta()
            print(
                f"[BROKER] Subscription created: {subscriber_id} → {target_type}:{target_name}"
            )
            return subscription

    def replay_for_subscriber(
        self, subscriber_id: str, channel_type: str, channel_name: str
    ):
        """Replay missed messages for a subscriber that just reconnected via WS."""
        with self._lock:
            subscriber = self.subscribers.get(subscriber_id)
            if not subscriber:
                return
            if channel_type == "topic":
                topic = self.get_topic(channel_name)
                if topic:
                    topic.add_subscriber(subscriber, replay=True)

    def get_subscriptions(self, subscriber_id: str) -> List[Dict]:
        with self._lock:
            return [
                s.to_dict()
                for s in self.subscriptions.values()
                if s.subscriber_id == subscriber_id
            ]

    # ── ACK ───────────────────────────────────────────────────────────────────

    def ack(
        self,
        subscriber_id: str,
        channel_type: str,
        channel_name: str,
        message_id: str,
    ) -> bool:
        with self._lock:
            if channel_type == "topic":
                topic = self.get_topic(channel_name)
                if topic:
                    topic.ack(subscriber_id, message_id)
                    return True
        return False

    # ── Metrics & info ────────────────────────────────────────────────────────

    def get_metrics(self) -> Dict:
        self._update_metrics()
        return self.metrics.to_dict()

    def get_dlq_info(self) -> Dict:
        if self.dlq:
            return self.dlq.get_info()
        return {"error": "DLQ disabled"}

    def _update_metrics(self):
        self.metrics.update_counts(len(self.topics), len(self.subscribers))

    def shutdown(self):
        print("[BROKER] Shutting down…")
        self._running = False
        self.storage.save_dedup_cache(dict(self._producer_dedup), self._dedup_max)


config = BrokerConfig()
broker = MessageBroker(config)
