import asyncio
import threading
import time
from collections import deque
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from .message import Message, MessageStatus, MessagePriority
from ..persistence.storage import JsonlLogStorage


class _InFlight:
    """Tracks a message sent to a subscriber but not yet ACKed."""

    __slots__ = ("msg", "sent_at", "retry_count")

    def __init__(self, msg: Message):
        self.msg = msg
        self.sent_at = time.monotonic()
        self.retry_count = 0


class Topic:
    ACK_TIMEOUT = 3.0  # seconds: re-send if no ACK within this window

    def __init__(
        self,
        name: str,
        storage: JsonlLogStorage,
        config=None,
        metrics=None,
        dlq=None,
    ):
        self.name = name
        self.storage = storage
        self.config = config
        self.metrics = metrics
        self.dlq = dlq

        self.subscribers: Dict[str, "Subscriber"] = {}
        # in-memory message cache (rebuilt from disk on restore)
        self.messages: deque = deque()
        self.created_at = datetime.now()
        self._lock = threading.RLock()

        # in_flight[(subscriber_id, message_id)] = _InFlight
        self._in_flight: Dict[Tuple[str, str], _InFlight] = {}
        # per-subscriber confirmed offset (highest ACKed offset)
        self._sub_offsets: Dict[str, int] = {}

    # ── Subscriber management ─────────────────────────────────────────────────

    def add_subscriber(self, subscriber: "Subscriber", replay: bool = True):
        """Add subscriber and replay any unread messages from their saved offset."""
        with self._lock:
            self.subscribers[subscriber.id] = subscriber
            print(f"[TOPIC:{self.name}] Subscriber {subscriber.id} added")

            if not replay:
                return

            saved_offset = self.storage.load_offset(subscriber.id, "topic", self.name)
            next_offset = saved_offset + 1  # deliver everything after last ACKed

            pending = [m for m in self.messages if m.offset is not None and m.offset >= next_offset]
            if pending:
                print(
                    f"[TOPIC:{self.name}] Replaying {len(pending)} missed messages "
                    f"to {subscriber.id} (from offset {next_offset})"
                )
                for msg in pending:
                    if not msg.is_expired():
                        self._deliver_one(subscriber, msg)

    def remove_subscriber(self, subscriber_id: str):
        with self._lock:
            self.subscribers.pop(subscriber_id, None)
            print(f"[TOPIC:{self.name}] Subscriber {subscriber_id} removed")

    # ── Publish ───────────────────────────────────────────────────────────────

    def publish_message(self, message: Message):
        """Persist message and fan-out to all current subscribers."""
        with self._lock:
            message.topic = self.name
            message.status = MessageStatus.PUBLISHED

            if message.is_expired():
                message.status = MessageStatus.EXPIRED
                if self.metrics:
                    self.metrics.increment_expired()
                print(f"[TOPIC:{self.name}] Message {message.id} expired on arrival")
                return

            # Priority insertion into in-memory deque
            if message.priority in (MessagePriority.HIGH, MessagePriority.CRITICAL):
                inserted = False
                for i, existing in enumerate(self.messages):
                    if existing.priority.value < message.priority.value:
                        self.messages.insert(i, message)
                        inserted = True
                        break
                if not inserted:
                    self.messages.append(message)
            else:
                self.messages.append(message)

            # Persist to JSONL log (sets message.offset)
            offset = self.storage.append("topic", self.name, message.to_dict())
            message.offset = offset

            if self.metrics:
                self.metrics.increment_published()

            print(f"[TOPIC:{self.name}] Message {message.id} stored at offset {offset}")

            self._deliver_to_all(message)

    def _deliver_to_all(self, message: Message):
        """Send message to every subscriber and register in-flight entries."""
        for subscriber in list(self.subscribers.values()):
            self._deliver_one(subscriber, message)

    def _deliver_one(self, subscriber: "Subscriber", message: Message):
        key = (subscriber.id, message.id)
        if key not in self._in_flight:
            self._in_flight[key] = _InFlight(message)
        try:
            start = time.time()
            subscriber.deliver_message(message)
            delivery_ms = (time.time() - start) * 1000
            if self.metrics:
                self.metrics.increment_delivered(delivery_ms)
        except Exception as e:
            print(f"[TOPIC:{self.name}] Delivery error to {subscriber.id}: {e}")
            if self.metrics:
                self.metrics.increment_failed()

    # ── ACK ───────────────────────────────────────────────────────────────────

    def ack(self, subscriber_id: str, message_id: str):
        """Acknowledge a message from a subscriber."""
        with self._lock:
            key = (subscriber_id, message_id)
            entry = self._in_flight.pop(key, None)
            if entry is None:
                return  # already acked or unknown

            msg = entry.msg
            if msg.offset is not None:
                prev = self._sub_offsets.get(subscriber_id, -1)
                new_offset = max(prev, msg.offset)
                self._sub_offsets[subscriber_id] = new_offset
                self.storage.save_offset(subscriber_id, "topic", self.name, new_offset)
                print(
                    f"[TOPIC:{self.name}] ACK from {subscriber_id} for msg {message_id} "
                    f"(offset {new_offset})"
                )

    # ── Retry loop (called by broker's background thread) ─────────────────────

    def check_inflight_timeouts(self):
        """Called periodically by the broker. Re-sends timed-out in-flight messages."""
        with self._lock:
            now = time.monotonic()
            to_retry = [
                (key, entry)
                for key, entry in list(self._in_flight.items())
                if now - entry.sent_at > self.ACK_TIMEOUT
            ]
            for (sub_id, msg_id), entry in to_retry:
                subscriber = self.subscribers.get(sub_id)
                msg = entry.msg

                if subscriber is None or msg.is_expired():
                    del self._in_flight[(sub_id, msg_id)]
                    continue

                if entry.retry_count >= (self.config.max_retry_attempts if self.config else 3):
                    print(
                        f"[TOPIC:{self.name}] Max retries for {msg_id} to {sub_id}; "
                        "moving to DLQ"
                    )
                    del self._in_flight[(sub_id, msg_id)]
                    if self.dlq:
                        self.dlq.add_message(msg, f"ack_timeout_{sub_id}")
                    if self.metrics:
                        self.metrics.increment_dlq()
                    continue

                entry.retry_count += 1
                entry.sent_at = now
                print(
                    f"[TOPIC:{self.name}] Retrying msg {msg_id} to {sub_id} "
                    f"(attempt {entry.retry_count})"
                )
                try:
                    subscriber.deliver_message(msg)
                except Exception as e:
                    print(f"[TOPIC:{self.name}] Retry delivery error: {e}")

    # ── TTL cleanup ───────────────────────────────────────────────────────────

    def cleanup_expired_messages(self):
        with self._lock:
            expired = [m for m in self.messages if m.is_expired()]
            for m in expired:
                self.messages.remove(m)
                m.status = MessageStatus.EXPIRED
            if expired and self.metrics:
                for _ in expired:
                    self.metrics.increment_expired()
            if expired:
                print(f"[TOPIC:{self.name}] Removed {len(expired)} expired messages")

    # ── Info ──────────────────────────────────────────────────────────────────

    def get_info(self) -> Dict:
        with self._lock:
            return {
                "name": self.name,
                "subscribers": list(self.subscribers.keys()),
                "message_count": len(self.messages),
                "in_flight_count": len(self._in_flight),
                "created_at": self.created_at.isoformat(),
            }

    def load_from_storage(self):
        """Load persisted messages from JSONL into in-memory deque (called on restore)."""
        from .message import Message as Msg
        msgs = self.storage.load_all_messages("topic", self.name)
        for d in msgs:
            msg = Msg.from_dict(d)
            if not msg.is_expired():
                self.messages.append(msg)
        print(f"[TOPIC:{self.name}] Loaded {len(self.messages)} messages from disk")
