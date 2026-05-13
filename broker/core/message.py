from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from enum import Enum
import uuid


class MessagePriority(Enum):
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


class MessageStatus(Enum):
    PENDING = "pending"
    PUBLISHED = "published"
    DELIVERED = "delivered"
    FAILED = "failed"
    EXPIRED = "expired"
    DEAD_LETTER = "dead_letter"


@dataclass
class Message:
    content: Any
    content_type: str = "application/json"
    headers: Dict[str, str] = field(default_factory=dict)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    # client_message_id: supplied by publisher for idempotent publish dedup
    client_message_id: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    topic: Optional[str] = None
    status: MessageStatus = MessageStatus.PENDING
    priority: MessagePriority = MessagePriority.NORMAL
    ttl_seconds: Optional[int] = None
    retry_count: int = 0
    max_retries: int = 3
    last_retry: Optional[datetime] = None
    # offset in the persistent JSONL log (set after writing to storage)
    offset: Optional[int] = None

    def is_expired(self) -> bool:
        if self.ttl_seconds is None:
            return False
        age = (datetime.now() - self.timestamp).total_seconds()
        return age > self.ttl_seconds

    def can_retry(self) -> bool:
        return self.retry_count < self.max_retries

    def mark_retry(self):
        self.retry_count += 1
        self.last_retry = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "client_message_id": self.client_message_id,
            "content": self.content,
            "content_type": self.content_type,
            "headers": self.headers,
            "timestamp": self.timestamp.isoformat(),
            "topic": self.topic,
            "status": self.status.value,
            "priority": self.priority.name,
            "ttl_seconds": self.ttl_seconds,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "last_retry": self.last_retry.isoformat() if self.last_retry else None,
            "expired": self.is_expired(),
            "offset": self.offset,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Message":
        """Reconstruct a Message from a stored dict (e.g. from JSONL)."""
        ts = data.get("timestamp")
        timestamp = datetime.fromisoformat(ts) if ts else datetime.now()

        lr = data.get("last_retry")
        last_retry = datetime.fromisoformat(lr) if lr else None

        priority_name = data.get("priority", "NORMAL")
        try:
            priority = MessagePriority[priority_name]
        except KeyError:
            priority = MessagePriority.NORMAL

        status_val = data.get("status", "pending")
        try:
            status = MessageStatus(status_val)
        except ValueError:
            status = MessageStatus.PENDING

        return cls(
            content=data.get("content"),
            content_type=data.get("content_type", "application/json"),
            headers=data.get("headers", {}),
            id=data.get("id", str(uuid.uuid4())),
            client_message_id=data.get("client_message_id"),
            timestamp=timestamp,
            topic=data.get("topic"),
            status=status,
            priority=priority,
            ttl_seconds=data.get("ttl_seconds"),
            retry_count=data.get("retry_count", 0),
            max_retries=data.get("max_retries", 3),
            last_retry=last_retry,
            offset=data.get("offset"),
        )
