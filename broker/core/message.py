from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
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
    timestamp: datetime = field(default_factory=datetime.now)
    topic: Optional[str] = None
    queue: Optional[str] = None
    status: MessageStatus = MessageStatus.PENDING
    priority: MessagePriority = MessagePriority.NORMAL
    ttl_seconds: Optional[int] = None  # Time to live в секундах
    retry_count: int = 0
    max_retries: int = 3
    last_retry: Optional[datetime] = None

    def is_expired(self) -> bool:
        """Проверить истекло ли время жизни сообщения"""
        if self.ttl_seconds is None:
            return False

        age = (datetime.now() - self.timestamp).total_seconds()
        return age > self.ttl_seconds

    def can_retry(self) -> bool:
        """Можно ли повторить доставку"""
        return self.retry_count < self.max_retries

    def mark_retry(self):
        """Отметить попытку повторной доставки"""
        self.retry_count += 1
        self.last_retry = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "content": self.content,
            "content_type": self.content_type,
            "headers": self.headers,
            "timestamp": self.timestamp.isoformat(),
            "topic": self.topic,
            "queue": self.queue,
            "status": self.status.value,
            "priority": self.priority.name,
            "ttl_seconds": self.ttl_seconds,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "last_retry": self.last_retry.isoformat() if self.last_retry else None,
            "expired": self.is_expired()
        }
