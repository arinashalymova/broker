from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional
import uuid

@dataclass
class Message:
    content: Any
    content_type: str = "application/json"
    headers: Dict[str, str] = field(default_factory=dict)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=datetime.now)
    topic: Optional[str] = None
    queue: Optional[str] = None
    status: str = "pending"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "content": self.content,
            "content_type": self.content_type,
            "headers": self.headers,
            "timestamp": self.timestamp.isoformat(),
            "topic": self.topic,
            "queue": self.queue,
            "status": self.status
        }
