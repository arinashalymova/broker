from typing import List, Dict, Any
from collections import deque
from .message import Message, MessageStatus
import threading
from datetime import datetime

class DeadLetterQueue:
    """Dead Letter Queue для сообщений, которые не удалось доставить"""

    def __init__(self, name: str = "dlq", max_size: int = 1000):
        self.name = name
        self.max_size = max_size
        self.messages: deque = deque(maxlen=max_size)
        self._lock = threading.RLock()

    def add_message(self, message: Message, reason: str = "delivery_failed"):
        """Добавить сообщение в DLQ"""
        with self._lock:
            # Обновляем статус и добавляем информацию о причине
            message.status = MessageStatus.DEAD_LETTER
            message.headers["dlq_reason"] = reason
            message.headers["dlq_timestamp"] = datetime.now().isoformat()

            self.messages.append(message)
            print(f"📪 [DLQ] Message {message.id} moved to DLQ. Reason: {reason}")

    def get_messages(self, limit: int = 100) -> List[Message]:
        """Получить сообщения из DLQ"""
        with self._lock:
            return list(self.messages)[-limit:]

    def reprocess_message(self, message_id: str) -> bool:
        """Переместить сообщение из DLQ обратно для обработки"""
        with self._lock:
            for i, message in enumerate(self.messages):
                if message.id == message_id:
                    # Сброс счетчика retry и статуса
                    message.retry_count = 0
                    message.status = MessageStatus.PENDING
                    message.headers.pop("dlq_reason", None)
                    message.headers.pop("dlq_timestamp", None)

                    # Удаляем из DLQ
                    del self.messages[i]
                    print(f"🔄 [DLQ] Message {message_id} reprocessed from DLQ")
                    return True
            return False

    def clear(self):
        """Очистить DLQ"""
        with self._lock:
            count = len(self.messages)
            self.messages.clear()
            print(f"🗑️ [DLQ] Cleared {count} messages from DLQ")

    def get_info(self) -> Dict[str, Any]:
        """Получить информацию о DLQ"""
        with self._lock:
            return {
                "name": self.name,
                "message_count": len(self.messages),
                "max_size": self.max_size,
                "messages": [msg.to_dict() for msg in list(self.messages)[-10:]]  # Последние 10
            }
