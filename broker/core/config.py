from dataclasses import dataclass
from typing import Optional

@dataclass
class BrokerConfig:
    """Конфигурация брокера"""

    # Основные настройки
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = True

    # Настройки сообщений
    default_message_ttl: Optional[int] = None  # секунды
    max_message_size: int = 1024 * 1024  # 1MB
    max_retry_attempts: int = 3
    retry_delay_seconds: int = 5

    # Настройки очистки
    cleanup_interval: int = 60  # 1 минута

    # Настройки DLQ
    enable_dlq: bool = True
    dlq_max_size: int = 1000

    # Настройки производительности
    max_topics: int = 1000
    max_subscribers_per_topic: int = 100

    def to_dict(self):
        """Конвертировать в словарь"""
        return {
            "host": self.host,
            "port": self.port,
            "debug": self.debug,
            "default_message_ttl": self.default_message_ttl,
            "max_message_size": self.max_message_size,
            "max_retry_attempts": self.max_retry_attempts,
            "retry_delay_seconds": self.retry_delay_seconds,
            "cleanup_interval": self.cleanup_interval,
            "enable_dlq": self.enable_dlq,
            "dlq_max_size": self.dlq_max_size,
            "max_topics": self.max_topics,
            "max_subscribers_per_topic": self.max_subscribers_per_topic
        }
