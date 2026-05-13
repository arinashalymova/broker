from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any
import threading
import time

@dataclass
class Metrics:
    """Метрики для мониторинга брокера"""

    # Счетчики сообщений
    messages_published: int = 0
    messages_delivered: int = 0
    messages_failed: int = 0
    messages_expired: int = 0
    messages_in_dlq: int = 0

    # Счетчики по топикам/подписчикам
    topics_count: int = 0
    subscribers_count: int = 0

    # Производительность
    messages_per_second: float = 0.0
    avg_delivery_time_ms: float = 0.0

    # Время запуска
    started_at: datetime = field(default_factory=datetime.now)

    # Приватные поля для расчетов
    _last_messages_count: int = field(default=0, init=False)
    _last_check_time: float = field(default_factory=time.time, init=False)
    _delivery_times: list = field(default_factory=list, init=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False)

    def increment_published(self):
        """Увеличить счетчик опубликованных сообщений"""
        with self._lock:
            self.messages_published += 1
            self._update_performance()

    def increment_delivered(self, delivery_time_ms: float = None):
        """Увеличить счетчик доставленных сообщений"""
        with self._lock:
            self.messages_delivered += 1
            if delivery_time_ms is not None:
                self._delivery_times.append(delivery_time_ms)
                # Храним только последние 100 измерений
                if len(self._delivery_times) > 100:
                    self._delivery_times.pop(0)
                self.avg_delivery_time_ms = sum(self._delivery_times) / len(self._delivery_times)

    def increment_failed(self):
        """Увеличить счетчик неудачных доставок"""
        with self._lock:
            self.messages_failed += 1

    def increment_expired(self):
        """Увеличить счетчик истекших сообщений"""
        with self._lock:
            self.messages_expired += 1

    def increment_dlq(self):
        """Увеличить счетчик сообщений в DLQ"""
        with self._lock:
            self.messages_in_dlq += 1

    def update_counts(self, topics: int, subscribers: int):
        """Обновить счетчики топиков/подписчиков"""
        with self._lock:
            self.topics_count = topics
            self.subscribers_count = subscribers

    def _update_performance(self):
        """Обновить метрики производительности"""
        current_time = time.time()
        if current_time - self._last_check_time >= 1.0:  # Обновляем раз в секунду
            messages_delta = self.messages_published - self._last_messages_count
            time_delta = current_time - self._last_check_time

            self.messages_per_second = messages_delta / time_delta

            self._last_messages_count = self.messages_published
            self._last_check_time = current_time

    def get_uptime_seconds(self) -> float:
        """Получить время работы в секундах"""
        return (datetime.now() - self.started_at).total_seconds()

    def to_dict(self) -> Dict[str, Any]:
        """Конвертировать в словарь для API"""
        with self._lock:
            return {
                "messages": {
                    "published": self.messages_published,
                    "delivered": self.messages_delivered,
                    "failed": self.messages_failed,
                    "expired": self.messages_expired,
                    "in_dlq": self.messages_in_dlq
                },
                "entities": {
                    "topics": self.topics_count,
                    "subscribers": self.subscribers_count
                },
                "performance": {
                    "messages_per_second": round(self.messages_per_second, 2),
                    "avg_delivery_time_ms": round(self.avg_delivery_time_ms, 2),
                    "uptime_seconds": round(self.get_uptime_seconds(), 2)
                },
                "started_at": self.started_at.isoformat()
            }
