"""BrokerPublisher — at-least-once producer SDK.

Usage::

    pub = BrokerPublisher("http://localhost:8000/api/v1")
    msg_id, offset = pub.publish("topic", "orders", {"order_id": 1})
"""

import time
import uuid
from typing import Any, Dict, Optional, Tuple

import requests


class BrokerPublisher:
    """Synchronous publisher with automatic retry and idempotency.

    If the broker does not respond within `request_timeout` seconds the call
    is retried up to `max_retries` times with exponential back-off.  A
    ``client_message_id`` is generated once per publish call so that even if
    the message reaches the broker but the HTTP response is lost, the second
    attempt will be deduplicated and you still receive the correct
    ``message_id`` and ``offset``.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8000/api/v1",
        request_timeout: float = 5.0,
        max_retries: int = 3,
    ):
        self.base_url = base_url.rstrip("/")
        self.request_timeout = request_timeout
        self.max_retries = max_retries

    # ── Public API ────────────────────────────────────────────────────────────

    def publish(
        self,
        channel_type: str,
        channel_name: str,
        content: Any,
        *,
        headers: Optional[Dict[str, str]] = None,
        priority: str = "NORMAL",
        ttl_seconds: Optional[int] = None,
        client_message_id: Optional[str] = None,
    ) -> Tuple[str, Optional[int]]:
        """Publish a message; returns ``(message_id, offset)``.

        The ``client_message_id`` is the idempotency key.  If not supplied a
        UUID is generated automatically so every ``publish()`` call is safe
        to retry.

        Raises ``RuntimeError`` if all retries are exhausted.
        """
        if client_message_id is None:
            client_message_id = str(uuid.uuid4())

        payload = {
            "content": content,
            "headers": headers or {},
            "priority": priority,
            "ttl_seconds": ttl_seconds,
            "client_message_id": client_message_id,
        }

        if channel_type != "topic":
            raise ValueError("Only topic publishing is supported")
        url = f"{self.base_url}/topics/{channel_name}/publish"

        delay = 0.5
        last_exc: Optional[Exception] = None

        for attempt in range(1, self.max_retries + 2):  # +1 for initial attempt
            try:
                resp = requests.post(url, json=payload, timeout=self.request_timeout)
                resp.raise_for_status()
                data = resp.json()
                msg_id = data["message_id"]
                offset = data.get("offset")
                deduped = data.get("deduplicated", False)
                print(
                    f"[PUB] Sent to {channel_type}:{channel_name} "
                    f"id={msg_id} offset={offset}"
                    + (" (deduplicated)" if deduped else "")
                )
                return msg_id, offset

            except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
                last_exc = exc
                if attempt <= self.max_retries:
                    print(
                        f"[PUB] Attempt {attempt} failed ({exc}); "
                        f"retrying in {delay:.1f}s …"
                    )
                    time.sleep(delay)
                    delay = min(delay * 2, 10.0)

        raise RuntimeError(
            f"Failed to publish after {self.max_retries + 1} attempts: {last_exc}"
        )

    # ── Convenience helpers ───────────────────────────────────────────────────

    def publish_topic(self, topic_name: str, content: Any, **kwargs) -> Tuple[str, Optional[int]]:
        return self.publish("topic", topic_name, content, **kwargs)

    def create_topic(self, name: str) -> Dict:
        resp = requests.post(f"{self.base_url}/topics", json={"name": name}, timeout=5)
        return resp.json()

    def health(self) -> Dict:
        resp = requests.get(f"{self.base_url}/health", timeout=5)
        return resp.json()
