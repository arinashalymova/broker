import asyncio
import uuid
from datetime import datetime
from typing import Callable, Optional

from fastapi import WebSocket

from .message import Message


class Subscriber:
    def __init__(self, subscriber_id: str, websocket: Optional[WebSocket] = None):
        self.id = subscriber_id
        self.websocket = websocket
        self.subscriptions = {}
        self.created_at = datetime.now()
        self.last_message_id: Optional[str] = None
        self.message_callback: Optional[Callable] = None
        # Event loop captured when the WebSocket connection is established.
        # Needed so background threads can schedule WS sends safely.
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def set_event_loop(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop

    def set_message_callback(self, callback: Callable[[Message], None]):
        self.message_callback = callback

    def deliver_message(self, message: Message):
        """Deliver a message synchronously (fire-and-forget for WS path)."""
        self.last_message_id = message.id

        if self.websocket and self._loop:
            # Schedule the coroutine on the event loop from any thread
            asyncio.run_coroutine_threadsafe(
                self._send_websocket_message(message), self._loop
            )
        elif self.message_callback:
            self.message_callback(message)
        else:
            print(f"[SUBSCRIBER:{self.id}] {message.id}: {message.content}")

    async def _send_websocket_message(self, message: Message):
        try:
            if self.websocket:
                await self.websocket.send_json(message.to_dict())
        except Exception as e:
            print(f"[SUBSCRIBER:{self.id}] WS send error: {e}")
            self.websocket = None
            self._loop = None


class Subscription:
    def __init__(self, subscriber_id: str, target_type: str, target_name: str):
        self.id = str(uuid.uuid4())
        self.subscriber_id = subscriber_id
        self.target_type = target_type
        self.target_name = target_name
        self.created_at = datetime.now()

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "subscriber_id": self.subscriber_id,
            "target_type": self.target_type,
            "target_name": self.target_name,
            "created_at": self.created_at.isoformat(),
        }
