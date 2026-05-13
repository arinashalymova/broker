"""BrokerSubscriber — at-least-once consumer SDK.

Usage::

    sub = BrokerSubscriber("http://localhost:8000", "my-service")
    sub.subscribe("topic", "orders")

    def handle(msg):
        print("Got:", msg["content"])

    sub.consume(handle)   # blocks forever, reconnects automatically
"""

import asyncio
import json
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set


class BrokerSubscriber:
    """WebSocket-based subscriber with auto-reconnect, dedup, and ACK.

    - Sends ``{"action": "subscribe", ...}`` for each subscribed channel on
      every (re)connect, so the broker replays missed messages automatically.
    - After the user callback returns the SDK sends an ACK, removing the
      message from the broker's in-flight tracker.
    - A local ``seen_ids`` set deduplicates messages that arrive twice (e.g.
      after a broker retry before the ACK was received).
    - Multiple ``BrokerSubscriber`` instances with different ``subscriber_id``
      values on the same topic receive independent copies (fan-out).
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        subscriber_id: str = "subscriber",
        seen_ids_max: int = 10_000,
    ):
        self.base_url = base_url.rstrip("/")
        # Derive WebSocket URL from HTTP base
        self.ws_url = self.base_url.replace("http://", "ws://").replace("https://", "wss://")
        # Strip /api/v1 suffix if present — WS endpoint lives at the root app
        if self.ws_url.endswith("/api/v1"):
            self.ws_url = self.ws_url[: -len("/api/v1")]
        self.subscriber_id = subscriber_id
        self._seen_ids_max = seen_ids_max

        self._subscriptions: List[Dict[str, str]] = []
        self._seen_ids: Set[str] = set()
        self._seen_ids_ordered: List[str] = []   # for LRU eviction
        self._callback: Optional[Callable[[Dict], Any]] = None
        self._running = False

    # ── Subscription registration ─────────────────────────────────────────────

    def subscribe(self, channel_type: str, channel_name: str):
        """Register a channel to subscribe to (call before consume)."""
        if channel_type != "topic":
            raise ValueError("Only topic subscriptions are supported")
        self._subscriptions.append(
            {"channel_type": channel_type, "channel_name": channel_name}
        )

    # ── Consume loop ──────────────────────────────────────────────────────────

    def consume(self, callback: Callable[[Dict], Any]):
        """Block forever, calling *callback* for each new message.

        Automatically reconnects on disconnect with exponential back-off
        (1 s → 2 s → 5 s → 5 s …).
        """
        self._callback = callback
        self._running = True
        asyncio.run(self._run_loop())

    def consume_background(self, callback: Callable[[Dict], Any]) -> threading.Thread:
        """Start consume in a background daemon thread. Returns the thread."""
        t = threading.Thread(
            target=self.consume, args=(callback,), daemon=True, name=f"sub-{self.subscriber_id}"
        )
        t.start()
        return t

    def stop(self):
        self._running = False

    # ── Internals ─────────────────────────────────────────────────────────────

    async def _run_loop(self):
        backoff_seq = [1, 2, 5, 5]
        attempt = 0
        while self._running:
            try:
                await self._ws_session()
                attempt = 0  # successful session resets backoff
            except Exception as e:
                if not self._running:
                    break
                delay = backoff_seq[min(attempt, len(backoff_seq) - 1)]
                print(
                    f"[SUB:{self.subscriber_id}] Disconnected ({e}); "
                    f"reconnecting in {delay}s …"
                )
                attempt += 1
                await asyncio.sleep(delay)

    async def _ws_session(self):
        import websockets  # imported lazily so the library is only required when using WS

        ws_endpoint = f"{self.ws_url}/api/v1/ws/{self.subscriber_id}"
        print(f"[SUB:{self.subscriber_id}] Connecting to {ws_endpoint}")

        async with websockets.connect(ws_endpoint, ping_interval=20) as ws:
            print(f"[SUB:{self.subscriber_id}] Connected")

            # Send subscribe frames for all registered channels
            for sub in self._subscriptions:
                await ws.send(json.dumps({"action": "subscribe", **sub}))

            async for raw in ws:
                if not self._running:
                    break
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                # Skip control frames from server
                server_action = data.get("action")
                if server_action in ("subscribed", "pong"):
                    continue

                msg_id = data.get("id")
                if not msg_id:
                    continue

                # Dedup: skip already-processed messages
                if msg_id in self._seen_ids:
                    print(f"[SUB:{self.subscriber_id}] Duplicate {msg_id}, skipping")
                    # Still ACK to clear broker's in-flight entry
                    await self._send_ack(ws, data)
                    continue

                # Process
                try:
                    if self._callback:
                        self._callback(data)
                except Exception as e:
                    print(f"[SUB:{self.subscriber_id}] Callback error for {msg_id}: {e}")
                    # Don't ACK on error — broker will retry after timeout

                    continue

                # ACK after successful processing
                self._mark_seen(msg_id)
                await self._send_ack(ws, data)

    async def _send_ack(self, ws, data: Dict):
        msg_id = data.get("id", "")
        ch_type = "topic"
        ch_name = data.get("topic") or ""
        if not ch_name:
            return
        try:
            await ws.send(
                json.dumps(
                    {
                        "action": "ack",
                        "message_id": msg_id,
                        "channel_type": ch_type,
                        "channel_name": ch_name,
                    }
                )
            )
        except Exception:
            pass

    def _mark_seen(self, msg_id: str):
        if msg_id in self._seen_ids:
            return
        self._seen_ids.add(msg_id)
        self._seen_ids_ordered.append(msg_id)
        if len(self._seen_ids_ordered) > self._seen_ids_max:
            evict = self._seen_ids_ordered.pop(0)
            self._seen_ids.discard(evict)
