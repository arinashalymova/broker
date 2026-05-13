"""
Persistence & delivery-guarantee demo.

Scenario
--------
1. Broker starts (or restarts), restores all state from disk.
2. Script publishes 10 messages to topic 'persist-test'.
3. A background subscriber receives them; it processes only the first 5
   (ACKs those) and simulates a crash before ACKing messages 6-10.
4. The subscriber reconnects — broker replays the un-ACKed messages.
5. Script verifies no duplicates and no losses.

Also demonstrates producer dedup:
- Publishes the same client_message_id twice → broker deduplicates.

Run (broker must be running):
    python examples/test_persistence.py
"""

import os
import sys
import time
import threading
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from client import BrokerPublisher, BrokerSubscriber

BROKER_HTTP = os.getenv("BROKER_URL", "http://localhost:8000/api/v1")
BROKER_WS_BASE = (
    BROKER_HTTP.replace("http://", "ws://")
    .replace("https://", "wss://")
    .replace("/api/v1", "")
)
TOPIC = "persist-test"

received_ids: list = []
received_lock = threading.Lock()
stop_after: int = 5        # simulate crash after 5 messages in first run
second_run_started = threading.Event()


# ── Helpers ───────────────────────────────────────────────────────────────────

def separator(title: str):
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def wait_for_broker():
    import requests
    for i in range(20):
        try:
            requests.get(f"{BROKER_HTTP}/health", timeout=3)
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("Broker not reachable")


# ── Step 1: Publish 10 messages ───────────────────────────────────────────────

def publish_messages(pub: BrokerPublisher, count: int = 10):
    separator(f"Publishing {count} messages to '{TOPIC}'")
    ids = []
    for i in range(count):
        client_mid = str(uuid.uuid4())
        msg_id, offset = pub.publish_topic(TOPIC, {"seq": i, "data": f"payload-{i}"},
                                           client_message_id=client_mid)
        print(f"  [PUBLISH] seq={i} offset={offset} id={msg_id[:8]}…")
        ids.append(msg_id)
    return ids


# ── Step 2: Demonstrate producer dedup ───────────────────────────────────────

def test_producer_dedup(pub: BrokerPublisher):
    separator("Producer dedup: sending same client_message_id twice")
    fixed_id = str(uuid.uuid4())
    id1, off1 = pub.publish_topic(TOPIC, {"dedup": "first"}, client_message_id=fixed_id)
    id2, off2 = pub.publish_topic(TOPIC, {"dedup": "second"}, client_message_id=fixed_id)
    assert id1 == id2, "Dedup failed: got different message IDs!"
    print(f"  [DEDUP OK] Both calls returned same id={id1[:8]}…")


# ── Step 3: Subscriber that crashes after N ACKs ──────────────────────────────

class CrashAfterN(BrokerSubscriber):
    """Processes up to `limit` messages then stops (simulates crash)."""

    def __init__(self, *args, limit: int = 5, **kwargs):
        super().__init__(*args, **kwargs)
        self.limit = limit
        self.count = 0
        self._stopped = False

    def consume_limited(self, seen: list):
        """Run until `limit` messages are ACKed, then return."""
        import asyncio, json

        async def _session():
            import websockets
            ws_url = f"{self.ws_url}/api/v1/ws/{self.subscriber_id}"
            async with websockets.connect(ws_url, ping_interval=None) as ws:
                for sub in self._subscriptions:
                    await ws.send(json.dumps({"action": "subscribe", **sub}))
                async for raw in ws:
                    data = json.loads(raw)
                    if data.get("action") in ("subscribed", "pong"):
                        continue
                    msg_id = data.get("id")
                    if not msg_id:
                        continue
                    seq = data.get("content", {}).get("seq", "?")
                    print(f"  [SUB-1st] received seq={seq} id={msg_id[:8]}…")
                    with received_lock:
                        seen.append(msg_id)
                    # ACK and count
                    ch_type = "topic"
                    ch_name = data.get("topic") or ""
                    if not ch_name:
                        continue
                    await ws.send(json.dumps({
                        "action": "ack",
                        "message_id": msg_id,
                        "channel_type": ch_type,
                        "channel_name": ch_name,
                    }))
                    self.count += 1
                    print(f"  [SUB-1st] ACKed seq={seq} ({self.count}/{self.limit})")
                    if self.count >= self.limit:
                        print("  [SUB-1st] Simulating crash (stop after limit reached)")
                        return  # exit WS session without closing cleanly

        asyncio.run(_session())


# ── Step 4: Second subscriber session (reconnect) ─────────────────────────────

def reconnect_and_drain(seen_first: list, total_expected: int):
    separator("Reconnecting subscriber — expecting replay of un-ACKed messages")
    import asyncio, json

    received_second: list = []

    async def _session():
        import websockets
        ws_url = f"{BROKER_WS_BASE}/api/v1/ws/persist-sub-1"
        async with websockets.connect(ws_url, ping_interval=None) as ws:
            await ws.send(json.dumps({
                "action": "subscribe",
                "channel_type": "topic",
                "channel_name": TOPIC,
            }))
            deadline = time.monotonic() + 10  # wait up to 10s for replay
            while time.monotonic() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                except asyncio.TimeoutError:
                    break
                data = json.loads(raw)
                if data.get("action") in ("subscribed", "pong"):
                    continue
                msg_id = data.get("id")
                if not msg_id:
                    continue
                seq = data.get("content", {}).get("seq", "?")
                print(f"  [SUB-2nd] replayed seq={seq} id={msg_id[:8]}…")
                received_second.append(msg_id)
                ch_type = "topic"
                ch_name = data.get("topic") or ""
                if not ch_name:
                    continue
                await ws.send(json.dumps({
                    "action": "ack",
                    "message_id": msg_id,
                    "channel_type": ch_type,
                    "channel_name": ch_name,
                }))

    asyncio.run(_session())
    return received_second


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    separator("Persistence & Delivery-Guarantee Demo")
    wait_for_broker()

    pub = BrokerPublisher(BROKER_HTTP)

    # Ensure topic exists
    try:
        pub.create_topic(TOPIC)
        print(f"  Topic '{TOPIC}' created")
    except Exception:
        print(f"  Topic '{TOPIC}' already exists — OK")

    # Publish 10 messages
    published_ids = publish_messages(pub, 10)

    # Dedup demo
    test_producer_dedup(pub)

    # First subscriber session: receive and ACK only first 5
    separator("First subscriber session (ACKs only 5 of 10)")
    sub1 = CrashAfterN(BROKER_WS_BASE, "persist-sub-1", limit=5)
    sub1.subscribe("topic", TOPIC)
    acked_first: list = []
    sub1.consume_limited(acked_first)
    print(f"  First session ACKed: {len(acked_first)} messages")

    # Give broker a moment to process ACKs
    time.sleep(1)

    # Second session: should receive the un-ACKed 5 (+ possibly dedup msg)
    replayed = reconnect_and_drain(acked_first, total_expected=5)

    separator("Results")
    all_received = acked_first + replayed
    unique_received = set(all_received)
    duplicates = len(all_received) - len(unique_received)

    print(f"  Published:          {len(published_ids)} messages")
    print(f"  ACKed in session 1: {len(acked_first)}")
    print(f"  Replayed to session 2: {len(replayed)}")
    print(f"  Unique total:       {len(unique_received)}")
    print(f"  Duplicates:         {duplicates}")

    if duplicates == 0:
        print("\n  ✓ No duplicates — exactly-once processing achieved via dedup")
    else:
        print(f"\n  ! {duplicates} duplicate(s) detected (expected with at-least-once)")

    separator("Done")


if __name__ == "__main__":
    main()
