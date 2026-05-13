"""
Subscriber B microservice demo.

Second independent subscriber on the same 'orders' topic.
Both A and B receive every message (pub/sub broadcast semantics).
Each has its own offset, so each gets a full independent copy.

Run:
    python -m examples.microservices.subscriber_service_b
    # or inside Docker — started by docker-compose
"""

import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from client import BrokerSubscriber

BROKER_URL = os.getenv("BROKER_URL", "http://localhost:8000")
TOPIC = "orders"
SUBSCRIBER_ID = os.getenv("SUBSCRIBER_ID", "subscriber-b")


def audit_order(msg: dict):
    """Subscriber B performs an 'audit' — different processing from A."""
    content = msg.get("content", {})
    msg_id = msg.get("id", "?")
    offset = msg.get("offset", "?")
    received_at = datetime.now().strftime("%H:%M:%S")

    print(
        f"[SUB-B] ← received @ {received_at} | "
        f"offset={offset} id={msg_id[:8]}… | "
        f"auditing order #{content.get('seq','?')} "
        f"qty={content.get('quantity','?')}"
    )
    time.sleep(0.05)
    print(f"[SUB-B] ✓ audit complete for order #{content.get('seq','?')}")


def wait_for_broker():
    import requests
    for i in range(30):
        try:
            requests.get(f"{BROKER_URL}/api/v1/health", timeout=3)
            print(f"[SUB-B] Broker ready")
            return
        except Exception:
            print(f"[SUB-B] Waiting for broker … ({i+1}/30)")
            time.sleep(1)
    raise RuntimeError("Broker not available")


def main():
    wait_for_broker()
    print(f"[SUB-B] Connecting as subscriber '{SUBSCRIBER_ID}'")

    sub = BrokerSubscriber(BROKER_URL, SUBSCRIBER_ID)
    sub.subscribe("topic", TOPIC)

    print(f"[SUB-B] Subscribed to topic '{TOPIC}'. Waiting for orders …")
    sub.consume(audit_order)


if __name__ == "__main__":
    main()
