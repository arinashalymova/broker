"""
Subscriber A microservice demo.

Subscribes to the 'orders' topic and processes each order.
Demonstrates:
  - multiple consumers on one topic (fan-out / broadcast)
  - at-least-once delivery with ACK
  - automatic reconnect after broker restart

Run:
    python -m examples.microservices.subscriber_service_a
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
SUBSCRIBER_ID = os.getenv("SUBSCRIBER_ID", "subscriber-a")


def process_order(msg: dict):
    content = msg.get("content", {})
    msg_id = msg.get("id", "?")
    offset = msg.get("offset", "?")
    received_at = datetime.now().strftime("%H:%M:%S")

    print(
        f"[SUB-A] ← received @ {received_at} | "
        f"offset={offset} id={msg_id[:8]}… | "
        f"order_id={content.get('order_id','?')[:8]}… "
        f"seq=#{content.get('seq','?')} "
        f"product={content.get('product','?')}"
    )
    # Simulate processing time (short, so ACK arrives quickly)
    time.sleep(0.1)
    print(f"[SUB-A] ✓ processed order #{content.get('seq','?')}")


def wait_for_broker():
    import requests
    for i in range(30):
        try:
            requests.get(f"{BROKER_URL}/api/v1/health", timeout=3)
            print(f"[SUB-A] Broker ready")
            return
        except Exception:
            print(f"[SUB-A] Waiting for broker … ({i+1}/30)")
            time.sleep(1)
    raise RuntimeError("Broker not available")


def main():
    wait_for_broker()
    print(f"[SUB-A] Connecting as subscriber '{SUBSCRIBER_ID}'")

    sub = BrokerSubscriber(BROKER_URL, SUBSCRIBER_ID)
    sub.subscribe("topic", TOPIC)

    print(f"[SUB-A] Subscribed to topic '{TOPIC}'. Waiting for orders …")
    sub.consume(process_order)   # blocks forever, reconnects automatically


if __name__ == "__main__":
    main()
