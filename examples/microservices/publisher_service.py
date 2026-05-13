"""
Publisher microservice demo.

Publishes an "order placed" event to the 'orders' topic every 2 seconds.
Demonstrates:
  - at-least-once delivery (auto-retry on timeout)
  - idempotency via client_message_id
  - graceful startup wait for the broker

Run:
    python -m examples.microservices.publisher_service
    # or inside Docker — started by docker-compose
"""

import os
import sys
import time
import uuid
from datetime import datetime

# Allow running from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from client import BrokerPublisher

BROKER_URL = os.getenv("BROKER_URL", "http://localhost:8000/api/v1")
TOPIC = "orders"
PUBLISH_INTERVAL = float(os.getenv("PUBLISH_INTERVAL", "2"))


def wait_for_broker(pub: BrokerPublisher, retries: int = 30):
    for i in range(retries):
        try:
            pub.health()
            print(f"[PUB-SVC] Broker ready at {BROKER_URL}")
            return
        except Exception:
            print(f"[PUB-SVC] Waiting for broker … ({i+1}/{retries})")
            time.sleep(1)
    raise RuntimeError("Broker not available")


def ensure_topic(pub: BrokerPublisher):
    try:
        result = pub.create_topic(TOPIC)
        if "name" in result:
            print(f"[PUB-SVC] Topic '{TOPIC}' created")
        else:
            print(f"[PUB-SVC] Topic '{TOPIC}': {result.get('detail', 'already exists')}")
    except Exception as e:
        print(f"[PUB-SVC] Topic setup error: {e}")


def main():
    pub = BrokerPublisher(BROKER_URL, request_timeout=5.0, max_retries=3)
    wait_for_broker(pub)
    ensure_topic(pub)

    order_seq = 1
    print(f"[PUB-SVC] Publishing to topic '{TOPIC}' every {PUBLISH_INTERVAL}s")

    while True:
        order_id = str(uuid.uuid4())
        payload = {
            "order_id": order_id,
            "seq": order_seq,
            "product": f"item-{(order_seq % 5) + 1}",
            "quantity": order_seq % 10 + 1,
            "created_at": datetime.now().isoformat(),
        }

        try:
            msg_id, offset = pub.publish_topic(TOPIC, payload)
            print(
                f"[PUB-SVC] ✓ order #{order_seq} sent | "
                f"msg_id={msg_id[:8]}… offset={offset}"
            )
        except RuntimeError as e:
            print(f"[PUB-SVC] ✗ Failed to publish order #{order_seq}: {e}")

        order_seq += 1
        time.sleep(PUBLISH_INTERVAL)


if __name__ == "__main__":
    main()
