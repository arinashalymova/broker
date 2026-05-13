import asyncio
import json
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from broker.core.broker import broker
from broker.core.message import MessagePriority

app = FastAPI(
    title="Message Broker API",
    description=(
        "Брокер сообщений с pub/sub, персистентностью, "
        "гарантией доставки (at-least-once) и дедупликацией."
    ),
    version="2.0.0",
)


# ── Pydantic models ───────────────────────────────────────────────────────────

class Priority(str, Enum):
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


PRIORITY_MAP = {
    Priority.LOW: MessagePriority.LOW,
    Priority.NORMAL: MessagePriority.NORMAL,
    Priority.HIGH: MessagePriority.HIGH,
    Priority.CRITICAL: MessagePriority.CRITICAL,
}


class MessageBody(BaseModel):
    content: Any
    content_type: str = "application/json"
    headers: Dict[str, str] = Field(default_factory=dict)
    priority: Priority = Priority.NORMAL
    ttl_seconds: Optional[int] = None
    # Idempotency key from producer (optional)
    client_message_id: Optional[str] = None


class TopicCreate(BaseModel):
    name: str


class TopicInfo(BaseModel):
    name: str
    subscribers: List[str]
    message_count: int
    created_at: str


class SubscriptionCreate(BaseModel):
    subscriber_id: str
    target_type: str
    target_name: str


class AckRequest(BaseModel):
    subscriber_id: str
    channel_type: str   # "topic"
    channel_name: str
    message_id: str


# ── Topics ────────────────────────────────────────────────────────────────────

@app.post("/topics", response_model=TopicInfo, tags=["Topics"])
async def create_topic(body: TopicCreate):
    try:
        topic = broker.create_topic(body.name)
        return TopicInfo(**topic.get_info())
    except ValueError as e:
        raise HTTPException(400, str(e))


@app.get("/topics", response_model=List[TopicInfo], tags=["Topics"])
async def get_topics():
    return [TopicInfo(**t) for t in broker.get_all_topics()]


@app.get("/topics/{topic_name}", response_model=TopicInfo, tags=["Topics"])
async def get_topic(topic_name: str):
    topic = broker.get_topic(topic_name)
    if not topic:
        raise HTTPException(404, "Topic not found")
    return TopicInfo(**topic.get_info())


@app.delete("/topics/{topic_name}", tags=["Topics"])
async def delete_topic(topic_name: str):
    if not broker.delete_topic(topic_name):
        raise HTTPException(404, "Topic not found")
    return {"status": "deleted", "topic": topic_name}


# ── Publish to topic ──────────────────────────────────────────────────────────

@app.post("/topics/{topic_name}/publish", tags=["Publishing"])
async def publish_to_topic(topic_name: str, body: MessageBody):
    """
    Publish a message to a topic.

    - `client_message_id` (optional): idempotency key — if the same key is
      sent again within the dedup window the broker returns the original
      message_id without storing a duplicate.
    - Response includes `message_id` and `offset` for at-least-once tracking.
    """
    topic = broker.get_topic(topic_name)
    if not topic:
        raise HTTPException(404, "Topic not found")

    result = broker.publish_to_topic(
        topic_name,
        body.content,
        priority=PRIORITY_MAP[body.priority],
        ttl_seconds=body.ttl_seconds,
        headers=body.headers,
        client_message_id=body.client_message_id,
    )
    if result is None:
        raise HTTPException(500, "Publish failed")

    return {
        "status": "published",
        "topic": topic_name,
        "message_id": result["message_id"],
        "offset": result["offset"],
        "deduplicated": result.get("deduplicated", False),
        "timestamp": datetime.now().isoformat(),
    }


# ── Subscriptions ─────────────────────────────────────────────────────────────

@app.post("/subscriptions", tags=["Subscriptions"])
async def create_subscription(body: SubscriptionCreate):
    if body.target_type != "topic":
        raise HTTPException(400, "target_type must be 'topic'")
    if not broker.get_topic(body.target_name):
        raise HTTPException(404, "Topic not found")

    sub = broker.create_subscription(
        body.subscriber_id, body.target_type, body.target_name
    )
    return sub.to_dict()


@app.get("/subscriptions/{subscriber_id}", tags=["Subscriptions"])
async def get_subscriptions(subscriber_id: str):
    return broker.get_subscriptions(subscriber_id)


# ── ACK endpoint ──────────────────────────────────────────────────────────────

@app.post("/ack", tags=["Delivery"])
async def acknowledge_message(body: AckRequest):
    """
    Acknowledge delivery of a message.

    Call this after successfully processing a message received via REST poll.
    WebSocket subscribers send ACK directly over the socket.
    """
    ok = broker.ack(
        body.subscriber_id, body.channel_type, body.channel_name, body.message_id
    )
    if not ok:
        raise HTTPException(404, "Channel not found")
    return {"status": "acked", "message_id": body.message_id}


# ── Monitoring ────────────────────────────────────────────────────────────────

@app.get("/health", tags=["Monitoring"])
async def health():
    m = broker.get_metrics()
    return {
        "status": "healthy",
        "uptime_seconds": m["performance"]["uptime_seconds"],
        "topics": m["entities"]["topics"],
        "subscribers": m["entities"]["subscribers"],
        "version": "2.0.0",
    }


@app.get("/metrics", tags=["Monitoring"])
async def get_metrics():
    return broker.get_metrics()


@app.get("/config", tags=["Monitoring"])
async def get_config():
    return broker.config.to_dict()


# ── Dead Letter Queue ─────────────────────────────────────────────────────────

@app.get("/dlq", tags=["Dead Letter Queue"])
async def get_dlq():
    return broker.get_dlq_info()


@app.post("/dlq/{message_id}/reprocess", tags=["Dead Letter Queue"])
async def reprocess_dlq(message_id: str):
    if broker.dlq and broker.dlq.reprocess_message(message_id):
        return {"status": "reprocessed", "message_id": message_id}
    raise HTTPException(404, "Message not found in DLQ")


@app.delete("/dlq", tags=["Dead Letter Queue"])
async def clear_dlq():
    if broker.dlq:
        broker.dlq.clear()
        return {"status": "cleared"}
    raise HTTPException(404, "DLQ not available")


# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws/{subscriber_id}")
async def websocket_endpoint(websocket: WebSocket, subscriber_id: str):
    """
    WebSocket protocol:

    CLIENT → SERVER:
      {"action": "subscribe", "channel_type": "topic", "channel_name": "<name>"}
      {"action": "ack", "message_id": "<id>", "channel_type": "topic", "channel_name": "<name>"}
      {"action": "ping"}

    SERVER → CLIENT:
      message dict (same as to_dict()) with fields: id, offset, topic, content, …
    """
    await websocket.accept()

    loop = asyncio.get_event_loop()
    subscriber = broker.create_subscriber(subscriber_id, websocket)
    subscriber.set_event_loop(loop)
    print(f"[WS] Connected: {subscriber_id}")

    # Track which channels this WS session subscribed to (for ACK routing)
    ws_channels: Dict[str, Dict] = {}  # message_id → {channel_type, channel_name}

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            action = msg.get("action")

            if action == "subscribe":
                ch_type = msg.get("channel_type", "topic")
                ch_name = msg.get("channel_name", "")
                if not ch_name or ch_type != "topic":
                    continue
                broker.create_subscription(subscriber_id, ch_type, ch_name)
                # Replay happens inside create_subscription → add_subscriber
                await websocket.send_json(
                    {"action": "subscribed", "channel_type": ch_type, "channel_name": ch_name}
                )
                print(f"[WS] {subscriber_id} subscribed to {ch_type}:{ch_name}")

            elif action == "ack":
                message_id = msg.get("message_id", "")
                ch_type = msg.get("channel_type", "topic")
                ch_name = msg.get("channel_name", "")
                if message_id and ch_name and ch_type == "topic":
                    broker.ack(subscriber_id, ch_type, ch_name, message_id)

            elif action == "ping":
                await websocket.send_json({"action": "pong"})

    except WebSocketDisconnect:
        print(f"[WS] Disconnected: {subscriber_id}")
    except Exception as e:
        print(f"[WS] Error for {subscriber_id}: {e}")
    finally:
        if subscriber_id in broker.subscribers:
            broker.subscribers[subscriber_id].websocket = None
            broker.subscribers[subscriber_id]._loop = None
