import asyncio
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from redis.asyncio import from_url as redis_from_url
from ..config import settings

router = APIRouter()

@router.websocket("/ws/opportunities")
async def websocket_opportunities(ws: WebSocket):
    await ws.accept()
    redis = redis_from_url(settings.redis_url, decode_responses=True)
    pubsub = redis.pubsub()
    await pubsub.subscribe(settings.opp_channel)
    try:
        async for message in pubsub.listen():
            if message is None:
                await asyncio.sleep(0.05)
                continue
            if message.get("type") == "message":
                await ws.send_text(message["data"])
    except WebSocketDisconnect:
        pass
    finally:
        try:
            await pubsub.unsubscribe(settings.opp_channel)
            await pubsub.close()
            await redis.close()
        except Exception:
            pass
