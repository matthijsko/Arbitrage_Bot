import asyncio, contextlib
from fastapi import FastAPI
from .workers.stream_worker import run as run_stream

app = FastAPI(title="Arbitrage Bot (Streams)")
_task = None

@app.on_event("startup")
async def startup():
    global _task
    _task = asyncio.create_task(run_stream())

@app.on_event("shutdown")
async def shutdown():
    global _task
    if _task:
        _task.cancel()
        with contextlib.suppress(Exception):
            await _task

@app.get("/health")
def health():
    return {"ok": True, "service": "bot", "streaming": bool(_task)}
