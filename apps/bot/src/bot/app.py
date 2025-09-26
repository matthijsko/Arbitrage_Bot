import asyncio, contextlib
from fastapi import FastAPI
from .workers.stream import run as run_stream
from .workers.strategy import run as run_strategy
from .execution.paper import run as run_paper

app = FastAPI(title="Arbitrage Bot (Streams + Strategy + PaperExec)")
_tasks = []

@app.on_event("startup")
async def startup():
    global _tasks
    _tasks.append(asyncio.create_task(run_stream()))
    _tasks.append(asyncio.create_task(run_strategy()))
    _tasks.append(asyncio.create_task(run_paper())) 

@app.on_event("shutdown")
async def shutdown():
    global _tasks
    for t in _tasks:
        t.cancel()
    for t in _tasks:
        with contextlib.suppress(Exception):
            await t

@app.get("/health")
def health():
    return {
        "ok": True,
        "service": "bot",
        "tasks": len(_tasks),
        "running": any(not t.done() for t in _tasks),
    }
