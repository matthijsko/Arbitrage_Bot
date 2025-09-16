import asyncio
from fastapi import FastAPI

app = FastAPI(title="Arbitrage Bot (Phase B Stub)")

@app.on_event("startup")
async def startup():
    # Start a background heartbeat to simulate a worker loop
    async def heartbeat():
        while True:
            await asyncio.sleep(5)
            # Here you'd typically check connections, queues, etc.
    asyncio.create_task(heartbeat())

@app.get("/health")
def health():
    return {"ok": True, "service": "bot", "workers": ["heartbeat"]}
