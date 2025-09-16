from fastapi import FastAPI

app = FastAPI(title="Arbitrage API (Phase B Stub)")

@app.get("/health")
def health():
    return {"ok": True, "service": "api"}
