from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .config import settings
from .routers.health import router as health_router
from .routers.opportunities import router as opps_router
from .ws.opportunities import router as ws_router
from .routers.arbitrage import router as arb_router
from .routers.markets import router as markets_router
from .routers.markets import router as markets_router
from .routers.diag import router as diag_router
from .routers.arbitrage import router as arb_router

app = FastAPI(title="Arbitrage API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_allow_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router)
app.include_router(opps_router)
app.include_router(ws_router)
app.include_router(arb_router)
app.include_router(markets_router)

app.include_router(markets_router)
app.include_router(diag_router)
app.include_router(arb_router)