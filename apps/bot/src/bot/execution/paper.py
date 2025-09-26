import os
import asyncio
import time
import hashlib
from typing import Any, Dict, List, Optional

import orjson
from redis.asyncio import from_url as redis_from_url

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

# Luister naar dezelfde channel als de strategy-publicatie
EXECUTE_CHANNEL = os.getenv("PUBLISH_CHANNEL", "opps")
# Stream waarin we “fills” bewaren
PAPER_STREAM = os.getenv("PAPER_STREAM", "paper_trades")

# Uitvoeringsparameters
PAPER_MIN_NET_QUOTE = float(os.getenv("PAPER_MIN_NET_QUOTE", os.getenv("STRAT_MIN_NET_QUOTE", "0")))
PAPER_MIN_ROI_PCT   = float(os.getenv("PAPER_MIN_ROI_PCT",  os.getenv("STRAT_MIN_ROI_PCT",  "0")))
PAPER_SLIPPAGE_BPS  = float(os.getenv("PAPER_SLIPPAGE_BPS", "2"))  # 2 bps default
PAPER_DEDUP_COOLDOWN_MS = int(float(os.getenv("PAPER_DEDUP_COOLDOWN_MS", "4000")))  # 4s

ALLOW_NO_PROFIT = os.getenv("ALLOW_NO_PROFIT", "1") not in ("0", "false", "False")

def _now_ms() -> int:
    return int(time.time() * 1000)

def _hash_item(item: Dict[str, Any]) -> str:
    d = item.get("depth") or {}
    parts = [
        str(item.get("symbol") or ""),
        str(item.get("buy") or ""),
        str(item.get("sell") or ""),
        str(round(float(item.get("best_ask") or 0.0), 2)),
        str(round(float(item.get("best_bid") or 0.0), 2)),
        str(round(float(d.get("qty_base_sold") or d.get("qty_base_bought") or 0.0), 8)),
    ]
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()

def _bps(x: float) -> float:
    return (x or 0.0) * 10000.0

async def _should_execute(r, item: Dict[str, Any]) -> bool:
    """Filter: ok/net/roi en de-dup. In dev kan ALLOW_NO_PROFIT ook 'ok=0' doorlaten."""
    d = item.get("depth") or {}
    qty = float(d.get("qty_base_sold") or d.get("qty_base_bought") or 0.0)
    if qty <= 0:
        return False

    ok = bool(item.get("ok"))
    net = float(d.get("net_profit_quote") or 0.0)
    roi = float(d.get("roi") or 0.0) * 100.0

    passes_thresholds = (net >= PAPER_MIN_NET_QUOTE and roi >= PAPER_MIN_ROI_PCT)
    if ok and passes_thresholds:
        pass  # reguliere case
    elif ALLOW_NO_PROFIT:
        # laat in dev alles door dat *zinvolle* qty heeft; thresholds negeren
        pass
    else:
        return False

    # Dedup korte termijn
    h = _hash_item(item)
    key = f"paper:dedup:{h}"
    exists = await r.set(key, b"1", nx=True, px=PAPER_DEDUP_COOLDOWN_MS)
    return bool(exists)


def _paper_fill(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Bouw een gesimuleerde fill met slippage en fees (alle input komt uit item/depth)."""
    symbol = item.get("symbol")
    buy_ex = item.get("buy")
    sell_ex = item.get("sell")
    best_ask = float(item.get("best_ask") or 0.0)
    best_bid = float(item.get("best_bid") or 0.0)
    d = item.get("depth") or {}

    fee_buy = float(d.get("buy_fee_quote") or 0.0)  
    
    fee_buy_rate = float(item.get("fee_buy") or 0.001)
    fee_sell_rate = float(item.get("fee_sell") or 0.001)

    qty = float(d.get("qty_base_sold") or d.get("qty_base_bought") or 0.0)
    if qty <= 0 or best_ask <= 0 or best_bid <= 0:
        return None

    # Slippage toepassen op top-of-book
    slip = PAPER_SLIPPAGE_BPS / 10000.0
    eff_ask = best_ask * (1.0 + slip)  # koop iets slechter
    eff_bid = best_bid * (1.0 - slip)  # verkoop iets slechter

    spent = qty * eff_ask * (1.0 + fee_buy_rate)
    received = qty * eff_bid * (1.0 - fee_sell_rate)
    net = received - spent
    roi = (net / spent) if spent > 0 else 0.0
    gross_bps = _bps((best_bid - best_ask) / best_ask)

    return {
        "ts": _now_ms(),
        "symbol": symbol,
        "buy": buy_ex,
        "sell": sell_ex,
        "qty_base": qty,
        "best_ask": best_ask,
        "best_bid": best_bid,
        "eff_ask": eff_ask,
        "eff_bid": eff_bid,
        "fee_buy_rate": fee_buy_rate,
        "fee_sell_rate": fee_sell_rate,
        "slippage_bps": PAPER_SLIPPAGE_BPS,
        "spent_quote": spent,
        "received_quote": received,
        "net_profit_quote": net,
        "roi": roi,
        "gross_spread_bps": gross_bps,
        "source": "paper-exec",
    }

async def run():
    """Subscribet op EXECUTE_CHANNEL en schrijft fills naar PAPER_STREAM."""
    while True:
        r = redis_from_url(REDIS_URL, decode_responses=False)
        pub = r.pubsub(ignore_subscribe_messages=True)
        try:
            await pub.subscribe(EXECUTE_CHANNEL)
            print(f"[paper] listening on channel '{EXECUTE_CHANNEL}', stream '{PAPER_STREAM}', "
                  f"minNet={PAPER_MIN_NET_QUOTE}, minRoiPct={PAPER_MIN_ROI_PCT}, slip={PAPER_SLIPPAGE_BPS}bps")

            async for msg in pub.listen():
                try:
                    if msg.get("type") != "message":
                        continue
                    raw = msg.get("data")
                    if not raw:
                        continue
                    payload = orjson.loads(raw)
                    items: List[Dict[str, Any]] = payload.get("items") or []
                    for it in items:
                        if not await _should_execute(r, it):
                            continue
                        trade = _paper_fill(it)
                        if not trade:
                            continue
                        # Log naar stream
                        await r.xadd(PAPER_STREAM, {"payload": orjson.dumps(trade)}, maxlen=5000, approximate=True)
                        # Console
                        print(f"[paper] {trade['symbol']} {trade['buy']}→{trade['sell']} "
                              f"qty={trade['qty_base']:.6f} net={trade['net_profit_quote']:.2f} "
                              f"roi={trade['roi']*100:.2f}% slip={trade['slippage_bps']}bps")
                except Exception as e:
                    print("[paper] handle message error:", e)
        except Exception as e:
            print("[paper] subscribe error, retrying soon:", e)
        finally:
            try:
                await pub.unsubscribe(EXECUTE_CHANNEL)
            except Exception:
                pass
            await r.close()
        await asyncio.sleep(1.0)  # back-off bij reconnect
