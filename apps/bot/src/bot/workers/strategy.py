import os, asyncio, time
from typing import List
from ..strategy.arbitrage_engine import run_strategy_once

def _env_list(key: str, default: str) -> List[str]:
    return [x.strip() for x in os.getenv(key, default).split(",") if x.strip()]

async def run():
    exchanges = _env_list("STRAT_EXCHANGES", os.getenv("STREAM_EXCHANGES", "bitvavo,coinbase,kraken"))
    symbols = _env_list("STRAT_SYMBOLS", os.getenv("STREAM_SYMBOLS", "BTC/EUR,ETH/EUR"))
    budget_quote = float(os.getenv("STRAT_BUDGET_QUOTE", "250"))
    withdraw_fee_base = float(os.getenv("STRAT_WITHDRAW_FEE_BASE", "0"))
    min_net_quote = float(os.getenv("STRAT_MIN_NET_QUOTE", "0"))
    min_roi_pct = float(os.getenv("STRAT_MIN_ROI_PCT", "0"))
    interval_ms = int(float(os.getenv("STRAT_INTERVAL_MS", "1500")))
    topn = int(os.getenv("STRAT_TOPN", "5"))

    print(f"[strategy] start — ex={exchanges} symbols={symbols} budget={budget_quote} "
          f"minNet={min_net_quote} minRoiPct={min_roi_pct} intervalMs={interval_ms} topN={topn}")

    while True:
        t0 = time.time()
        try:
            res = await run_strategy_once(
                symbols, exchanges, budget_quote, withdraw_fee_base,
                min_net_quote, min_roi_pct, topn
            )

            printed = False
            for block in (res.get("blocks") or []):
                sym = block["symbol"]
                best = block.get("best")
                if best:
                    d = best.get("depth", {}) or {}
                    print(f"[strategy] best {sym}: {best['buy']}→{best['sell']} "
                          f"net={d.get('net_profit_quote',0):.2f} roi={(d.get('roi',0)*100):.2f}% "
                          f"ask={best.get('best_ask')} bid={best.get('best_bid')}")
                    printed = True
                else:
                    # Geen winstgevende match → toon debug_top[0] ongefilterd
                    dbg = (block.get("debug_best_any")
                           or (block.get("debug_top") or [None])[0])
                    if dbg:
                        dd = dbg.get("depth", {}) or {}
                        gross = (dbg.get("gross_spread") or 0.0) * 10000.0
                        print(f"[strategy] no-profit {sym}: {dbg['buy']}→{dbg['sell']} "
                              f"gross={gross:.1f}bps net={dd.get('net_profit_quote',0):.2f} "
                              f"ask={dbg.get('best_ask')} bid={dbg.get('best_bid')}")
                        printed = True

            if not printed:
                print("[strategy] no pairs computed")

        except Exception as e:
            print("[strategy] error:", e)

        dt_ms = int((time.time() - t0) * 1000)
        await asyncio.sleep(max(0, (interval_ms - dt_ms) / 1000))
