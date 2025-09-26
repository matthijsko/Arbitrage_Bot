import os, asyncio, time
from typing import List
from ..strategy.arbitrage_engine import run_strategy_once

def _env_list(key: str, default: str) -> List[str]:
    return [x.strip() for x in os.getenv(key, default).split(",") if x.strip()]

PRINT_TOPN = int(os.getenv("PRINT_TOPN", "3"))

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
          f"minNet={min_net_quote} minRoiPct={min_roi_pct} intervalMs={interval_ms} topN={topn}, PRINT_TOPN={PRINT_TOPN}")

    while True:
        t0 = time.time()
        try:
            res = await run_strategy_once(
                symbols, exchanges, budget_quote, withdraw_fee_base,
                min_net_quote, min_roi_pct, topn
            )

            for block in (res.get("blocks") or []):
                sym = block["symbol"]

                # Beste na filters
                best = block.get("best")
                if best:
                    d = best.get("depth", {}) or {}
                    print(f"[strategy] BEST {sym}: {best['buy']}→{best['sell']} "
                          f"net={d.get('net_profit_quote',0):.2f} roi={(d.get('roi',0)*100):.2f}% "
                          f"ask={best.get('best_ask')} bid={best.get('best_bid')}")

                # TopN ongefilterd (toon ook foutmeldingen)
                debug_top = block.get("debug_top") or []
                if debug_top:
                    lines = []
                    for i, p in enumerate(debug_top[:PRINT_TOPN], 1):
                        d = p.get("depth", {}) or {}
                        net = float(d.get("net_profit_quote") or 0.0)
                        roi = float(d.get("roi") or 0.0) * 100.0
                        gross_bps = float(p.get("gross_spread") or 0.0) * 10000.0
                        tag = "OK" if p.get("ok") else ("ERR" if p.get("error") else "NO")
                        line = f"{i}. {p['buy']}→{p['sell']} [{tag}] gross={gross_bps:.1f}bps net={net:.2f} roi={roi:.2f}%"
                        err_type = p.get("error_type")
                        err_msg  = p.get("error")
                        if err_type or err_msg:
                            line += f" (err={err_type or 'Error'}: {err_msg})"
                        lines.append(line)
                    print(f"[strategy] TOP{min(PRINT_TOPN,len(debug_top))} {sym}: " + " | ".join(lines))
                else:
                    print(f"[strategy] no pairs computed for {sym}")

        except Exception as e:
            print("[strategy] error:", e)

        dt_ms = int((time.time() - t0) * 1000)
        await asyncio.sleep(max(0, (interval_ms - dt_ms) / 1000))
