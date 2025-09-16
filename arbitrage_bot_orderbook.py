
import time
import requests
import asyncio
import websockets
import json
import threading
import ssl
import certifi
from datetime import datetime, timedelta

# Live prijsopslag
bitvavo_prices = {}
coinbase_prices = {}
kraken_prices = {}

# Exchange fees
EXCHANGE_FEES = {
    "Bitvavo": 0.25,  
    "Coinbase": 0.50,
    "Kraken": 0.26
}


# Cleanup the csv file
def cleanup_csv(max_age_hours=24):
    rows = []
    cutoff = datetime.now() - timedelta(hours=max_age_hours)

    try:
        with open(logfile, mode="r") as file:
            reader = csv.reader(file)
            for row in reader:
                try:
                    row_time = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
                    if row_time > cutoff:
                        rows.append(row)
                except:
                    continue
    except FileNotFoundError:
        return

    with open(logfile, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerows(rows)

    print(f"🧹 Oude data verwijderd, {len(rows)} regels overgehouden")


# REST API ophalen van EUR-markten
def get_bitvavo_eur_markets():
    url = "https://api.bitvavo.com/v2/markets"
    try:
        r = requests.get(url)
        markets = r.json()
        return {m["market"].split("-")[0] for m in markets if m["quote"] == "EUR"}
    except Exception as e:
        print(f"Fout bij ophalen Bitvavo-markten: {e}")
        return set()

def get_coinbase_eur_markets():
    url = "https://api.exchange.coinbase.com/products"
    try:
        r = requests.get(url)
        data = r.json()
        return {item["base_currency"] for item in data if item["quote_currency"] == "EUR"}
    except Exception as e:
        print(f"Fout bij ophalen Coinbase-markten: {e}")
        return set()

def get_kraken_eur_markets():
    url = "https://api.kraken.com/0/public/AssetPairs"
    try:
        r = requests.get(url)
        pairs = r.json()["result"]
        coins = set()
        for pair in pairs.values():
            if pair.get("quote") == "ZEUR":
                base = pair.get("wsname", "").split("/")[0]
                if base:
                    coins.add(base)
        return coins
    except Exception as e:
        print(f"Fout bij ophalen Kraken-markten: {e}")
        return set()

def get_common_eur_coins():
    bitvavo = get_bitvavo_eur_markets()
    coinbase = get_coinbase_eur_markets()
    kraken = get_kraken_eur_markets()

    print(f"Bitvavo: {len(bitvavo)} coins")
    print(f"Coinbase: {len(coinbase)} coins")
    print(f"Kraken: {len(kraken)} coins")

    common = {c.upper() for c in bitvavo} & {c.upper() for c in coinbase} & {c.upper() for c in kraken}
    print(f"Gedeelde EUR-munten op alle exchanges: {sorted(list(common))}")
    return sorted(list(common))

# WebSocket-implementaties
def start_bitvavo_ws(symbols):
    async def listen():
        url = "wss://ws.bitvavo.com/v2/"
        async with websockets.connect(url, ssl=ssl.create_default_context(cafile=certifi.where())) as ws:
            for sym in symbols:
                await ws.send(json.dumps({
                    "action": "subscribe",
                    "channel": "ticker",
                    "market": f"{sym}-EUR"
                }))
            while True:
                try:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if data.get("event") == "ticker":
                        sym = data["market"].split("-")[0]
                        price = float(data["last"])
                        bitvavo_prices[sym.upper()] = price
                except Exception as e:
                    print(f"[Bitvavo WS] Fout: {e}")
                    await asyncio.sleep(5)

    threading.Thread(target=lambda: asyncio.new_event_loop().run_until_complete(listen()), daemon=True).start()

def start_coinbase_ws(symbols):
    async def listen():
        url = "wss://ws-feed.exchange.coinbase.com"
        pairs = [f"{sym}-EUR" for sym in symbols]

        subscribe_msg = {
            "type": "subscribe",
            "channels": [{"name": "ticker", "product_ids": pairs}]
        }

        async with websockets.connect(url, ssl=ssl.create_default_context(cafile=certifi.where())) as ws:
            await ws.send(json.dumps(subscribe_msg))
            while True:
                try:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if data.get("type") == "ticker":
                        product = data.get("product_id")
                        price = data.get("price")
                        if product and price:
                            coin = product.split("-")[0]
                            coinbase_prices[coin.upper()] = float(price)
                except Exception as e:
                    print(f"[Coinbase WS] Fout: {e}")
                    await asyncio.sleep(5)

    threading.Thread(target=lambda: asyncio.new_event_loop().run_until_complete(listen()), daemon=True).start()

def start_kraken_ws(symbols):
    async def listen():
        url = "wss://ws.kraken.com/"
        pairs = [f"{sym}/EUR" for sym in symbols]

        subscribe_msg = {
            "event": "subscribe",
            "pair": pairs,
            "subscription": {"name": "ticker"}
        }

        async with websockets.connect(url, ssl=ssl.create_default_context(cafile=certifi.where())) as ws:
            await ws.send(json.dumps(subscribe_msg))
            symbol_map = {f"{sym}/EUR": sym for sym in symbols}

            while True:
                try:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if isinstance(data, list) and len(data) > 1:
                        info = data[1]
                        pair = data[-1]
                        price = float(info["c"][0])
                        coin = symbol_map.get(pair)
                        if coin:
                            kraken_prices[coin.upper()] = price
                except Exception as e:
                    print(f"[Kraken WS] Fout: {e}")
                    await asyncio.sleep(5)

    threading.Thread(target=lambda: asyncio.new_event_loop().run_until_complete(listen()), daemon=True).start()

# Entry point
if __name__ == "__main__":
    coins = get_common_eur_coins()
    if not coins:
        print("Geen gedeelde munten gevonden. Stoppen.")
        exit()

    print("📡 Start WebSocket feeds...")
    start_bitvavo_ws(coins)
    start_coinbase_ws(coins)
    start_kraken_ws(coins)

    time.sleep(5)
    print("✅ Live prijsfeeds gestart. Klaar voor arbitrage-controle...")

import csv
from datetime import datetime

logfile = "arbitrage_log.csv"

def calculate_arbitrage_and_log(coins):
    counter = 0
    cleanup_interval = 60
    
    while True:
        for coin in coins:
            try:
                b_price = bitvavo_prices.get(coin)
                c_price = coinbase_prices.get(coin)
                k_price = kraken_prices.get(coin)

                prices = {
                    "Bitvavo": b_price,
                    "Coinbase": c_price,
                    "Kraken": k_price
                }

                # Filter op alleen beschikbare prijzen
                valid_prices = {k: v for k, v in prices.items() if v is not None}
                if len(valid_prices) < 2:
                    continue

                max_ex = max(valid_prices, key=valid_prices.get)
                min_ex = min(valid_prices, key=valid_prices.get)
                max_price = valid_prices[max_ex]
                min_price = valid_prices[min_ex]

                percent_diff = ((max_price - min_price) / min_price) * 100
                
                buy_fee = EXCHANGE_FEES[min_ex]
                sell_fee = EXCHANGE_FEES[max_ex]
                total_fees = buy_fee + sell_fee
                
                net_percent = percent_diff - total_fees

                if net_percent > 0:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    log_to_csv(timestamp, coin, prices, f"{min_ex} → {max_ex}", net_percent)
                    print(f"[{timestamp}] {coin}: {min_ex} → {max_ex}, Bruto: {percent_diff:.2f}%, Netto: {net_percent:.2f}%")

            except Exception as e:
                print(f"Fout bij {coin}: {e}")
                
        counter += 1
        if counter >= cleanup_interval:
            cleanup_csv(24)  # bewaar alleen laatste 24 uur
            counter = 0

        time.sleep(5)

def log_to_csv(timestamp, coin, prices, route, net_percent, gross_percent=None):
    with open(logfile, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([
            timestamp,
            coin,
            prices.get("Kraken", "N/A"),
            prices.get("Coinbase", "N/A"),
            prices.get("Bitvavo", "N/A"),
            route,
            f"{gross_percent:.2f}%" if gross_percent else "N/A",
            f"{net_percent:.2f}%"
        ])



# Start de arbitrage loop
calculate_arbitrage_and_log(coins)



# === Volume-aware arbitrage integration (appended by assistant) ===
# This adds a depth-aware simulator so you can compute the *actual* executable size and PnL.
# It is designed to be non-invasive: existing code above remains unchanged.

try:
    from arb_depth_utils import simulate_cross_fill
except Exception as _e:
    simulate_cross_fill = None
    # If needed, copy arb_depth_utils.py into your project and ensure it's importable.

def evaluate_opportunity_with_depth(
    asks,                      # list[(price, size_base)] low->high from BUY exchange
    bids,                      # list[(price, size_base)] high->low from SELL exchange
    fee_buy=0.0010,            # taker fee on BUY exchange (e.g., 0.001 = 0.10%)
    fee_sell=0.0010,           # taker fee on SELL exchange
    withdraw_fee_base=0.0,     # network withdrawal fee in BASE (deducted from transfer)
    max_quote_buy=None,        # cap in QUOTE currency for the BUY side (e.g., USD/USDT)
    max_base_sell=None,        # optional: pre-funded base balance at SELL exchange
    base_step=None,            # lot size step for BASE (precision), e.g., 0.0001 BTC
    min_base=None,             # minimum base trade size
    min_notional_buy=None,     # min notional on BUY side (in QUOTE)
    min_notional_sell=None     # min notional on SELL side (in QUOTE)
):
    """
    Returns a dict with filled quantities, average prices, fees, net profit (QUOTE), and ROI.
    """
    if simulate_cross_fill is None:
        raise RuntimeError("simulate_cross_fill not available. Ensure arb_depth_utils.py is on PYTHONPATH.")
    return simulate_cross_fill(
        asks=asks,
        bids=bids,
        fee_buy=fee_buy,
        fee_sell=fee_sell,
        withdraw_fee_base=withdraw_fee_base,
        max_quote_buy=max_quote_buy,
        max_base_sell=max_base_sell,
        base_step=base_step,
        min_base=min_base,
        min_notional_buy=min_notional_buy,
        min_notional_sell=min_notional_sell
    )


def fetch_orderbooks_ccxt(exchange_buy, exchange_sell, symbol, limit=50):
    """
    Convenience helper using CCXT exchanges to fetch depth in the correct sorting:
    - asks low->high (for buying)
    - bids high->low (for selling)
    """
    order_book_buy = exchange_buy.fetch_order_book(symbol, limit=limit)
    order_book_sell = exchange_sell.fetch_order_book(symbol, limit=limit)

    asks = order_book_buy.get("asks", [])
    bids = order_book_sell.get("bids", [])

    # Normalize numeric types, sort properly for safety
    asks = [(float(p), float(s)) for p, s in asks if float(s) > 0.0]
    bids = [(float(p), float(s)) for p, s in bids if float(s) > 0.0]
    asks.sort(key=lambda x: x[0])           # low -> high
    bids.sort(key=lambda x: x[0], reverse=True)  # high -> low
    return asks, bids


def print_depth_result(res, quote_symbol="QUOTE", base_symbol="BASE"):
    """
    Pretty-print the simulation result.
    """
    def f(x): 
        return f"{x:,.8f}" if abs(x) < 1 else f"{x:,.4f}"
    print("=== Depth-aware Arbitrage Result ===")
    print(f"Qty bought ({base_symbol}): {f(res.get('qty_base_bought', 0.0))}")
    print(f"Qty after withdraw ({base_symbol}): {f(res.get('qty_base_after_withdraw', 0.0))}")
    print(f"Qty sold ({base_symbol}): {f(res.get('qty_base_sold', 0.0))}")
    print(f"Avg buy px ({quote_symbol}/{base_symbol}): {f(res.get('avg_buy_px', 0.0))}")
    print(f"Avg sell px ({quote_symbol}/{base_symbol}): {f(res.get('avg_sell_px', 0.0))}")
    print(f"Spread (%): {res.get('effective_spread', 0.0)*100:.4f}")
    print(f"Spent ({quote_symbol}): {f(res.get('spent_quote', 0.0))}")
    print(f"Received ({quote_symbol}): {f(res.get('received_quote', 0.0))}")
    print(f"Buy fees ({quote_symbol}): {f(res.get('buy_fee_quote', 0.0))}")
    print(f"Sell fees ({quote_symbol}): {f(res.get('sell_fee_quote', 0.0))}")
    print(f"Withdraw fee ({base_symbol}): {f(res.get('withdraw_fee_base', 0.0))}")
    print(f"Net profit ({quote_symbol}): {f(res.get('net_profit_quote', 0.0))}")
    print(f"ROI (% on spent): {res.get('roi', 0.0)*100:.4f}")
    print(f"OK (profitable?): {res.get('ok', 0)}")


if __name__ == "__main__":
    # Optional CLI demo if user runs this file directly with CCXT configured.
    # This block won't affect library usage of your existing functions.
    import os
    if os.environ.get("DEPTH_DEMO", "0") == "1":
        try:
            import ccxt  # type: ignore
            # Example: buy on Exchange A, sell on Exchange B (replace with your keys/config)
            a = ccxt.binance({"enableRateLimit": True})
            b = ccxt.kraken({"enableRateLimit": True})
            symbol = "BTC/USDT"  # adjust as needed

            asks, bids = fetch_orderbooks_ccxt(a, b, symbol, limit=50)

            # You should pull these from exchange.markets[symbol] for production
            base_step = 0.0001
            min_notional = 5.0

            res = evaluate_opportunity_with_depth(
                asks=asks,
                bids=bids,
                fee_buy=0.0010,
                fee_sell=0.0012,
                withdraw_fee_base=0.0005,    # e.g., BTC network fee
                max_quote_buy=200.0,         # try with a small budget
                base_step=base_step,
                min_notional_buy=min_notional,
                min_notional_sell=min_notional
            )
            print_depth_result(res, quote_symbol="USDT", base_symbol="BTC")
        except Exception as e:
            print("[DEPTH_DEMO] Skipped demo:", e)
# === End of volume-aware integration ===
