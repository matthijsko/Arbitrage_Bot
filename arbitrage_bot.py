
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
