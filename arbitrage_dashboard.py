import streamlit as st
import pandas as pd
import os
from datetime import datetime

LOG_FILE = "spread_log.csv"

st.set_page_config(page_title="Arbitrage Dashboard", layout="wide")
st.title("📈 Crypto Arbitrage Dashboard - Meest recente kansen per coin")

if os.path.exists(LOG_FILE):
    df = pd.read_csv(LOG_FILE)

    if not df.empty:
        # Convert timestamp kolom naar datetime
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Voor elke coin pakken we de laatste entry (op timestamp)
        latest_per_coin = df.sort_values("timestamp").groupby("coin").tail(1)

        # Maak een kolom alleen met tijd (uur:minuut:seconde)
        latest_per_coin["time"] = latest_per_coin["timestamp"].dt.strftime("%H:%M:%S")

        # Kolommen kiezen voor weergave
        display_df = latest_per_coin[[
            "coin", "buy_exchange", "buy_price", "sell_exchange", "sell_price", "spread", "time"
        ]].sort_values("spread", ascending=False).reset_index(drop=True)

        st.dataframe(display_df, use_container_width=True)
    else:
        st.warning("Spread-log bestaat, maar bevat nog geen data.")
else:
    st.error("spread_log.csv niet gevonden. Zorg dat je hoofdscript draait.")
