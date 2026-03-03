import pandas as pd
from logic import InvestmentEngine
import requests, json, os

WEBHOOK = "https://discord.com/api/webhooks/1478181689095360665/kI0vRkzbjcrn4MaqIICJ2v_uyKl1H_9B-4S2bq1IDNHMa8h6X05zN2fpX-iGJxJVGBvd"

def run_automated_brief():
    print("◈ Starting Background Scan...")
    engine = InvestmentEngine()
    df = engine.fetch_market_data() # Scans all 500+
    
    # Send hottest stock to Discord
    top = df.sort_values("Oracle_Score", ascending=False).iloc[0]
    msg = f"🏛️ **INSTITUTIONAL BRIEF (AUTO)**\n🔥 Top Pick: {top['Ticker']} (Score: {top['Oracle_Score']})\n💰 Price: ${top['Price']} | Upside: {top['margin_of_safety']}%"
    requests.post(WEBHOOK, json={"content": msg})
    print("✅ Scan complete and Discord alerted.")

if __name__ == "__main__":
    run_automated_brief()
