import pandas as pd
from logic import InvestmentEngine
import requests, json

WEBHOOK = "https://discord.com/api/webhooks/1478181689095360665/kI0vRkzbjcrn4MaqIICJ2v_uyKl1H_9B-4S2bq1IDNHMa8h6X05zN2fpX-iGJxJVGBvd"

def run_all():
    engine = InvestmentEngine()
    print("Starting full scan...")
    # This might take 15 mins
    df = engine.fetch_market_data()
    top = df.sort_values("Oracle_Score", ascending=False).iloc[0]
    
    msg = f"🏛️ **INSTITUTIONAL BRIEF**\nTop: {top['Ticker']} (Score: {top['Oracle_Score']})\nUpside: {top['margin_of_safety']}%"
    requests.post(WEBHOOK, json={"content": msg})

if __name__ == "__main__":
    run_all()
