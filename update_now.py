import pandas as pd
from logic import InvestmentEngine
import requests
import json

# --- CONFIG ---
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1478181689095360665/kI0vRkzbjcrn4MaqIICJ2v_uyKl1H_9B-4S2bq1IDNHMa8h6X05zN2fpX-iGJxJVGBvd"

def send_discord_alert(top_picks):
    """ Sends the Top 3 Oracle picks to Discord """
    if top_picks.empty:
        return

    message = "🚀 **Hourly Market Intelligence Report**\n"
    message += "Top Conviction Picks (Rational Opportunist):\n"
    
    for i, row in top_picks.iterrows():
        message += f"\n🔹 **{row['Ticker']}** | Score: {row['Oracle_Score']}\n"
        message += f"   Price: ${row['Price']} | RSI: {row['RSI']}\n"
        message += f"   *Analysis: {row['AI_Verdict']}*\n"
    
    payload = {"content": message}
    requests.post(DISCORD_WEBHOOK_URL, data=json.dumps(payload), headers={"Content-Type": "application/json"})

def run_headless_update():
    print("Starting hourly background scan...")
    engine = InvestmentEngine()
    
    # 1. Fetch new data
    df = engine.fetch_market_data()
    
    # 2. Get Top 3 Picks from the Oracle
    top_picks = df.sort_values("Oracle_Score", ascending=False).head(3)
    
    # 3. Alert Discord
    send_discord_alert(top_picks)
    print("Update complete. Discord notified.")

if __name__ == "__main__":
    run_headless_update()
