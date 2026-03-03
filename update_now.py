import pandas as pd
from logic import InvestmentEngine
import requests
import json

DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1478181689095360665/kI0vRkzbjcrn4MaqIICJ2v_uyKl1H_9B-4S2bq1IDNHMa8h6X05zN2fpX-iGJxJVGBvd"

def send_update(df):
    if df.empty: return
    top = df.sort_values("Oracle_Score", ascending=False).iloc[0]
    
    msg = f"🏛️ **INSTITUTIONAL BRIEF**\n"
    msg += f"🔥 **Top Pick:** {top['Ticker']} (Score: {top['Oracle_Score']})\n"
    msg += f"💰 **Price:** ${top['Price']} | **Upside:** {top['margin_of_safety']}%\n"
    
    # News Snippet
    news = json.loads(top['Articles_JSON'])
    if news:
        msg += f"📰 **Brief:** {news[0]['reason']}\n"
    
    # Macro check
    geo = df[df['Articles_JSON'].str.contains('war|conflict|tariff', case=False)]
    if not geo.empty:
        msg += f"\n🌍 **MACRO ALERT:** Geopolitical risks detected in {len(geo)} assets.\n"

    requests.post(DISCORD_WEBHOOK_URL, json={"content": msg})

if __name__ == "__main__":
    engine = InvestmentEngine()
    df = engine.fetch_market_data()
    send_update(df)
