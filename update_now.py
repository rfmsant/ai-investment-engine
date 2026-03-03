import pandas as pd
from logic import InvestmentEngine
import requests
import json
import time

# Configuration
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1478181689095360665/kI0vRkzbjcrn4MaqIICJ2v_uyKl1H_9B-4S2bq1IDNHMa8h6X05zN2fpX-iGJxJVGBvd"

def send_enhanced_discord_alert(df):
    """Institutional brief for Discord - Fast, short, and actionable"""
    if df.empty:
        return

    # 1. IDENTIFY THE HOTTEST STOCKS (Highest Oracle Score)
    top_picks = df.sort_values("Oracle_Score", ascending=False).head(3)
    
    # 2. MARKET STATS
    avg_score = df['Oracle_Score'].mean()
    value_deals = len(df[df['margin_of_safety'] > 20])
    
    # 3. BUILD THE MESSAGE
    message = "🏛️ **APEX PRO: INSTITUTIONAL BRIEF**\n"
    message += f"📊 **Pulse:** Market Score {avg_score:.0f}/100 | {value_deals} Deep Value Opportunities detected.\n\n"
    message += "🔥 **HOTTEST CONVICTION PLAYS:**\n"
    
    for i, (_, row) in enumerate(top_picks.iterrows(), 1):
        ticker = row['Ticker']
        score = row['Oracle_Score']
        upside = row['margin_of_safety']
        price = row['Price']
        
        # Get the quickest news summary from our JSON logic
        news_brief = "No recent wire data."
        try:
            news_items = json.loads(row['Articles_JSON'])
            if news_items:
                # Take the first headline and its summary
                top_story = news_items[0]
                headline = top_story['headline'][:60]
                summary = top_story['reason'][:100]
                news_brief = f"*{headline}* — {summary}..."
        except:
            pass
            
        message += f"**{i}. {ticker}** (Score: {score} | Upside: {upside:.1f}%)\n"
        message += f"   💰 Price: ${price} | Risk: {row['risk_level']}\n"
        message += f"   📰 Brief: {news_brief}\n\n"
    
    # 4. GEO-RISK / SYSTEM WARNING
    geo_warnings = 0
    for _, row in df.iterrows():
        if "war" in str(row['Articles_JSON']).lower() or "tension" in str(row['Articles_JSON']).lower():
            geo_warnings += 1
            
    if geo_warnings > 5:
        message += f"⚠️ **MACRO ALERT:** Elevated Geopolitical signals detected in {geo_warnings} sectors.\n"
    
    message += "\n*Powered by Apex Markets Pro v6.5*"
    
    payload = {"content": message}
    
    try:
        response = requests.post(
            DISCORD_WEBHOOK_URL, 
            data=json.dumps(payload), 
            headers={"Content-Type": "application/json"},
            timeout=15
        )
        return response.status_code == 204
    except Exception as e:
        print(f"Discord notification error: {e}")
        return False

def run_headless_update():
    """Run the institutional scan and alert Discord"""
    print("◈ Starting Apex Market Intelligence Scan...")
    
    try:
        engine = InvestmentEngine()
        
        # This will scan your full universe (~100+ stocks)
        df = engine.fetch_market_data()
        
        if df.empty:
            print("❌ No data retrieved from market.")
            return
        
        print(f"✅ Analysis complete for {len(df)} assets.")
        
        # Send to Discord
        success = send_enhanced_discord_alert(df)
        
        if success:
            print("📱 Intelligence Brief sent to Discord.")
        else:
            print("❌ Discord communication failed.")
            
    except Exception as e:
        print(f"❌ Critical System Failure: {e}")
        # Send emergency error to Discord
        err_msg = {"content": f"🚨 **SYSTEM FAILURE**: Market scan crashed - {str(e)[:150]}"}
        requests.post(DISCORD_WEBHOOK_URL, json=err_msg)

if __name__ == "__main__":
    run_headless_update()
