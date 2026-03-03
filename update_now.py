import pandas as pd
from logic import InvestmentEngine
import requests
import json

# Configuration
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1478181689095360665/kI0vRkzbjcrn4MaqIICJ2v_uyKl1H_9B-4S2bq1IDNHMa8h6X05zN2fpX-iGJxJVGBvd"

def send_enhanced_discord_alert(df):
    """Enhanced Discord alert with institutional metrics"""
    if df.empty:
        return

    # Get top picks
    top_picks = df.sort_values("Oracle_Score", ascending=False).head(3)
    
    # Market summary
    avg_oracle = df['Oracle_Score'].mean()
    undervalued_count = len(df[df['margin_of_safety'] > 20])
    high_risk_count = len(df[df['risk_level'] == 'High'])
    
    message = "🏛️ **INSTITUTIONAL MARKET INTELLIGENCE REPORT**\n"
    message += f"📊 Market Summary: Avg Oracle Score: {avg_oracle:.1f} | Undervalued Assets: {undervalued_count} | High Risk: {high_risk_count}\n\n"
    message += "🎯 **TOP CONVICTION PLAYS:**\n"
    
    for i, (_, row) in enumerate(top_picks.iterrows(), 1):
        upside = row.get('margin_of_safety', 0)
        risk = row.get('risk_level', 'Unknown')
        
        message += f"\n**#{i} {row['Ticker']}** | Score: {row['Oracle_Score']} | Risk: {risk}\n"
        message += f"   💰 Current: ${row['Price']} | Fair Value: ${row.get('intrinsic_value', 0)}\n"
        message += f"   📈 Upside: {upside:.1f}% | RSI: {row['RSI']}\n"
        
        # Add key insight
        if 'LLM_Reasoning' in row and row['LLM_Reasoning']:
            reasoning = row['LLM_Reasoning'][:100] + "..." if len(row['LLM_Reasoning']) > 100 else row['LLM_Reasoning']
            message += f"   🤖 AI: {reasoning}\n"
    
    # Add risk warnings
    if high_risk_count > len(df) * 0.5:
        message += "\n⚠️ **RISK ALERT:** High volatility market conditions detected"
    
    # Add geopolitical warnings
    geo_risks = df['Geo_Risk'].sum() if 'Geo_Risk' in df.columns else 0
    if geo_risks > 0:
        message += f"\n🌍 **MACRO ALERT:** {geo_risks} assets with geopolitical exposure"
    
    message += "\n\n*Powered by Institutional Investment Engine v2.0*"
    
    payload = {"content": message}
    
    try:
        response = requests.post(
            DISCORD_WEBHOOK_URL, 
            data=json.dumps(payload), 
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        return response.status_code == 204
    except Exception as e:
        print(f"Discord notification error: {e}")
        return False

def run_headless_update():
    """Enhanced headless update with institutional metrics"""
    print("🏛️ Starting institutional market scan...")
    
    try:
        engine = InvestmentEngine()
        
        # Fetch comprehensive data
        df = engine.fetch_market_data()
        
        if df.empty:
            print("❌ No data retrieved")
            return
        
        print(f"✅ Analyzed {len(df)} assets")
        
        # Send Discord alert
        success = send_enhanced_discord_alert(df)
        
        if success:
            print("📱 Discord notification sent successfully")
        else:
            print("❌ Discord notification failed")
            
        # Print top picks to console
        print("\n🎯 TOP 3 INSTITUTIONAL PICKS:")
        top_picks = df.sort_values("Oracle_Score", ascending=False).head(3)
        for _, row in top_picks.iterrows():
            print(f"{row['Ticker']}: Score {row['Oracle_Score']}, Upside {row.get('margin_of_safety', 0):.1f}%")
            
    except Exception as e:
        print(f"❌ Update failed: {e}")
        # Send error notification
        error_payload = {"content": f"🚨 **SYSTEM ERROR**: Market scan failed - {str(e)[:200]}"}
        requests.post(DISCORD_WEBHOOK_URL, data=json.dumps(error_payload), headers={"Content-Type": "application/json"})

if __name__ == "__main__":
    run_headless_update()
