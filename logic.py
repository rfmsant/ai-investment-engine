import yfinance as yf
import pandas as pd
import numpy as np
import requests
import xml.etree.ElementTree as ET
from textblob import TextBlob
import os, json, time, warnings
from datetime import datetime
from typing import Dict, List, Tuple
from openai import OpenAI

warnings.filterwarnings("ignore")

CACHE_FILE = "market_cache.csv"
NO_DCF_SECTORS = {"ETF", "Commodities", "FX"}

# ===========================================================================
# 1. AI PROMPT ENGINEERING (Motley Fool Style)
# ===========================================================================
def generate_ai_report(ticker: str, data: dict, news: list, api_key: str) -> str:
    try:
        client = OpenAI(api_key=api_key)
        
        # Prepare news summary
        if news and len(news) > 0:
            news_summary = "\n".join([f"- {n['headline']} (Sent: {n['label']})" for n in news[:8]])
        else:
            news_summary = "No specific news headlines found. Rely on fundamental data."
        
        prompt = f"""
        Act as a Lead Analyst for 'The Motley Fool'. 
        Conduct a rigorous 'Stock Advisor' deep dive on {ticker}.

        --- DATA SNAPSHOT ---
        Current Price: ${data.get('Price')}
        Intrinsic Value (DCF): ${data.get('intrinsic_value')}
        Upside Potential: {data.get('margin_of_safety')}%
        Growth Estimate (5yr): {data.get('growth_rate')}%
        RSI (14d): {data.get('RSI')}
        Risk Level: {data.get('risk_level')}

        --- NEWS WIRE ---
        {news_summary}

        --- ANALYSIS FRAMEWORK ---
        Please structure your response exactly as follows (Use Markdown):

        ### 1. The Numbers 📊
        *   Analyze the valuation gap (Price vs DCF). Is it a bargain or a trap?
        *   Comment on the growth rate ({data.get('growth_rate')}%) relative to a tech-heavy or defensive market.

        ### 2. The Forecast 🔮
        *   Based on the data, where could this stock be in 12-24 months?
        *   What is the consensus trajectory?

        ### 3. The News Cycle 📰
        *   Synthesize the provided headlines. What is the dominant narrative? (Fear, Greed, Uncertainty?)
        *   Are these short-term noise or long-term signals?

        ### 4. Geopolitical & Macro Impact 🌍
        *   Analyze exposure to: China/Taiwan tensions, Supply Chain wars, Interest Rates.
        *   Does the news feed suggest any macro headwinds?

        ### 5. Risk Analysis (The Bear Case) 🐻
        *   **Valuation Risk:** Is it priced for perfection?
        *   **Execution Risk:** What if they miss earnings?
        *   **Macro Risk:** Recession vulnerability.

        ### 6. THE VERDICT ⚖️
        *   **Rating:** [BUY / HOLD / SELL] (Make this bold and color coded if possible)
        *   **Timeframe:** (e.g., "Hold for 3-5 years" or "Trade for 2 weeks")
        *   **Summary:** One definitive sentence explaining why.

        Tone: Educational, Conviction-based, but realistic.
        """

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"⚠️ Error generating AI report: {str(e)}"

# ===========================================================================
# 2. ROBUST NEWS FETCHING (Multi-Source)
# ===========================================================================
def fetch_news(ticker: str) -> List[Dict]:
    articles = []
    
    # HEADERS ARE CRITICAL to avoid 403 Forbidden errors
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    # SOURCE 1: Google News RSS (Specific Topic Query)
    # Using 'when:30d' ensures freshness
    try:
        url = f"https://news.google.com/rss/search?q={ticker}+stock+when:14d&hl=en-US&gl=US&ceid=US:en"
        response = requests.get(url, headers=headers, timeout=5)
        
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            items = root.findall('./channel/item')
            for item in items[:5]:
                title = item.find('title').text
                link = item.find('link').text
                
                # Filter out generic junk
                if "Market cap" in title or "ETF" in title: continue

                # Sentiment
                blob = TextBlob(title)
                score = blob.sentiment.polarity
                
                articles.append({
                    "headline": title,
                    "link": link,
                    "score": round(score, 2),
                    "label": "Bullish" if score > 0.05 else "Bearish" if score < -0.05 else "Neutral",
                    "reason": "Sentiment Scan"
                })
    except:
        pass

    # SOURCE 2: Fallback to Yahoo if Google is empty or fails
    if not articles:
        try:
            stock = yf.Ticker(ticker)
            y_news = stock.news
            if y_news:
                for item in y_news[:4]:
                    title = item.get('title')
                    if title:
                        blob = TextBlob(title)
                        score = blob.sentiment.polarity
                        articles.append({
                            "headline": title,
                            "link": item.get('link', '#'),
                            "score": round(score, 2),
                            "label": "Bullish" if score > 0.05 else "Bearish" if score < -0.05 else "Neutral",
                            "reason": "Yahoo Data"
                        })
        except:
            pass

    return articles

# ===========================================================================
# 3. DCF LOGIC
# ===========================================================================
def calc_dcf(stock: yf.Ticker, info: dict, sector: str) -> Dict:
    res = {"intrinsic_value": 0.0, "margin_of_safety": 0.0, "growth_rate": 0.0, "dcf_method": "none"}
    
    if sector in NO_DCF_SECTORS: 
        res["dcf_method"] = "skipped (sector)"
        return res
    
    try:
        fcf = 0
        cf_df = stock.cashflow
        
        if cf_df is not None and not cf_df.empty:
            if "Free Cash Flow" in cf_df.index:
                val = pd.to_numeric(cf_df.loc["Free Cash Flow"], errors='coerce').iloc[0]
                if val > 0: fcf = val
            
            # Manual fallback (OCF + CapEx)
            if fcf == 0 and "Total Cash From Operating Activities" in cf_df.index and "Capital Expenditures" in cf_df.index:
                ocf = cf_df.loc["Total Cash From Operating Activities"].iloc[0]
                capex = cf_df.loc["Capital Expenditures"].iloc[0]
                if (ocf + capex) > 0: fcf = ocf + capex

        if fcf == 0: fcf = info.get("freeCashflow", 0)

        if fcf <= 0: return res

        rev_g = info.get("revenueGrowth", 0.05) or 0.05
        res["growth_rate"] = round(rev_g * 100, 1)
        
        discount = 0.10
        terminal = 0.025
        
        # 5 Year Projection
        future_fcf = [fcf * ((1 + rev_g) ** i) for i in range(1, 6)]
        tv = (future_fcf[-1] * (1 + terminal)) / (discount - terminal)
        
        enterprise_val = sum([f / ((1 + discount) ** (i + 1)) for i, f in enumerate(future_fcf)])
        enterprise_val += tv / ((1 + discount) ** 5)
        
        equity_val = enterprise_val - info.get("totalDebt", 0) + info.get("totalCash", 0)
        shares = info.get("sharesOutstanding", 0)
        
        if shares > 0:
            fair_val = equity_val / shares
            cp = info.get("currentPrice", 0) or 1
            
            # Sanity Cap
            if fair_val > cp * 4: fair_val = cp * 1.5
            
            res["intrinsic_value"] = round(fair_val, 2)
            res["margin_of_safety"] = round(((fair_val - cp) / cp) * 100, 1)

        return res

    except:
        return res

# ===========================================================================
# 4. ENGINE CORE
# ===========================================================================
def _calc_rsi(prices, period=14):
    try:
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs.iloc[-1]))
    except:
        return 50

def _calc_risk_level(daily_vol):
    if daily_vol > 3.0: return "High"
    if daily_vol > 1.5: return "Medium"
    return "Low"

class InvestmentEngine:
    def __init__(self):
        # Expanded Universe
        self.tickers = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AMD", "INTC", "NFLX", "JPM", "BAC", "LLY", "XOM", "CVX", "KO", "PEP", "COST", "WMT", "DIS", "TSM", "AVGO", "ORCL", "CRM"]

    def load_data(self):
        if os.path.exists(CACHE_FILE):
            return pd.read_csv(CACHE_FILE), True
        return pd.DataFrame(), False

    def fetch_market_data(self, progress_bar=None):
        data = []
        total = len(self.tickers)
        
        for i, t in enumerate(self.tickers):
            if progress_bar: 
                progress_bar.progress((i+1)/total, text=f"Analyzing {t}...")
            
            try:
                stock = yf.Ticker(t)
                info = stock.info
                hist = stock.history(period="3mo")
                
                price = info.get("currentPrice", 0)
                if price == 0 and not hist.empty: price = hist['Close'].iloc[-1]
                
                dcf = calc_dcf(stock, info, info.get("sector", ""))
                rsi = _calc_rsi(hist['Close']) if not hist.empty else 50
                
                # Risk Calc
                daily_vol = 0
                if not hist.empty:
                     daily_returns = hist['Close'].pct_change().dropna()
                     daily_vol = daily_returns.std() * 100
                
                news = fetch_news(t)
                avg_sent = np.mean([n['score'] for n in news]) if news else 0
                
                score = 50 + (dcf['margin_of_safety'] / 3) + (avg_sent * 10)
                if rsi < 30: score += 10
                if rsi > 70: score -= 10
                score = max(0, min(100, int(score)))

                record = {
                    "Ticker": t,
                    "Sector": info.get("sector", "N/A"),
                    "Price": round(price, 2),
                    "intrinsic_value": dcf["intrinsic_value"],
                    "margin_of_safety": dcf["margin_of_safety"],
                    "growth_rate": dcf["growth_rate"],
                    "RSI": round(rsi, 1),
                    "Sentiment": round(avg_sent, 2),
                    "Oracle_Score": score,
                    "AI_Verdict": "Strong Buy" if score > 75 else "Sell" if score < 40 else "Hold",
                    "risk_level": _calc_risk_level(daily_vol),
                    "Articles_JSON": json.dumps(news)
                }
                data.append(record)
                time.sleep(0.5) 
            except Exception as e:
                print(f"Skipping {t}: {e}")
                
        df = pd.DataFrame(data)
        if not df.empty:
            df.to_csv(CACHE_FILE, index=False)
        return df
