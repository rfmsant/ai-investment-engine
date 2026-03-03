import yfinance as yf
import pandas as pd
import numpy as np
import requests
import xml.etree.ElementTree as ET
from textblob import TextBlob
from bs4 import BeautifulSoup
import os, json, time, warnings
from datetime import datetime
from typing import Dict, List
from openai import OpenAI

warnings.filterwarnings("ignore")

CACHE_FILE = "market_cache.csv"
NO_DCF_SECTORS = {"ETF", "Commodities", "FX"}

# ===========================================================================
# 1. EXPANDED UNIVERSE (Categorized)
# ===========================================================================
SECTOR_MAP = {
    "AI_Tech": ["NVDA", "MSFT", "GOOGL", "AMD", "PLTR", "SMCI", "TSM", "META", "TSLA", "CRM", "ADBE", "NFLX", "AMZN", "ORCL", "IBM", "INTC", "QCOM", "AVGO", "NOW", "SNOW"],
    "Energy": ["XOM", "CVX", "SHEL", "TTE", "COP", "SLB", "EOG", "VLO", "MPC", "BP"],
    "Nuclear_Energy": ["CCJ", "NEE", "DUK", "SO", "AEP", "EXC"],
    "Food_Agri": ["ADM", "CTVA", "CF", "MOS", "NTR", "FMC", "DE", "CAT", "CNH", "AGCO"],
    "Food_Staples": ["KO", "PEP", "MDLZ", "GIS", "K", "CPB", "KHC", "HSY", "STZ"],
    "Raw_Materials": ["FCX", "SCCO", "RIO", "VALE", "BHP", "AA", "NUE", "CLF", "X"],
    "Gold_Minerals": ["NEM", "GOLD", "AEM", "KGC"],
    "Biochem_Pharma": ["LLY", "NVO", "JNJ", "PFE", "MRK", "ABBV", "BMY", "GILD", "AMGN", "BIIB", "REGN", "VRTX"],
    "War_Defense": ["LMT", "RTX", "NOC", "GD", "BA", "HII", "LHX", "TXT", "KTOS", "AVAV", "RHM.DE"]
}

# Flatten list for scanner
FULL_TICKER_LIST = [t for sublist in SECTOR_MAP.values() for t in sublist]
# Remove duplicates
FULL_TICKER_LIST = list(set(FULL_TICKER_LIST))

# ===========================================================================
# 2. NEWS CLASSIFIER (The Summary Fix)
# ===========================================================================
def classify_news(headline):
    h = headline.lower()
    if any(x in h for x in ['earnings', 'profit', 'revenue', 'eps', 'quarter']): return "📊 Earnings/Financials"
    if any(x in h for x in ['downgrade', 'upgrade', 'rating', 'target']): return "🎯 Analyst Rating"
    if any(x in h for x in ['war', 'conflict', 'china', 'tariff', 'ban', 'sanction']): return "🌍 Geo-Politics"
    if any(x in h for x in ['buy', 'acquire', 'merger', 'deal']): return "🤝 M&A / Deals"
    if any(x in h for x in ['fda', 'drug', 'approval', 'trial']): return "💊 Biotech/Reg"
    if any(x in h for x in ['strike', 'lawsuit', 'fine', 'regulator']): return "⚖️ Legal/Labor"
    return "📰 General News"

def fetch_news(ticker: str) -> List[Dict]:
    articles = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"}

    try:
        # Use Google RSS for speed
        url = f"https://news.google.com/rss/search?q={ticker}+stock+when:7d&hl=en-US&gl=US&ceid=US:en"
        response = requests.get(url, headers=headers, timeout=4)
        
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            for item in root.findall('./channel/item')[:5]:
                title = item.find('title').text
                link = item.find('link').text
                
                # Sentiment
                blob = TextBlob(title)
                score = blob.sentiment.polarity
                
                # Context Tag
                context = classify_news(title)

                articles.append({
                    "headline": title,
                    "link": link,
                    "score": round(score, 2),
                    "label": "Bullish" if score > 0.05 else "Bearish" if score < -0.05 else "Neutral",
                    "reason": context # This provides the "Why"
                })
    except:
        pass
    
    return articles

# ===========================================================================
# 3. AI REPORT
# ===========================================================================
def generate_ai_report(ticker: str, data: dict, news: list, api_key: str) -> str:
    try:
        client = OpenAI(api_key=api_key)
        
        # News Context
        news_txt = "\n".join([f"- {n['reason']}: {n['headline']}" for n in news[:6]])
        
        prompt = f"""
        Act as a Senior Analyst for 'The Motley Fool'. Analysis for: {ticker}.

        --- DATA ---
        Price: ${data.get('Price')} | Fair Value: ${data.get('intrinsic_value')}
        Upside: {data.get('margin_of_safety')}% | Growth: {data.get('growth_rate')}%
        Sector: {data.get('Sector')} | Risk: {data.get('risk_level')}

        --- NEWS WIRE ---
        {news_txt}

        --- FORMAT (Markdown) ---
        ### 1. The Numbers 📊
        *   **Valuation:** Analyze Price vs Fair Value.
        *   **Growth:** Comment on the {data.get('growth_rate')}% estimate.

        ### 2. The Forecast 🔮
        *   Prediction for the next 12 months.

        ### 3. Geopolitical & Macro 🌍
        *   Specific risks related to {data.get('Sector')} (e.g. War, Rates, Supply Chain).

        ### 4. Risk Analysis 🐻
        *   Bull case vs Bear case.

        ### 5. THE VERDICT ⚖️
        *   **RATING:** [BUY / HOLD / SELL] (Bold)
        *   **Summary:** 1 sentence conclusion.
        """

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"⚠️ Error: {str(e)}"

# ===========================================================================
# 4. CALCULATIONS
# ===========================================================================
def calc_dcf(stock: yf.Ticker, info: dict, sector: str) -> Dict:
    res = {"intrinsic_value": 0.0, "margin_of_safety": 0.0, "growth_rate": 0.0, "dcf_method": "none"}
    if sector in NO_DCF_SECTORS: return res
    try:
        fcf = 0
        cf_df = stock.cashflow
        if cf_df is not None and not cf_df.empty:
            if "Free Cash Flow" in cf_df.index:
                val = pd.to_numeric(cf_df.loc["Free Cash Flow"], errors='coerce').iloc[0]
                if val > 0: fcf = val
            if fcf == 0 and "Total Cash From Operating Activities" in cf_df.index and "Capital Expenditures" in cf_df.index:
                ocf = cf_df.loc["Total Cash From Operating Activities"].iloc[0]
                capex = cf_df.loc["Capital Expenditures"].iloc[0]
                if (ocf + capex) > 0: fcf = ocf + capex
        if fcf == 0: fcf = info.get("freeCashflow", 0)
        
        if fcf > 0:
            rev_g = info.get("revenueGrowth", 0.05) or 0.05
            res["growth_rate"] = round(rev_g * 100, 1)
            discount = 0.10
            future_fcf = [fcf * ((1 + rev_g) ** i) for i in range(1, 6)]
            tv = (future_fcf[-1] * 1.025) / (discount - 0.025)
            ev = sum([f / ((1 + discount) ** (i + 1)) for i, f in enumerate(future_fcf)]) + (tv / (1 + discount) ** 5)
            equity = ev - info.get("totalDebt", 0) + info.get("totalCash", 0)
            shares = info.get("sharesOutstanding", 0)
            if shares > 0:
                iv = equity / shares
                cp = info.get("currentPrice", 0) or 1
                if iv > cp * 4: iv = cp * 1.5 # Cap
                res["intrinsic_value"] = round(iv, 2)
                res["margin_of_safety"] = round(((iv - cp) / cp) * 100, 1)
    except: pass
    return res

def _calc_rsi(prices, period=14):
    try:
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs.iloc[-1]))
    except: return 50

def _calc_risk_level(daily_vol):
    if daily_vol > 3.0: return "High"
    if daily_vol > 1.5: return "Medium"
    return "Low"

# ===========================================================================
# 5. ENGINE
# ===========================================================================
class InvestmentEngine:
    def __init__(self):
        self.tickers = FULL_TICKER_LIST

    def load_data(self):
        if os.path.exists(CACHE_FILE): return pd.read_csv(CACHE_FILE), True
        return pd.DataFrame(), False

    def fetch_market_data(self, progress_bar=None):
        data = []
        total = len(self.tickers)
        for i, t in enumerate(self.tickers):
            if progress_bar: progress_bar.progress((i+1)/total, text=f"Scanning {t}...")
            try:
                stock = yf.Ticker(t)
                info = stock.info
                hist = stock.history(period="3mo")
                price = info.get("currentPrice", 0)
                if price == 0 and not hist.empty: price = hist['Close'].iloc[-1]
                
                # Determine Sector from our map if YF misses it
                sector = info.get("sector", "Unknown")
                for sec, tick_list in SECTOR_MAP.items():
                    if t in tick_list: sector = sec

                dcf = calc_dcf(stock, info, sector)
                rsi = _calc_rsi(hist['Close']) if not hist.empty else 50
                
                daily_vol = 0
                if not hist.empty:
                     daily_returns = hist['Close'].pct_change().dropna()
                     daily_vol = daily_returns.std() * 100
                
                news = fetch_news(t)
                avg_sent = np.mean([n['score'] for n in news]) if news else 0
                
                score = 50 + (dcf['margin_of_safety'] / 3) + (avg_sent * 10)
                if rsi < 35: score += 10
                if rsi > 70: score -= 10
                score = max(0, min(100, int(score)))

                data.append({
                    "Ticker": t,
                    "Sector": sector,
                    "Price": round(price, 2),
                    "intrinsic_value": dcf["intrinsic_value"],
                    "margin_of_safety": dcf["margin_of_safety"],
                    "growth_rate": dcf["growth_rate"],
                    "RSI": round(rsi, 1),
                    "Sentiment": round(avg_sent, 2),
                    "Oracle_Score": score,
                    "risk_level": _calc_risk_level(daily_vol),
                    "Articles_JSON": json.dumps(news)
                })
                time.sleep(0.1) 
            except: pass
        
        df = pd.DataFrame(data)
        if not df.empty: df.to_csv(CACHE_FILE, index=False)
        return df
