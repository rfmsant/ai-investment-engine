import yfinance as yf
import pandas as pd
import numpy as np
import json
import os
import time
import warnings
from textblob import TextBlob
from datetime import datetime
from typing import Dict, List, Tuple
from openai import OpenAI

warnings.filterwarnings("ignore")

CACHE_FILE = "market_cache.csv"
NO_DCF_SECTORS = {"ETF", "Commodities", "FX"}

# ===========================================================================
# 1. AI GENERATION LOGIC
# ===========================================================================
def generate_ai_report(ticker: str, data: dict, news: list, api_key: str) -> str:
    """
    Connects to OpenAI to generate a Motley Fool style investment memo.
    """
    try:
        client = OpenAI(api_key=api_key)
        
        # Prepare context for the LLM
        news_summary = "\n".join([f"- {n['headline']} ({n['label']})" for n in news[:5]])
        
        prompt = f"""
        Act as a senior financial analyst for 'The Motley Fool'. 
        Analyze {ticker} based on the data below.
        
        FINANCIALS:
        - Price: ${data.get('Price')}
        - DCF Fair Value: ${data.get('intrinsic_value')} (Upside: {data.get('margin_of_safety')}%)
        - Growth Estimate: {data.get('growth_rate')}%
        - RSI: {data.get('RSI')}
        - Market Sentiment Score: {data.get('Sentiment')}
        
        RECENT NEWS HEADLINES:
        {news_summary}
        
        OUTPUT FORMAT (Markdown):
        1. **The Elevator Pitch**: 2 sentences on the business model.
        2. **The Bull Case** (3 bullet points): Focus on moats, growth, and margins.
        3. **The Bear Case** (3 bullet points): Focus on valuation, debt, and risks.
        4. **The Verdict**: A definitive "BUY", "HOLD", or "SELL" with a 1-sentence rationale.
        
        Tone: Professional, insightful, direct.
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
# 2. ROBUST DCF LOGIC
# ===========================================================================
def calc_dcf(stock: yf.Ticker, info: dict, sector: str) -> Dict:
    res = {
        "intrinsic_value": 0.0, 
        "margin_of_safety": 0.0, 
        "growth_rate": 0.0, 
        "dcf_method": "none"
    }
    
    if sector in NO_DCF_SECTORS: 
        res["dcf_method"] = "skipped (sector)"
        return res
    
    try:
        # A. GET FREE CASH FLOW (FCF)
        # Priority: 1. Explicit Field -> 2. Calc (OCF + CapEx) -> 3. Fallback
        fcf = 0
        cf_df = stock.cashflow
        
        if cf_df is not None and not cf_df.empty:
            # Method 1: Explicit
            if "Free Cash Flow" in cf_df.index:
                val = pd.to_numeric(cf_df.loc["Free Cash Flow"], errors='coerce').iloc[0]
                if val > 0:
                    fcf = val
                    res["dcf_method"] = "explicit_fcf"
            
            # Method 2: Manual (OCF + CapEx)
            # Note: CapEx is usually negative in yfinance, so we add it.
            if fcf == 0 and "Total Cash From Operating Activities" in cf_df.index:
                ocf = cf_df.loc["Total Cash From Operating Activities"].iloc[0]
                capex = 0
                if "Capital Expenditures" in cf_df.index:
                    capex = cf_df.loc["Capital Expenditures"].iloc[0]
                
                fcf_calc = ocf + capex
                if fcf_calc > 0:
                    fcf = fcf_calc
                    res["dcf_method"] = "manual_fcf"

        # Method 3: Info dict fallback
        if fcf == 0:
            fcf = info.get("freeCashflow", 0)
            if fcf > 0: res["dcf_method"] = "info_fallback"

        if fcf <= 0:
            res["dcf_method"] = "failed (no positive FCF)"
            return res

        # B. GET GROWTH RATE
        # Priority: Revenue Growth -> Earnings Growth -> Default 5%
        rev_g = info.get("revenueGrowth", 0.05) or 0.05
        res["growth_rate"] = round(rev_g * 100, 1)
        
        # C. DISCOUNTING
        discount_rate = 0.10
        terminal_rate = 0.025
        years = 5
        
        future_fcf = []
        for i in range(1, years + 1):
            future_fcf.append(fcf * ((1 + rev_g) ** i))
            
        terminal_value = (future_fcf[-1] * (1 + terminal_rate)) / (discount_rate - terminal_rate)
        
        dcf_val = sum([val / ((1 + discount_rate) ** (i + 1)) for i, val in enumerate(future_fcf)])
        pv_terminal = terminal_value / ((1 + discount_rate) ** years)
        
        enterprise_value = dcf_val + pv_terminal
        
        # D. EQUITY BRIDGE
        debt = info.get("totalDebt", 0) or 0
        cash = info.get("totalCash", 0) or 0
        equity_value = enterprise_value - debt + cash
        
        # E. PER SHARE
        shares = info.get("sharesOutstanding", 0)
        if not shares:
            res["dcf_method"] = "failed (no shares)"
            return res
            
        fair_value = equity_value / shares
        current_price = info.get("currentPrice", 0) or 1
        
        # Sanity Cap (If IV > 5x price, cap it to avoid bad data outliers)
        if fair_value > current_price * 5:
            fair_value = current_price * 1.5
        
        if fair_value > 0:
            res["intrinsic_value"] = round(fair_value, 2)
            res["margin_of_safety"] = round(((fair_value - current_price) / current_price) * 100, 1)

        return res

    except Exception as e:
        print(f"DCF Error: {e}")
        res["dcf_method"] = "error"
        return res

# ===========================================================================
# 3. HELPERS & ENGINE
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

def fetch_news(ticker: str) -> List[Dict]:
    articles = []
    try:
        s = yf.Ticker(ticker)
        for item in s.news[:5]:
            blob = TextBlob(item['title'])
            score = blob.sentiment.polarity
            articles.append({
                "headline": item['title'],
                "link": item['link'],
                "score": round(score, 2),
                "label": "Bullish" if score > 0.1 else "Bearish" if score < -0.1 else "Neutral",
                "reason": "AI Sentiment Analysis"
            })
    except:
        pass
    return articles

class InvestmentEngine:
    def __init__(self):
        # Default universe
        self.tickers = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AMD", "INTC", "NFLX", "JPM", "BAC", "LLY", "XOM", "CVX", "KO", "PEP", "COST", "WMT", "DIS"]

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
                
                # Metrics
                price = info.get("currentPrice", 0)
                if price == 0 and not hist.empty: price = hist['Close'].iloc[-1]
                
                dcf = calc_dcf(stock, info, info.get("sector", ""))
                rsi = _calc_rsi(hist['Close']) if not hist.empty else 50
                news = fetch_news(t)
                avg_sent = np.mean([n['score'] for n in news]) if news else 0
                
                # Scoring
                score = 50
                if dcf['margin_of_safety'] > 20: score += 20
                if dcf['margin_of_safety'] < -20: score -= 20
                if rsi < 30: score += 10
                if avg_sent > 0.2: score += 10
                score = max(0, min(100, score))
                
                verdict = "Hold"
                if score > 75: verdict = "Strong Buy"
                elif score < 30: verdict = "Sell"

                record = {
                    "Ticker": t,
                    "Sector": info.get("sector", "N/A"),
                    "Price": round(price, 2),
                    "intrinsic_value": dcf["intrinsic_value"],
                    "margin_of_safety": dcf["margin_of_safety"],
                    "growth_rate": dcf["growth_rate"],
                    "RSI": round(rsi, 1),
                    "Sentiment": round(avg_sent, 2),
                    "Oracle_Score": int(score),
                    "AI_Verdict": verdict,
                    "Articles_JSON": json.dumps(news)
                }
                data.append(record)
                time.sleep(0.5) # Prevent rate limit
            except Exception as e:
                print(f"Skipping {t}: {e}")
                
        df = pd.DataFrame(data)
        if not df.empty:
            df.to_csv(CACHE_FILE, index=False)
        return df
