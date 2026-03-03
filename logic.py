import yfinance as yf
import pandas as pd
import numpy as np
import requests
import xml.etree.ElementTree as ET
from textblob import TextBlob
import os, json, time, warnings
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from openai import OpenAI

warnings.filterwarnings("ignore")

try:
    import pandas_ta as ta
    HAS_TA = True
except ImportError:
    HAS_TA = False

CACHE_FILE   = "market_cache.csv"
RATE_DELAY   = 0.5   # seconds between tickers

NO_DCF_SECTORS = {"ETF", "Commodities", "FX"}

# ===========================================================================
# UNIVERSE (Simplified for context, loads from CSV if exists)
# ===========================================================================
DEFAULT_UNIVERSE = [
    {"Ticker": "AAPL",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "MSFT",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "NVDA",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "GOOGL", "Sector": "Technology",  "Region": "US"},
    {"Ticker": "AMZN",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "TSLA",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "META",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "JPM",   "Sector": "Financials",  "Region": "US"},
    {"Ticker": "LLY",   "Sector": "Healthcare",  "Region": "US"},
    {"Ticker": "XOM",   "Sector": "Energy",      "Region": "US"},
]

# ===========================================================================
# HELPERS
# ===========================================================================

def _safe(val, default: float = 0.0) -> float:
    try:
        if val is None:
            return default
        f = float(val)
        return default if (f != f or not np.isfinite(f)) else f
    except Exception:
        return default

def _flatten_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [c[0] if isinstance(c, tuple) else str(c) for c in df.columns]
    return df

# ===========================================================================
# AI ANALYSIS ENGINE (NEW)
# ===========================================================================

def generate_ai_report(ticker: str, data_context: dict, news_context: list, api_key: str) -> str:
    """
    Generates a Motley Fool style analysis using OpenAI.
    """
    if not api_key:
        return "⚠️ OpenAI API Key missing. Please add it in the Sidebar."

    client = OpenAI(api_key=api_key)

    # Format news for the prompt
    news_summary = "\n".join([f"- {n['headline']} ({n['label']})" for n in news_context[:5]])
    
    prompt = f"""
    Act as a senior financial analyst for 'The Motley Fool'. 
    Perform a deep dive analysis on {ticker} based on the provided data.
    
    FINANCIAL DATA:
    - Price: ${data_context.get('Price')}
    - Fair Value (DCF): ${data_context.get('intrinsic_value')} (Margin of Safety: {data_context.get('margin_of_safety')}%)
    - PE Ratio: {data_context.get('PE')}
    - ROE: {data_context.get('ROE')}%
    - Revenue Growth (Est): {data_context.get('growth_rate')}%
    
    RECENT NEWS:
    {news_summary}
    
    OUTPUT FORMAT (Markdown):
    1. **The Elevator Pitch**: 2 sentences on what the company does and why it matters now.
    2. **The Bull Case (3 Bullet Points)**: Why could this double? Focus on moats, growth, and margins.
    3. **The Bear Case (3 Bullet Points)**: What could go wrong? Focus on valuation, competition, and debt.
    4. **The Verdict**: A definitive "Buy", "Hold", or "Sell" with a 1-sentence rationale.
    
    Tone: Professional, insightful, yet accessible (Motley Fool style).
    """

    try:
        response = client.chat.completions.create(
            model="gpt-4o", # or gpt-3.5-turbo
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=600
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"Error generating report: {str(e)}"

# ===========================================================================
# DCF ENGINE (FIXED)
# ===========================================================================

_EMPTY_DCF = {
    "intrinsic_value":  0.0,
    "margin_of_safety": 0.0,
    "dcf_confidence":   "N/A",
    "growth_rate":      0.0,
    "dcf_method":       "none",
}

def calc_dcf(stock: yf.Ticker, info: dict, sector: str = "") -> Dict:
    try:
        if sector in NO_DCF_SECTORS:
            return {**_EMPTY_DCF, "dcf_method": "skipped_etf"}

        fcf         = None
        growth_rate = None
        method      = "none"

        # --- Strategy 1: Calculate FCF Manually from Cashflow DF (Most Robust) ---
        # FCF = Operating Cash Flow - Capital Expenditures
        try:
            cf_df = stock.cashflow
            if cf_df is not None and not cf_df.empty:
                # Try explicit field first
                if "Free Cash Flow" in cf_df.index:
                    fcf_series = pd.to_numeric(cf_df.loc["Free Cash Flow"], errors='coerce').dropna()
                    if not fcf_series.empty:
                        fcf = float(fcf_series.iloc[0])
                        method = "cashflow_df_explicit"

                # Fallback: Manual Calculation (OCF + CapEx)
                # Note: CapEx is usually negative in yfinance, so we ADD it.
                if (fcf is None or fcf <= 0) and \
                   "Total Cash From Operating Activities" in cf_df.index and \
                   "Capital Expenditures" in cf_df.index:
                    
                    ocf = cf_df.loc["Total Cash From Operating Activities"].iloc[0]
                    capex = cf_df.loc["Capital Expenditures"].iloc[0]
                    
                    # Handle None/NaN
                    if pd.notna(ocf) and pd.notna(capex):
                        fcf_calc = ocf + capex 
                        if fcf_calc > 0:
                            fcf = fcf_calc
                            method = "cashflow_df_manual (OCF+CapEx)"

        except Exception as e:
            print(f"  [DCF] Cashflow DF error: {e}")

        # --- Strategy 2: info['freeCashflow'] ---
        if fcf is None or fcf <= 0:
            v = _safe(info.get("freeCashflow"))
            if v > 0:
                fcf    = v
                method = "info_freecashflow"

        # --- Strategy 3: info['operatingCashflow'] (Aggressive fallback) ---
        if fcf is None or fcf <= 0:
            v = _safe(info.get("operatingCashflow"))
            if v > 0:
                fcf    = v # Assume 0 capex as last resort
                method = "info_ocf_fallback"

        # If we still have no FCF, abort
        if fcf is None or fcf <= 0:
            return {**_EMPTY_DCF, "dcf_method": "no_fcf_found"}

        # --- Growth Rate Logic ---
        # Prioritize analyst estimates, fallback to historical
        if growth_rate is None:
            try:
                # Try to get 5y estimate
                growth_rate = info.get("pegRatio", 1.0) # Placeholder logic, real logic uses growth estimates
                # Better: Revenue Growth
                rev_g = _safe(info.get("revenueGrowth"),  0.05)
                ear_g = _safe(info.get("earningsGrowth"), 0.05)
                
                # Weighted average
                growth_rate = rev_g * 0.4 + ear_g * 0.6
                
                # Sanity Caps (-20% to +25%)
                growth_rate = max(-0.20, min(0.25, growth_rate))
                
                # Default if 0
                if growth_rate == 0.0:
                    growth_rate = 0.06
            except:
                growth_rate = 0.05

        # --- The Math (5-Year DCF) ---
        discount_rate = 0.10  # 10% Discount Rate (Standard)
        terminal_g    = 0.025 # 2.5% Terminal Growth (Inflation)
        
        # Adjust discount rate if growth is very high to be conservative
        if growth_rate > 0.15:
            discount_rate = 0.12

        # 1. Project Cash Flows
        proj   = [fcf * (1 + growth_rate) ** yr for yr in range(1, 6)]
        
        # 2. Terminal Value
        tv     = proj[-1] * (1 + terminal_g) / (discount_rate - terminal_g)
        
        # 3. Discount to Present
        pv_fcf = sum(f / (1 + discount_rate) ** yr for yr, f in enumerate(proj, 1))
        pv_tv  = tv / (1 + discount_rate) ** 5
        enterprise_value = pv_fcf + pv_tv

        # 4. Equity Bridge (EV -> Market Cap)
        debt = _safe(info.get("totalDebt"))
        cash = _safe(info.get("totalCash")) or _safe(info.get("cash"))
        
        # If debt data is missing, assume neutral impact to avoid crash
        equity_val = enterprise_value - (debt - cash)
        if equity_val <= 0:
            equity_val = enterprise_value # Fallback

        # 5. Per Share Value
        shares = _safe(info.get("sharesOutstanding") or info.get("impliedSharesOutstanding"))
        
        if shares <= 0:
            return {**_EMPTY_DCF, "dcf_method": "no_shares"}

        iv = equity_val / shares

        # Sanity check against current price (if IV is 100x price, something is wrong)
        cp  = _safe(info.get("currentPrice") or info.get("regularMarketPrice"))
        if cp > 0 and iv > cp * 5:
            iv = cp * 1.5 # Cap upside for safety if data looks weird
            method += " (capped)"

        mos = ((iv - cp) / cp * 100.0) if cp > 0 else 0.0

        return {
            "intrinsic_value":  round(iv, 2),
            "margin_of_safety": round(mos, 1),
            "dcf_confidence":   "Medium" if "manual" in method else "Low",
            "growth_rate":      round(growth_rate * 100.0, 1),
            "dcf_method":       method,
        }

    except Exception as e:
        print(f"  [DCF] exception: {e}")
        return {**_EMPTY_DCF, "dcf_method": f"error:{e}"}

# ===========================================================================
# TECHNICAL INDICATORS
# ===========================================================================

def _calc_rsi(close: pd.Series, length: int = 14) -> float:
    if HAS_TA:
        try:
            r = ta.rsi(close, length=length)
            if r is not None:
                clean = r.dropna()
                if len(clean):
                    return _safe(clean.iloc[-1], 50.0)
        except Exception:
            pass
    try:
        delta = close.diff().dropna()
        gain  = delta.clip(lower=0).ewm(com=length - 1, min_periods=length).mean()
        loss  = (-delta).clip(lower=0).ewm(com=length - 1, min_periods=length).mean()
        rs    = gain.iloc[-1] / loss.iloc[-1] if loss.iloc[-1] != 0 else 100
        return _safe(100 - 100 / (1 + rs), 50.0)
    except Exception:
        return 50.0

def _calc_atr(high: pd.Series, low: pd.Series, close: pd.Series, length: int = 14) -> float:
    try:
        if HAS_TA:
            r = ta.atr(high, low, close, length=length)
            if r is not None:
                return _safe(r.dropna().iloc[-1])
    except:
        pass
    return _safe(close.iloc[-1]) * 0.02

# ===========================================================================
# SENTIMENT & NEWS
# ===========================================================================

def _sentiment(headline: str) -> Tuple[float, str]:
    try:
        blob = TextBlob(headline)
        pol = blob.sentiment.polarity
        
        # Simple keyword override
        hl_lower = headline.lower()
        if "surge" in hl_lower or "jump" in hl_lower or "beat" in hl_lower:
            pol = max(pol, 0.3)
        if "plunge" in hl_lower or "drop" in hl_lower or "miss" in hl_lower:
            pol = min(pol, -0.3)
            
        reason = "Neutral"
        if pol > 0.1: reason = "Positive Sentiment"
        if pol < -0.1: reason = "Negative Sentiment"
        
        return round(pol, 2), reason
    except:
        return 0.0, "Neutral"

def fetch_news(ticker: str, max_items: int = 5) -> List[Dict]:
    articles = []
    try:
        # Use Yahoo Finance ticker news specifically
        stock = yf.Ticker(ticker)
        news_data = stock.news
        
        if news_data:
            for item in news_data[:max_items]:
                headline = item.get('title', '')
                score, reason = _sentiment(headline)
                
                # Check for simple geo risks
                geo_risk = any(x in headline.lower() for x in ['china', 'war', 'tariff', 'ban', 'sanction'])
                
                articles.append({
                    "headline": headline,
                    "score": score,
                    "label": "Bullish" if score > 0 else "Bearish" if score < 0 else "Neutral",
                    "reason": reason,
                    "geo_risk": geo_risk,
                    "pub_date": str(item.get('providerPublishTime', '')),
                    "link": item.get('link', '')
                })
    except Exception as e:
        print(f"[News] Error for {ticker}: {e}")

    if not articles:
        # Fallback dummy data so UI doesn't break
        articles.append({
            "headline": "No recent news available",
            "score": 0.0, "label": "Neutral",
            "reason": "N/A", "geo_risk": False, "pub_date": "", "link": ""
        })
    return articles

# ===========================================================================
# ORACLE SCORE
# ===========================================================================

def oracle_score(row: Dict) -> float:
    score = 50.0
    # Valuation (30%)
    mos = row.get("margin_of_safety", 0)
    if mos > 20: score += 15
    elif mos > 0: score += 5
    elif mos < -20: score -= 15
    
    # Technicals (30%)
    rsi = row.get("RSI", 50)
    if rsi < 30: score += 15 # Oversold is good for buying
    elif rsi > 70: score -= 10 # Overbought
    
    # Quality (20%)
    if row.get("ROE", 0) > 15: score += 10
    if row.get("profit_margin", 0) > 15: score += 10
    
    # Sentiment (20%)
    sent = row.get("Sentiment", 0)
    if sent > 0.2: score += 10
    elif sent < -0.2: score -= 10
    
    return max(0, min(100, score))

def build_verdict(row: Dict) -> str:
    mos = row.get("margin_of_safety", 0)
    score = row.get("Oracle_Score", 50)
    
    if score > 75: return "STRONG BUY 🚀"
    if score > 60: return "BUY 🟢"
    if score < 30: return "SELL 🔴"
    if mos > 30: return "VALUE PLAY 💰"
    return "HOLD 🟡"

def size_position(price, atr, account=100000):
    if price == 0: return {"position_size": 0, "risk_level": "N/A", "daily_volatility": 0}
    daily_vol = (atr/price) * 100
    risk = "High" if daily_vol > 3 else "Medium" if daily_vol > 1.5 else "Low"
    return {"position_size": 5.0 if risk=="Low" else 2.5, "risk_level": risk, "daily_volatility": round(daily_vol, 2)}

# ===========================================================================
# INVESTMENT ENGINE
# ===========================================================================

class InvestmentEngine:
    def __init__(self):
        try:
            u = pd.read_csv("universe.csv")
            if "Ticker" not in u.columns: raise ValueError
            self.universe = u
        except:
            self.universe = pd.DataFrame(DEFAULT_UNIVERSE)
        self.tickers = list(dict.fromkeys(self.universe["Ticker"].tolist()))

    def load_data(self) -> Tuple[pd.DataFrame, bool]:
        if os.path.exists(CACHE_FILE):
            try:
                df = pd.read_csv(CACHE_FILE)
                return df, True
            except: pass
        return pd.DataFrame(), False

    def fetch_market_data(self, progress_bar=None) -> pd.DataFrame:
        results = []
        tickers = self.tickers
        total = len(tickers)
        
        for i, ticker in enumerate(tickers):
            try:
                if progress_bar:
                    progress_bar.progress((i+1)/total, text=f"Analyzing {ticker}...")
                
                # 1. Fetch Data
                stock = yf.Ticker(ticker)
                
                # Try getting fast info first
                fast_info = stock.fast_info
                current_price = fast_info.last_price if fast_info else 0
                
                if current_price == 0: # Fallback
                    hist = stock.history(period="1mo")
                    if not hist.empty:
                        current_price = hist['Close'].iloc[-1]
                
                info = stock.info
                
                # 2. History for Technicals
                hist = stock.history(period="6mo")
                rsi = 50.0
                atr = 0.0
                vol_rel = 1.0
                if not hist.empty and len(hist) > 15:
                    close = hist['Close']
                    rsi = _calc_rsi(close)
                    atr = _calc_atr(hist['High'], hist['Low'], close)
                
                # 3. DCF
                sector = self.universe.loc[self.universe["Ticker"]==ticker, "Sector"].values[0] if "Sector" in self.universe.columns else "Unknown"
                dcf = calc_dcf(stock, info, sector)
                
                # 4. News
                news = fetch_news(ticker)
                avg_sent = float(np.mean([x['score'] for x in news])) if news else 0
                
                # 5. Position
                pos = size_position(current_price, atr)
                
                # 6. Compile
                record = {
                    "Ticker": ticker,
                    "Sector": sector,
                    "Price": round(current_price, 2),
                    "RSI": round(rsi, 1),
                    "PE": round(_safe(info.get('trailingPE')), 2),
                    "ROE": round(_safe(info.get('returnOnEquity'))*100, 2),
                    "profit_margin": round(_safe(info.get('profitMargins'))*100, 2),
                    "intrinsic_value": dcf['intrinsic_value'],
                    "margin_of_safety": dcf['margin_of_safety'],
                    "growth_rate": dcf['growth_rate'],
                    "dcf_method": dcf['dcf_method'],
                    "Sentiment": round(avg_sent, 2),
                    "Articles_JSON": json.dumps(news),
                    "risk_level": pos['risk_level'],
                    "daily_volatility": pos['daily_volatility'],
                    "position_size": pos['position_size'],
                    "Last_Updated": datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                
                record["Oracle_Score"] = oracle_score(record)
                record["AI_Verdict"] = build_verdict(record)
                
                results.append(record)
                time.sleep(RATE_DELAY)
                
            except Exception as e:
                print(f"Skipping {ticker}: {e}")
                continue
                
        df_out = pd.DataFrame(results)
        if not df_out.empty:
            df_out.to_csv(CACHE_FILE, index=False)
        return df_out
