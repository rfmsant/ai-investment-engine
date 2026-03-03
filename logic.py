import yfinance as yf
import pandas as pd
import numpy as np
import requests
import xml.etree.ElementTree as ET
from textblob import TextBlob
from bs4 import BeautifulSoup
import os, json, time, warnings
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Dict, List
from openai import OpenAI

warnings.filterwarnings("ignore")
CACHE_FILE = "market_cache.csv"

# ===========================================================================
# 1. THE 500+ UNIVERSE
# ===========================================================================
SECTOR_MAP = {
    "AI_Tech": ["NVDA", "MSFT", "GOOGL", "AMD", "PLTR", "SMCI", "TSM", "META", "TSLA", "CRM", "ADBE", "NFLX", "AMZN", "ORCL", "IBM", "INTC", "QCOM", "AVGO", "NOW", "SNOW", "PANW", "CRWD", "FTNT", "ZS", "NET", "SHOP", "UBER", "DDOG", "TEAM", "WDAY", "MDB", "SQ", "PYPL", "AFRM", "HOOD", "COIN", "MSTR", "AI", "PATH", "IOT"],
    "War_Defense": ["LMT", "RTX", "NOC", "GD", "BA", "HII", "LHX", "TXT", "KTOS", "AVAV", "LDOS", "CACI", "SAIC", "BWXT", "CW", "PLTR", "OSK", "TDG", "HEI", "RHM.DE", "BAESY", "AIR.PA", "LDO.MI"],
    "Nuclear_Energy": ["CCJ", "NEE", "DUK", "SO", "AEP", "EXC", "PEG", "ETR", "D", "PCG", "CEG", "SMR", "FLR", "VUSTX", "UUUU", "LEU", "NXE", "DNN"],
    "Energy": ["XOM", "CVX", "SHEL", "TTE", "COP", "SLB", "EOG", "VLO", "MPC", "BP", "OXY", "KMI", "WMB", "PSX", "HES", "DVN", "FANG", "MRO", "HAL", "BKR", "TRGP", "CTRA", "APA", "OVV"],
    "Food_Agri": ["ADM", "CTVA", "CF", "MOS", "NTR", "FMC", "DE", "CAT", "CNH", "AGCO", "TSN", "BG", "ANDE", "SMG", "RKDA", "SEED"],
    "Food_Staples": ["KO", "PEP", "MDLZ", "GIS", "K", "CPB", "KHC", "HSY", "STZ", "CL", "PG", "COST", "WMT", "KR", "SYY", "TGT", "HD", "LOW", "EL", "CLX"],
    "Raw_Materials": ["FCX", "SCCO", "RIO", "VALE", "BHP", "AA", "NUE", "CLF", "X", "STLD", "RS", "CMC", "VMC", "MLM", "LIN", "APD", "SHW", "ECL", "DD", "ALB", "SQM"],
    "Gold_Minerals": ["NEM", "GOLD", "AEM", "KGC", "FNV", "WPM", "RGLD", "PAAS", "AU", "GFI", "HMY", "BTG", "SA", "NGD"],
    "Biochem_Pharma": ["LLY", "NVO", "JNJ", "PFE", "MRK", "ABBV", "BMY", "GILD", "AMGN", "BIIB", "REGN", "VRTX", "MRNA", "BNTX", "AZN", "SNY", "NVS", "ZTS", "ISRG", "SYK", "BSX", "MDT", "EW", "BAX"],
}

# Expand to 500: Adding S&P 500 mid-caps and others to fill the gap
EXTRA_TICKERS = ["SPY", "QQQ", "DIA", "IWM", "VUG", "VTI", "VOO", "VEA", "VWO", "BKNG", "MA", "V", "AXP", "JPM", "BAC", "WFC", "C", "GS", "MS", "BLK", "SCHW", "TROW", "UPS", "FDX", "UNP", "HON", "GE", "MMM", "CAT", "DE", "EMR", "ETN", "PH", "ITW", "NSC", "CSX", "WM", "RSG", "AMT", "PLD", "CCI", "EQIX", "DLR", "PSA", "O", "VICI", "SBAC", "WELL", "AVB", "EQR", "VTR", "BXP", "ARE", "MAA", "T", "VZ", "TMUS", "CMCSA", "CHTR", "DIS", "WBD", "PARA", "FOXA", "LYV", "MAR", "HLT", "RCL", "CCL", "NCLH", "MGM", "WYNN", "LVS", "DRI", "SBUX", "YUM", "MCD", "CMG", "LEN", "DHI", "PHM", "NVR", "TOL", "HD", "LOW", "TGT", "TJX", "ROST", "DLTR", "DG", "AZO", "ORLY", "GPC", "TSCO", "ULTA", "BBY", "EBAY", "ETSY", "MELI", "SE", "CPNG", "BABA", "JD", "PDD", "BIDU", "NTES", "NIU", "XPEV", "LI", "NIO", "BYDDY"]

FULL_TICKER_LIST = list(set([t for sublist in SECTOR_MAP.values() for t in sublist] + EXTRA_TICKERS))

# ===========================================================================
# 2. LOGIC FUNCTIONS
# ===========================================================================

def fetch_news(ticker: str) -> List[Dict]:
    articles = []
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/120.0.0.0 Safari/537.36"}
    try:
        url = f"https://news.google.com/rss/search?q={ticker}+stock+when:7d&hl=en-US&gl=US&ceid=US:en"
        response = requests.get(url, headers=headers, timeout=2)
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            for item in root.findall('./channel/item')[:3]:
                title = item.find('title').text
                desc_html = item.find('description').text if item.find('description') is not None else ""
                soup = BeautifulSoup(desc_html, "html.parser")
                summary = soup.get_text()[:150]
                blob = TextBlob(title)
                score = blob.sentiment.polarity
                articles.append({
                    "headline": title,
                    "link": item.find('link').text,
                    "score": round(score, 2),
                    "label": "Bullish" if score > 0.05 else "Bearish" if score < -0.05 else "Neutral",
                    "reason": summary if len(summary) > 20 else "Market tracking updates."
                })
    except: pass
    return articles

def generate_ai_report(ticker: str, data: dict, news: list, api_key: str) -> str:
    try:
        client = OpenAI(api_key=api_key)
        news_txt = "\n".join([f"- {n['headline']} (Brief: {n['reason']})" for n in news[:5]])
        prompt = f"""
        Act as a Lead Investment Analyst at The Motley Fool. 
        Deep Dive for {ticker}.
        Price: ${data.get('Price')} | Fair Value: ${data.get('intrinsic_value')} | Upside: {data.get('margin_of_safety')}%
        Risk: {data.get('risk_level')}
        News: {news_txt}
        
        Provide: 1. Numbers 2. Forecasts 3. News Context 4. Geopolitics 5. Risk Analysis 6. VERDICT.
        Use Apple-style clean formatting.
        """
        response = client.chat.completions.create(model="gpt-4o", messages=[{"role": "user", "content": prompt}])
        return response.choices[0].message.content
    except Exception as e: return f"Error: {str(e)}"

def calc_dcf(stock: yf.Ticker, info: dict, sector: str) -> Dict:
    res = {"intrinsic_value": 0.0, "margin_of_safety": 0.0, "growth_rate": 0.0}
    try:
        fcf = info.get("freeCashflow")
        if not fcf:
            cf_df = stock.cashflow
            if cf_df is not None and not cf_df.empty and "Free Cash Flow" in cf_df.index:
                fcf = cf_df.loc["Free Cash Flow"].iloc[0]
        if not fcf or fcf <= 0: return res
        
        rev_g = info.get("revenueGrowth", 0.05) or 0.05
        res["growth_rate"] = round(rev_g * 100, 1)
        discount, terminal, years = 0.10, 0.025, 5
        future_fcf = [fcf * ((1 + rev_g) ** i) for i in range(1, years + 1)]
        tv = (future_fcf[-1] * (1 + terminal)) / (discount - terminal)
        ev = sum([f / ((1 + discount) ** (i + 1)) for i, f in enumerate(future_fcf)]) + (tv / (1 + discount) ** years)
        equity = ev - info.get("totalDebt", 0) + info.get("totalCash", 0)
        shares = info.get("sharesOutstanding")
        if shares and shares > 0:
            iv = equity / shares
            cp = info.get("currentPrice", 1)
            if iv > cp * 4: iv = cp * 1.5
            res["intrinsic_value"] = round(iv, 2)
            res["margin_of_safety"] = round(((iv - cp) / cp) * 100, 1)
    except: pass
    return res

# ===========================================================================
# 3. MULTI-THREADED ENGINE
# ===========================================================================
class InvestmentEngine:
    def __init__(self):
        self.tickers = FULL_TICKER_LIST

    def process_ticker(self, t):
        """Single ticker worker for the thread pool"""
        try:
            stock = yf.Ticker(t)
            info = stock.info
            price = info.get("currentPrice")
            if not price: return None
            
            sector = "Unknown"
            for s_name, t_list in SECTOR_MAP.items():
                if t in t_list: sector = s_name
            
            dcf = calc_dcf(stock, info, sector)
            news = fetch_news(t)
            avg_sent = np.mean([n['score'] for n in news]) if news else 0
            
            # Simple Oracle Score
            score = 50 + (dcf['margin_of_safety'] / 3) + (avg_sent * 10)
            score = max(0, min(100, int(score)))
            
            hist = stock.history(period="1mo")
            vol = hist['Close'].pct_change().std() * 100 if not hist.empty else 0
            
            return {
                "Ticker": t, "Sector": sector, "Price": price,
                "intrinsic_value": dcf["intrinsic_value"],
                "margin_of_safety": dcf["margin_of_safety"],
                "growth_rate": dcf["growth_rate"],
                "Oracle_Score": score,
                "Sentiment": round(avg_sent, 2),
                "risk_level": "High" if vol > 3.0 else "Medium" if vol > 1.5 else "Low",
                "Articles_JSON": json.dumps(news)
            }
        except: return None

    def load_data(self):
        if os.path.exists(CACHE_FILE): return pd.read_csv(CACHE_FILE), True
        return pd.DataFrame(), False

    def fetch_market_data(self, progress_bar=None):
        results = []
        # Increase workers to 20 for maximum speed
        with ThreadPoolExecutor(max_workers=20) as executor:
            ticker_count = len(self.tickers)
            for i, result in enumerate(executor.map(self.process_ticker, self.tickers)):
                if result: results.append(result)
                if progress_bar:
                    progress_bar.progress((i+1)/ticker_count, text=f"Processing {i+1}/{ticker_count} assets...")
        
        df = pd.DataFrame(results)
        if not df.empty: df.to_csv(CACHE_FILE, index=False)
        return df
