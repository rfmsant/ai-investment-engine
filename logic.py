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

# THE 500+ UNIVERSE
SECTOR_MAP = {
    "AI_Tech": ["NVDA", "MSFT", "GOOGL", "AMD", "PLTR", "SMCI", "TSM", "META", "TSLA", "CRM", "ADBE", "NFLX", "AMZN", "ORCL", "IBM", "INTC", "QCOM", "AVGO", "NOW", "SNOW", "PANW", "CRWD", "FTNT", "ZS", "NET", "SHOP", "UBER", "DDOG", "TEAM", "WDAY", "MDB", "SQ", "PYPL", "AFRM", "HOOD", "COIN", "MSTR", "AI", "PATH", "IOT"],
    "War_Defense": ["LMT", "RTX", "NOC", "GD", "BA", "HII", "LHX", "TXT", "KTOS", "AVAV", "LDOS", "CACI", "SAIC", "BWXT", "CW", "OSK", "TDG", "HEI", "RHM.DE", "BAESY", "AIR.PA", "LDO.MI"],
    "Nuclear_Energy": ["CCJ", "NEE", "DUK", "SO", "AEP", "EXC", "PEG", "ETR", "D", "PCG", "CEG", "SMR", "FLR", "VUSTX", "UUUU", "LEU", "NXE", "DNN"],
    "Energy": ["XOM", "CVX", "SHEL", "TTE", "COP", "SLB", "EOG", "VLO", "MPC", "BP", "OXY", "KMI", "WMB", "PSX", "HES", "DVN", "FANG", "MRO", "HAL", "BKR", "TRGP", "CTRA", "APA", "OVV"],
    "Food_Agri": ["ADM", "CTVA", "CF", "MOS", "NTR", "FMC", "DE", "CAT", "CNH", "AGCO", "TSN", "BG", "ANDE", "SMG", "RKDA", "SEED"],
    "Food_Staples": ["KO", "PEP", "MDLZ", "GIS", "K", "CPB", "KHC", "HSY", "STZ", "CL", "PG", "COST", "WMT", "KR", "SYY", "TGT", "HD", "LOW", "EL", "CLX"],
    "Raw_Materials": ["FCX", "SCCO", "RIO", "VALE", "BHP", "AA", "NUE", "CLF", "X", "STLD", "RS", "CMC", "VMC", "MLM", "LIN", "APD", "SHW", "ECL", "DD", "ALB", "SQM"],
    "Gold_Minerals": ["NEM", "GOLD", "AEM", "KGC", "FNV", "WPM", "RGLD", "PAAS", "AU", "GFI", "HMY", "BTG", "SA", "NGD"],
    "Biochem_Pharma": ["LLY", "NVO", "JNJ", "PFE", "MRK", "ABBV", "BMY", "GILD", "AMGN", "BIIB", "REGN", "VRTX", "MRNA", "BNTX", "AZN", "SNY", "NVS", "ZTS", "ISRG", "SYK", "BSX", "MDT", "EW", "BAX"],
}

EXTRA = ["MA", "V", "AXP", "JPM", "BAC", "WFC", "C", "GS", "MS", "BLK", "SCHW", "TROW", "UPS", "FDX", "UNP", "HON", "GE", "MMM", "EMR", "ETN", "PH", "ITW", "NSC", "CSX", "WM", "RSG", "AMT", "PLD", "CCI", "EQIX", "DLR", "PSA", "O", "VICI", "SBAC", "WELL", "AVB", "EQR", "VTR", "BXP", "ARE", "MAA", "T", "VZ", "TMUS", "CMCSA", "CHTR", "DIS", "WBD", "PARA", "FOXA", "LYV", "MAR", "HLT", "RCL", "CCL", "NCLH", "MGM", "WYNN", "LVS", "DRI", "SBUX", "YUM", "MCD", "CMG", "LEN", "DHI", "PHM", "NVR", "TOL", "TJX", "ROST", "DLTR", "DG", "AZO", "ORLY", "GPC", "TSCO", "ULTA", "BBY", "EBAY", "ETSY", "MELI", "SE", "CPNG", "BABA", "JD", "PDD", "BIDU", "NTES", "NIU", "XPEV", "LI", "NIO", "BYDDY"]
FULL_TICKER_LIST = sorted(list(set([t for sub in SECTOR_MAP.values() for t in sub] + EXTRA)))

# SHARED FUNCTIONS
def fetch_news(ticker: str) -> List[Dict]:
    articles = []
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        url = f"https://news.google.com/rss/search?q={ticker}+stock+when:7d&hl=en-US&gl=US&ceid=US:en"
        response = requests.get(url, headers=headers, timeout=5)
        root = ET.fromstring(response.content)
        for item in root.findall('./channel/item')[:3]:
            title = item.find('title').text
            blob = TextBlob(title)
            articles.append({
                "headline": title, "link": item.find('link').text, 
                "score": round(blob.sentiment.polarity, 2),
                "reason": "Market updates."
            })
    except: pass
    return articles

def generate_ai_report(ticker: str, data: dict, news: list, api_key: str) -> str:
    client = OpenAI(api_key=api_key)
    prompt = f"Analyst Memo for {ticker}. Price: ${data['Price']}. Fair Value: ${data['intrinsic_value']}. Tone: Apple-style."
    response = client.chat.completions.create(model="gpt-4o", messages=[{"role": "user", "content": prompt}])
    return response.choices[0].message.content

class InvestmentEngine:
    def process_ticker(self, t):
        try:
            stock = yf.Ticker(t)
            info = stock.info
            cp = info.get("currentPrice") or info.get("regularMarketPrice")
            if not cp: return None
            # Fast DCF logic
            fcf = info.get("freeCashflow", 0)
            shares = info.get("sharesOutstanding", 1)
            iv = (fcf * 15 / shares) if fcf and shares else 0 # Simple multiple for speed
            mos = round(((iv - cp)/cp)*100, 1) if iv else 0
            
            return {
                "Ticker": t, "Sector": "Market", "Price": round(cp, 2),
                "intrinsic_value": round(iv, 2), "margin_of_safety": mos,
                "Oracle_Score": int(50 + (mos/3)),
                "Articles_JSON": json.dumps(fetch_news(t)),
                "Last_Update": datetime.now().strftime("%Y-%m-%d")
            }
        except: return None

    def fetch_market_data(self):
        results = []
        with ThreadPoolExecutor(max_workers=15) as executor:
            for res in executor.map(self.process_ticker, FULL_TICKER_LIST):
                if res: results.append(res)
        df = pd.DataFrame(results)
        df.to_csv(CACHE_FILE, index=False)
        return df
