import yfinance as yf
import pandas as pd
import pandas_ta as ta
import requests
import xml.etree.ElementTree as ET
from textblob import TextBlob
import os
from datetime import datetime

CACHE_FILE = "market_cache.csv"

class InvestmentEngine:
    def __init__(self):
        try:
            self.universe = pd.read_csv('universe.csv')
            self.tickers = self.universe['Ticker'].tolist()
        except:
            self.tickers = []

    def load_data(self):
        """ Loads data from local cache if it exists. """
        if os.path.exists(CACHE_FILE):
            try:
                df = pd.read_csv(CACHE_FILE)
                return df, True
            except:
                pass
        return pd.DataFrame(), False

    def get_google_news_analysis(self, ticker):
        """ Scrapes Google News and checks for Geo-Political Keywords """
        try:
            url = f"https://news.google.com/rss/search?q={ticker}+stock+news&hl=en-US&gl=US&ceid=US:en"
            headers = {"User-Agent": "Mozilla/5.0"}
            response = requests.get(url, headers=headers, timeout=2)
            
            headline = "No news found"
            score = 0.0
            geo_flag = False
            
            if response.status_code == 200:
                root = ET.fromstring(response.content)
                item = root.find(".//item")
                if item is not None:
                    headline = item.find("title").text.split(" - ")[0]
                    score = round(TextBlob(headline).sentiment.polarity, 2)
                    
                    # Geo-Political/Macro Keyword Check
                    risk_keywords = ["war", "china", "tariff", "sanction", "supply", "ban", "strike", "geopolitic"]
                    if any(word in headline.lower() for word in risk_keywords):
                        geo_flag = True
            
            return score, headline, geo_flag
        except:
            return 0.0, "Connection Error", False

    def generate_ai_verdict(self, row):
        """ Generates a text-based analysis based on data points """
        verdict = []
        
        # 1. Technical Verdict
        if row['RSI'] < 30: verdict.append("🟢 STRONG BUY: Asset is heavily oversold (RSI < 30).")
        elif row['RSI'] < 45: verdict.append("🟢 BUY: Asset is in a dip/pullback zone.")
        elif row['RSI'] > 70: verdict.append("🔴 SELL/WAIT: Asset is overbought/expensive.")
        else: verdict.append("⚪ HOLD: Price action is neutral.")
        
        # 2. Fundamental Verdict
        if row['PE'] > 0 and row['PE'] < 20: verdict.append("✅ VALUATION: Stock is cheap (PE < 20).")
        if row['ROE'] > 20: verdict.append("💎 QUALITY: High efficiency compounder (ROE > 20%).")
        
        # 3. Sentiment Verdict
        if row['Sentiment'] > 0.3: verdict.append("🚀 NEWS: Sentiment is very positive.")
        elif row['Sentiment'] < -0.2: verdict.append("⚠️ NEWS: Sentiment is negative/fearful.")
        
        if row['Geo_Risk']: verdict.append("🌍 MACRO ALERT: Recent news mentions Geopolitical risks.")
        
        return " ".join(verdict)

    def fetch_market_data(self, progress_bar=None):
        results = []
        total = len(self.tickers)
        
        # Bulk Download for Speed
        history = yf.download(self.tickers, period="6mo", group_by='ticker', progress=False)
        
        for i, ticker in enumerate(self.tickers):
            try:
                if progress_bar:
                    progress_bar.progress((i + 1) / total, text=f"Analyzing {ticker}...")
                
                df = history[ticker].copy()
                if df.empty: continue
                
                # Metrics
                df['RSI'] = ta.rsi(df['Close'], length=14)
                avg_vol = df['Volume'].rolling(20).mean().iloc[-1]
                curr_vol = df['Volume'].iloc[-1]
                
                # Fundamentals
                try:
                    stock = yf.Ticker(ticker)
                    info = stock.info
                    pe = info.get('trailingPE', 0)
                    roe = info.get('returnOnEquity', 0)
                except:
                    pe = 0; roe = 0
                
                # News & AI
                sent, headline, geo_flag = self.get_google_news_analysis(ticker)
                
                # Sector
                sector_row = self.universe[self.universe['Ticker'] == ticker]
                sector = sector_row['Sector'].values[0] if not sector_row.empty else "Unknown"

                # Data Dictionary
                data_point = {
                    "Ticker": ticker,
                    "Sector": sector,
                    "Price": round(df['Close'].iloc[-1], 2),
                    "RSI": round(df['RSI'].iloc[-1], 2),
                    "Vol_Rel": round(curr_vol / avg_vol, 2) if avg_vol > 0 else 1.0,
                    "PE": round(pe, 2) if pe else 0,
                    "ROE": round(roe * 100, 2) if roe else 0,
                    "Sentiment": sent,
                    "Headline": headline,
                    "Geo_Risk": geo_flag,
                    "Last_Updated": datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                
                # Generate AI Verdict
                data_point['AI_Verdict'] = self.generate_ai_verdict(data_point)
                
                # Calculate Oracle Score (0 to 100)
                # Logic: Low RSI is good (40 pts), High Sentiment is good (30 pts), High ROE is good (30 pts)
                # We invert RSI (100 - RSI) because lower is better for buying
                rsi_score = max(0, (70 - data_point['RSI'])) 
                sent_score = max(0, data_point['Sentiment'] * 20) 
                roe_score = min(30, data_point['ROE'])
                data_point['Oracle_Score'] = round(rsi_score + sent_score + roe_score, 1)

                results.append(data_point)
                
            except Exception as e:
                print(f"Skip {ticker}: {e}")
        
        final_df = pd.DataFrame(results)
        final_df.to_csv(CACHE_FILE, index=False)
        return final_df