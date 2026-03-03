import yfinance as yf
import pandas as pd
import pandas_ta as ta
import numpy as np
import requests
import xml.etree.ElementTree as ET
from textblob import TextBlob
import os
from datetime import datetime
import json
import time
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

# Configuration
CACHE_FILE = "market_cache.csv"
MOCK_MODE = True  # Set to False when you have LLM API keys

class LLMAnalyst:
    """Advanced LLM-powered news analysis with fallback mock mode"""
    
    def __init__(self, mock_mode: bool = True):
        self.mock_mode = mock_mode
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        
    def analyze_sentiment(self, headline: str, ticker: str) -> Tuple[float, str]:
        """Returns sentiment score (-1 to 1) and detailed reasoning"""
        if self.mock_mode or not self.openai_api_key:
            return self._mock_analysis(headline, ticker)
        else:
            return self._llm_analysis(headline, ticker)
    
    def _mock_analysis(self, headline: str, ticker: str) -> Tuple[float, str]:
        """Sophisticated mock analysis simulating LLM reasoning"""
        headline_lower = headline.lower()
        
        # Complex sentiment rules
        bullish_signals = ['beat', 'exceed', 'growth', 'expansion', 'partnership', 'breakthrough', 'upgrade']
        bearish_signals = ['miss', 'decline', 'loss', 'cut', 'probe', 'lawsuit', 'downgrade']
        
        sentiment_score = 0.0
        reasoning_parts = []
        
        # Sector-specific analysis
        sector_context = {
            'NVDA': 'AI leadership driving premium valuations',
            'XOM': 'Energy sector cyclical recovery',
            'LLY': 'Pharmaceutical innovation pipeline',
        }
        
        # Calculate sophisticated score
        for word in bullish_signals:
            if word in headline_lower:
                sentiment_score += 0.15
                reasoning_parts.append(f"Positive catalyst: {word}")
        
        for word in bearish_signals:
            if word in headline_lower:
                sentiment_score -= 0.15
                reasoning_parts.append(f"Risk factor: {word}")
        
        # Add sector context
        if ticker in sector_context:
            reasoning_parts.append(f"Sector view: {sector_context[ticker]}")
        
        # Normalize score
        sentiment_score = max(-1, min(1, sentiment_score))
        
        reasoning = f"Multi-factor analysis: {'; '.join(reasoning_parts) if reasoning_parts else 'Neutral sentiment detected'}"
        
        return sentiment_score, reasoning
    
    def _llm_analysis(self, headline: str, ticker: str) -> Tuple[float, str]:
        """Real LLM API call (OpenAI GPT-4)"""
        try:
            headers = {
                "Authorization": f"Bearer {self.openai_api_key}",
                "Content-Type": "application/json"
            }
            
            prompt = f"""
            Analyze this stock headline for {ticker}: "{headline}"
            
            Provide:
            1. Sentiment score from -1.0 (very bearish) to 1.0 (very bullish)
            2. Brief institutional-grade reasoning (max 100 words)
            
            Format: sentiment_score|reasoning
            """
            
            data = {
                "model": "gpt-4",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 150,
                "temperature": 0.3
            }
            
            response = requests.post("https://api.openai.com/v1/chat/completions", 
                                   headers=headers, json=data, timeout=10)
            
            if response.status_code == 200:
                result = response.json()['choices'][0]['message']['content']
                parts = result.split('|')
                score = float(parts[0].strip())
                reasoning = parts[1].strip() if len(parts) > 1 else "Analysis complete"
                return score, reasoning
            
        except Exception as e:
            print(f"LLM API error: {e}")
        
        # Fallback to TextBlob
        score = TextBlob(headline).sentiment.polarity
        return score, "LLM unavailable, using TextBlob fallback"

class ValuationEngine:
    """DCF-based intrinsic value calculator"""
    
    @staticmethod
    def calculate_dcf_value(ticker: str, default_discount_rate: float = 0.10) -> Dict:
        """Calculates DCF intrinsic value with error handling"""
        try:
            stock = yf.Ticker(ticker)
            
            # Get financial data
            cash_flow = stock.cashflow
            balance_sheet = stock.balance_sheet
            info = stock.info
            
            if cash_flow.empty:
                return {"intrinsic_value": 0, "margin_of_safety": 0, "dcf_confidence": "Low"}
            
            # Extract Free Cash Flow (Operating CF - CapEx)
            operating_cf = cash_flow.loc['Total Cash From Operating Activities'].dropna()
            capex = cash_flow.loc['Capital Expenditures'].dropna() if 'Capital Expenditures' in cash_flow.index else pd.Series()
            
            if len(operating_cf) < 3:
                return {"intrinsic_value": 0, "margin_of_safety": 0, "dcf_confidence": "Low"}
            
            # Calculate FCF
            if not capex.empty and len(capex) >= len(operating_cf):
                fcf = operating_cf + capex  # CapEx is negative
            else:
                fcf = operating_cf  # Use Operating CF as proxy
            
            # Calculate FCF growth rate (CAGR)
            if len(fcf) >= 3:
                years = len(fcf) - 1
                growth_rate = (fcf.iloc[0] / fcf.iloc[-1]) ** (1/years) - 1
                growth_rate = max(-0.5, min(0.5, growth_rate))  # Cap growth between -50% and 50%
            else:
                growth_rate = 0.05  # Default 5%
            
            # Terminal growth rate (conservative)
            terminal_growth = 0.025
            
            # Project 5-year FCF
            current_fcf = fcf.iloc[0]
            projected_fcf = []
            
            for year in range(1, 6):
                projected_fcf.append(current_fcf * (1 + growth_rate) ** year)
            
            # Terminal value
            terminal_fcf = projected_fcf[-1] * (1 + terminal_growth)
            terminal_value = terminal_fcf / (default_discount_rate - terminal_growth)
            
            # Present value calculation
            pv_fcf = sum([fcf / (1 + default_discount_rate) ** year for year, fcf in enumerate(projected_fcf, 1)])
            pv_terminal = terminal_value / (1 + default_discount_rate) ** 5
            
            enterprise_value = pv_fcf + pv_terminal
            
            # Get shares outstanding
            shares_outstanding = info.get('sharesOutstanding', info.get('impliedSharesOutstanding', 1))
            
            # Calculate intrinsic value per share
            intrinsic_value = enterprise_value / shares_outstanding
            current_price = info.get('currentPrice', info.get('regularMarketPrice', 0))
            
            # Margin of safety
            if current_price > 0:
                margin_of_safety = ((intrinsic_value - current_price) / current_price) * 100
            else:
                margin_of_safety = 0
            
            confidence = "High" if len(fcf) >= 5 else "Medium"
            
            return {
                "intrinsic_value": round(intrinsic_value, 2),
                "margin_of_safety": round(margin_of_safety, 2),
                "dcf_confidence": confidence,
                "current_fcf": current_fcf,
                "growth_rate": round(growth_rate * 100, 2)
            }
            
        except Exception as e:
            print(f"DCF calculation error for {ticker}: {e}")
            return {"intrinsic_value": 0, "margin_of_safety": 0, "dcf_confidence": "Low"}

class RiskManager:
    """Advanced risk management and position sizing"""
    
    @staticmethod
    def calculate_position_size(price: float, atr: float, account_size: float = 100000, risk_per_trade: float = 0.02) -> Dict:
        """Kelly Criterion inspired position sizing"""
        if atr <= 0:
            return {"position_size": 0, "max_shares": 0, "risk_level": "Unknown"}
        
        # Calculate volatility-adjusted position size
        daily_risk = atr / price  # ATR as percentage of price
        
        # Risk-adjusted position size
        risk_amount = account_size * risk_per_trade
        stop_loss_distance = atr * 2  # 2x ATR stop loss
        
        max_shares = int(risk_amount / stop_loss_distance)
        position_value = max_shares * price
        position_size = (position_value / account_size) * 100
        
        # Risk level classification
        if daily_risk < 0.02:
            risk_level = "Low"
        elif daily_risk < 0.05:
            risk_level = "Medium"
        else:
            risk_level = "High"
        
        return {
            "position_size": round(position_size, 2),
            "max_shares": max_shares,
            "risk_level": risk_level,
            "daily_volatility": round(daily_risk * 100, 2)
        }
    
    @staticmethod
    def calculate_correlation_matrix(df: pd.DataFrame) -> pd.DataFrame:
        """Calculate correlation matrix for systemic risk"""
        if 'Price' not in df.columns or len(df) < 2:
            return pd.DataFrame()
        
        # Create price matrix
        price_data = df.pivot_table(index='Ticker', values='Price').T
        
        # Calculate correlation (using returns would be better with historical data)
        correlation_matrix = price_data.corr()
        
        return correlation_matrix

class InvestmentEngine:
    """Main institutional-grade investment engine"""
    
    def __init__(self):
        self.llm_analyst = LLMAnalyst(mock_mode=MOCK_MODE)
        self.valuation_engine = ValuationEngine()
        self.risk_manager = RiskManager()
        
        try:
            self.universe = pd.read_csv('universe.csv')
            self.tickers = self.universe['Ticker'].tolist()
        except:
            self.tickers = []
    
    def load_data(self):
        """Load cached data with validation"""
        if os.path.exists(CACHE_FILE):
            try:
                df = pd.read_csv(CACHE_FILE)
                return df, True
            except Exception as e:
                print(f"Cache load error: {e}")
        return pd.DataFrame(), False
    
    def get_advanced_news_analysis(self, ticker: str) -> Tuple[float, str, bool, str]:
        """Enhanced news analysis with LLM integration"""
        try:
            url = f"https://news.google.com/rss/search?q={ticker}+stock+news&hl=en-US&gl=US&ceid=US:en"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            response = requests.get(url, headers=headers, timeout=5)
            
            headline = "No news found"
            score = 0.0
            geo_flag = False
            reasoning = "No analysis available"
            
            if response.status_code == 200:
                root = ET.fromstring(response.content)
                item = root.find(".//item")
                if item is not None:
                    headline = item.find("title").text.split(" - ")[0]
                    
                    # LLM Analysis
                    score, reasoning = self.llm_analyst.analyze_sentiment(headline, ticker)
                    
                    # Geopolitical risk detection
                    risk_keywords = ["war", "china", "tariff", "sanction", "supply", "ban", "strike", "geopolitic", "regulation"]
                    geo_flag = any(word in headline.lower() for word in risk_keywords)
            
            return score, headline, geo_flag, reasoning
            
        except Exception as e:
            print(f"News analysis error for {ticker}: {e}")
            return 0.0, "Connection Error", False, "Analysis unavailable"
    
    def get_alternative_data(self, ticker: str) -> Dict:
        """Fetch alternative data: insider trading and short interest"""
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # Short interest data
            short_percent = info.get('shortPercentOfFloat', 0)
            if short_percent:
                short_percent = short_percent * 100  # Convert to percentage
            
            # Mock insider trading (in production, would use OpenInsider API)
            insider_sentiment = "Neutral"
            if short_percent > 20:
                insider_sentiment = "Bearish (High Short Interest)"
            elif short_percent < 5:
                insider_sentiment = "Bullish (Low Short Interest)"
            
            return {
                "short_interest": round(short_percent, 2),
                "insider_sentiment": insider_sentiment
            }
            
        except Exception as e:
            print(f"Alternative data error for {ticker}: {e}")
            return {"short_interest": 0, "insider_sentiment": "Unknown"}
    
    def calculate_oracle_score_v2(self, row: Dict) -> float:
        """Advanced Oracle algorithm with institutional weightings"""
        
        # Valuation Score (40% weight) - Based on Margin of Safety
        valuation_score = 0
        if row['margin_of_safety'] > 50:
            valuation_score = 40  # Deeply undervalued
        elif row['margin_of_safety'] > 20:
            valuation_score = 30  # Undervalued
        elif row['margin_of_safety'] > 0:
            valuation_score = 20  # Fair value
        else:
            valuation_score = max(0, 20 + row['margin_of_safety'] * 0.2)  # Overvalued penalty
        
        # Technical Score (30% weight) - RSI + Volume
        rsi_score = max(0, (70 - row['RSI']) * 0.4)  # Lower RSI is better
        volume_score = min(10, row.get('Vol_Rel', 1) * 5)  # Volume breakout bonus
        technical_score = rsi_score + volume_score
        
        # Quality Score (20% weight) - ROE and margins
        quality_score = min(20, row['ROE'] * 0.5)
        if row.get('profit_margin', 0) > 20:
            quality_score += 5  # Bonus for high margins
        
        # Sentiment Score (10% weight) - LLM analysis
        sentiment_score = max(0, row['Sentiment'] * 10)
        
        total_score = valuation_score + technical_score + quality_score + sentiment_score
        return round(min(100, max(0, total_score)), 1)
    
    def fetch_market_data(self, progress_bar=None):
        """Comprehensive market data fetching with institutional metrics"""
        results = []
        total = len(self.tickers)
        
        # Bulk download for efficiency
        try:
            history = yf.download(self.tickers, period="6mo", group_by='ticker', progress=False)
        except:
            history = {}
        
        for i, ticker in enumerate(self.tickers):
            try:
                if progress_bar:
                    progress_bar.progress((i + 1) / total, text=f"Analyzing {ticker}...")
                
                # Price data
                if ticker in history and not history[ticker].empty:
                    df = history[ticker].copy()
                else:
                    # Fallback for individual ticker
                    df = yf.download(ticker, period="6mo", progress=False)
                
                if df.empty:
                    continue
                
                # Technical indicators
                df['RSI'] = ta.rsi(df['Close'], length=14)
                df['ATR'] = ta.atr(df['High'], df['Low'], df['Close'], length=14)
                
                current_price = df['Close'].iloc[-1]
                current_rsi = df['RSI'].iloc[-1]
                current_atr = df['ATR'].iloc[-1]
                
                # Volume analysis
                avg_volume = df['Volume'].rolling(20).mean().iloc[-1]
                current_volume = df['Volume'].iloc[-1]
                vol_relative = current_volume / avg_volume if avg_volume > 0 else 1.0
                
                # Fundamental data
                try:
                    stock = yf.Ticker(ticker)
                    info = stock.info
                    pe_ratio = info.get('trailingPE', 0)
                    roe = info.get('returnOnEquity', 0) * 100 if info.get('returnOnEquity') else 0
                    profit_margin = info.get('profitMargins', 0) * 100 if info.get('profitMargins') else 0
                except:
                    pe_ratio, roe, profit_margin = 0, 0, 0
                
                # DCF Valuation
                dcf_data = self.valuation_engine.calculate_dcf_value(ticker)
                
                # Advanced news analysis
                sentiment, headline, geo_risk, llm_reasoning = self.get_advanced_news_analysis(ticker)
                
                # Alternative data
                alt_data = self.get_alternative_data(ticker)
                
                # Position sizing
                position_data = self.risk_manager.calculate_position_size(current_price, current_atr)
                
                # Sector information
                sector_row = self.universe[self.universe['Ticker'] == ticker]
                sector = sector_row['Sector'].values[0] if not sector_row.empty else "Unknown"
                
                # Compile all data
                data_point = {
                    "Ticker": ticker,
                    "Sector": sector,
                    "Price": round(current_price, 2),
                    "RSI": round(current_rsi, 2),
                    "ATR": round(current_atr, 2),
                    "Vol_Rel": round(vol_relative, 2),
                    "PE": round(pe_ratio, 2) if pe_ratio else 0,
                    "ROE": round(roe, 2),
                    "profit_margin": round(profit_margin, 2),
                    "intrinsic_value": dcf_data["intrinsic_value"],
                    "margin_of_safety": dcf_data["margin_of_safety"],
                    "dcf_confidence": dcf_data["dcf_confidence"],
                    "Sentiment": round(sentiment, 3),
                    "Headline": headline,
                    "LLM_Reasoning": llm_reasoning,
                    "Geo_Risk": geo_risk,
                    "short_interest": alt_data["short_interest"],
                    "insider_sentiment": alt_data["insider_sentiment"],
                    "position_size": position_data["position_size"],
                    "risk_level": position_data["risk_level"],
                    "daily_volatility": position_data["daily_volatility"],
                    "Last_Updated": datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                
                # Calculate Oracle Score v2.0
                data_point['Oracle_Score'] = self.calculate_oracle_score_v2(data_point)
                
                # Generate AI verdict
                data_point['AI_Verdict'] = self.generate_institutional_verdict(data_point)
                
                results.append(data_point)
                
                # Rate limiting
                time.sleep(0.1)
                
            except Exception as e:
                print(f"Error processing {ticker}: {e}")
                continue
        
        # Create final DataFrame
        final_df = pd.DataFrame(results)
        
        # Save to cache
        try:
            final_df.to_csv(CACHE_FILE, index=False)
        except Exception as e:
            print(f"Cache save error: {e}")
        
        return final_df
    
    def generate_institutional_verdict(self, row: Dict) -> str:
        """Generate sophisticated investment thesis"""
        verdict_parts = []
        
        # Valuation assessment
        if row['margin_of_safety'] > 30:
            verdict_parts.append("🟢 STRONG VALUE: Trading at significant discount to intrinsic value")
        elif row['margin_of_safety'] > 10:
            verdict_parts.append("🟡 MODERATE VALUE: Some upside to fair value")
        elif row['margin_of_safety'] < -20:
            verdict_parts.append("🔴 OVERVALUED: Trading well above intrinsic value")
        
        # Technical assessment
        if row['RSI'] < 30 and row['Vol_Rel'] > 1.5:
            verdict_parts.append("📈 TECHNICAL SETUP: Oversold with volume confirmation")
        elif row['RSI'] > 70:
            verdict_parts.append("⚠️ TECHNICAL RISK: Overbought conditions")
        
        # Quality assessment
        if row['ROE'] > 20 and row['profit_margin'] > 15:
            verdict_parts.append("💎 HIGH QUALITY: Strong profitability metrics")
        elif row['ROE'] < 10:
            verdict_parts.append("⚡ QUALITY CONCERN: Below-average returns")
        
        # Risk factors
        if row['short_interest'] > 15:
            verdict_parts.append(f"🐻 HIGH SHORT INTEREST: {row['short_interest']}% of float")
        
        if row['Geo_Risk']:
            verdict_parts.append("🌍 MACRO RISK: Geopolitical exposure detected")
        
        # LLM insight
        if row['LLM_Reasoning']:
            verdict_parts.append(f"🤖 AI ANALYSIS: {row['LLM_Reasoning'][:100]}...")
        
        return " | ".join(verdict_parts) if verdict_parts else "Neutral outlook"
