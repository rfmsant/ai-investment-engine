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
        
    def analyze_multiple_headlines(self, headlines: List[str], ticker: str) -> Tuple[float, List[Dict]]:
        """Analyze multiple headlines and return overall sentiment + individual analysis"""
        if self.mock_mode or not self.openai_api_key:
            return self._mock_multi_analysis(headlines, ticker)
        else:
            return self._llm_multi_analysis(headlines, ticker)
    
    def _mock_multi_analysis(self, headlines: List[str], ticker: str) -> Tuple[float, List[Dict]]:
        """Sophisticated mock analysis for multiple headlines"""
        overall_sentiment = 0.0
        analyses = []
        
        for headline in headlines[:5]:  # Analyze top 5
            headline_lower = headline.lower()
            
            # Sentiment calculation
            bullish_signals = ['beat', 'exceed', 'growth', 'expansion', 'partnership', 'breakthrough', 'upgrade', 'buy', 'strong']
            bearish_signals = ['miss', 'decline', 'loss', 'cut', 'probe', 'lawsuit', 'downgrade', 'sell', 'weak']
            
            sentiment_score = 0.0
            sentiment_text = "Neutral"
            
            for word in bullish_signals:
                if word in headline_lower:
                    sentiment_score += 0.2
            
            for word in bearish_signals:
                if word in headline_lower:
                    sentiment_score -= 0.2
            
            sentiment_score = max(-1, min(1, sentiment_score))
            
            if sentiment_score > 0.2:
                sentiment_text = "🟢 Bullish"
            elif sentiment_score < -0.2:
                sentiment_text = "🔴 Bearish"
            else:
                sentiment_text = "⚪ Neutral"
            
            # Generate summary
            if 'earnings' in headline_lower or 'revenue' in headline_lower:
                summary = "Financial results update"
            elif 'partnership' in headline_lower or 'deal' in headline_lower:
                summary = "Strategic business development"
            elif 'upgrade' in headline_lower or 'downgrade' in headline_lower:
                summary = "Analyst recommendation change"
            else:
                summary = "General market news"
            
            analyses.append({
                'headline': headline,
                'sentiment': sentiment_text,
                'summary': summary,
                'score': sentiment_score
            })
            
            overall_sentiment += sentiment_score
        
        # Average sentiment
        overall_sentiment = overall_sentiment / len(headlines) if headlines else 0
        
        return overall_sentiment, analyses
    
    def _llm_multi_analysis(self, headlines: List[str], ticker: str) -> Tuple[float, List[Dict]]:
        """Real LLM API call for multiple headlines"""
        # Implementation for when LLM API is available
        # For now, fall back to mock
        return self._mock_multi_analysis(headlines, ticker)

class ValuationEngine:
    """Enhanced DCF-based intrinsic value calculator"""
    
    @staticmethod
    def calculate_dcf_value(ticker: str, default_discount_rate: float = 0.10) -> Dict:
        """Enhanced DCF calculation with better error handling"""
        try:
            stock = yf.Ticker(ticker)
            
            # Get financial data with retries
            financials = None
            cash_flow = None
            balance_sheet = None
            info = None
            
            for attempt in range(3):
                try:
                    financials = stock.financials
                    cash_flow = stock.cashflow
                    balance_sheet = stock.balance_sheet
                    info = stock.info
                    break
                except:
                    time.sleep(1)
                    continue
            
            if info is None:
                return {"intrinsic_value": 0, "margin_of_safety": 0, "dcf_confidence": "No Data"}
            
            # Try to get current price first
            current_price = info.get('currentPrice') or info.get('regularMarketPrice') or info.get('previousClose')
            if not current_price:
                return {"intrinsic_value": 0, "margin_of_safety": 0, "dcf_confidence": "No Price Data"}
            
            # Simplified DCF using available metrics from info
            market_cap = info.get('marketCap', 0)
            shares_outstanding = info.get('sharesOutstanding') or info.get('impliedSharesOutstanding')
            
            if not shares_outstanding or shares_outstanding == 0:
                return {"intrinsic_value": 0, "margin_of_safety": 0, "dcf_confidence": "No Shares Data"}
            
            # Use financial ratios for quick valuation
            pe_ratio = info.get('trailingPE')
            forward_pe = info.get('forwardPE')
            peg_ratio = info.get('pegRatio')
            book_value = info.get('bookValue', 0)
            
            # Simple valuation models
            intrinsic_estimates = []
            
            # Model 1: PE-based (if PE exists and is reasonable)
            if pe_ratio and 5 < pe_ratio < 50:
                industry_avg_pe = 18  # Conservative industry average
                pe_fair_value = current_price * (industry_avg_pe / pe_ratio)
                intrinsic_estimates.append(pe_fair_value)
            
            # Model 2: Forward PE
            if forward_pe and 5 < forward_pe < 40:
                forward_fair_value = current_price * (15 / forward_pe)  # 15x forward PE target
                intrinsic_estimates.append(forward_fair_value)
            
            # Model 3: Book value multiple (for asset-heavy companies)
            if book_value > 0:
                pb_ratio = current_price / book_value
                if pb_ratio > 0:
                    # Assume fair P/B of 2.0 for most companies
                    book_fair_value = book_value * 2.0
                    intrinsic_estimates.append(book_fair_value)
            
            # Model 4: Simple growth model
            revenue_growth = info.get('revenueGrowth')
            if revenue_growth and revenue_growth > 0:
                growth_multiple = min(2.0, 1 + revenue_growth)  # Cap at 2x
                growth_fair_value = current_price * growth_multiple
                intrinsic_estimates.append(growth_fair_value)
            
            # Calculate final intrinsic value
            if intrinsic_estimates:
                # Use median to avoid outliers
                intrinsic_value = np.median(intrinsic_estimates)
                confidence = "Medium"
                
                # Adjust confidence based on number of models
                if len(intrinsic_estimates) >= 3:
                    confidence = "High"
                elif len(intrinsic_estimates) == 1:
                    confidence = "Low"
            else:
                # Fallback: Conservative estimate based on current metrics
                intrinsic_value = current_price  # No premium/discount
                confidence = "Low"
            
            # Calculate margin of safety
            margin_of_safety = ((intrinsic_value - current_price) / current_price) * 100
            
            return {
                "intrinsic_value": round(intrinsic_value, 2),
                "margin_of_safety": round(margin_of_safety, 2),
                "dcf_confidence": confidence,
                "current_price": current_price,
                "pe_ratio": pe_ratio
            }
            
        except Exception as e:
            print(f"DCF calculation error for {ticker}: {e}")
            return {"intrinsic_value": 0, "margin_of_safety": 0, "dcf_confidence": "Error"}

class RiskManager:
    """Advanced risk management and position sizing"""
    
    @staticmethod
    def calculate_position_size(price: float, atr: float, account_size: float = 100000, risk_per_trade: float = 0.02) -> Dict:
        """Enhanced position sizing with better risk metrics"""
        if atr <= 0 or price <= 0:
            return {
                "position_size": 0, 
                "max_shares": 0, 
                "risk_level": "Unknown",
                "daily_volatility": 0,
                "stop_loss": 0
            }
        
        # Calculate volatility-adjusted position size
        daily_risk = atr / price  # ATR as percentage of price
        
        # Risk-adjusted position size
        risk_amount = account_size * risk_per_trade
        stop_loss_distance = atr * 2  # 2x ATR stop loss
        stop_loss_price = price - stop_loss_distance
        
        max_shares = int(risk_amount / stop_loss_distance) if stop_loss_distance > 0 else 0
        position_value = max_shares * price
        position_size = (position_value / account_size) * 100 if account_size > 0 else 0
        
        # Enhanced risk level classification
        if daily_risk < 0.015:  # Less than 1.5% daily volatility
            risk_level = "Low"
        elif daily_risk < 0.035:  # Less than 3.5% daily volatility
            risk_level = "Medium"
        elif daily_risk < 0.06:  # Less than 6% daily volatility
            risk_level = "High"
        else:
            risk_level = "Extreme"
        
        return {
            "position_size": round(position_size, 2),
            "max_shares": max_shares,
            "risk_level": risk_level,
            "daily_volatility": round(daily_risk * 100, 2),
            "stop_loss": round(stop_loss_price, 2)
        }

class InvestmentEngine:
    """Main institutional-grade investment engine"""
    
    def __init__(self):
        self.llm_analyst = LLMAnalyst(mock_mode=MOCK_MODE)
        self.valuation_engine = ValuationEngine()
        self.risk_manager = RiskManager()
        
        try:
            self.universe = pd.read_csv('universe.csv')
            self.tickers = self.universe['Ticker'].tolist()
            print(f"Loaded {len(self.tickers)} tickers for analysis")
        except Exception as e:
            print(f"Error loading universe: {e}")
            self.tickers = []
    
    def load_data(self):
        """Load cached data with validation"""
        if os.path.exists(CACHE_FILE):
            try:
                df = pd.read_csv(CACHE_FILE)
                if not df.empty and len(df) > 50:  # Ensure we have substantial data
                    return df, True
            except Exception as e:
                print(f"Cache load error: {e}")
        return pd.DataFrame(), False
    
    def get_advanced_news_analysis(self, ticker: str) -> Tuple[float, str, bool, str, List[Dict]]:
        """Enhanced news analysis with multiple headlines"""
        try:
            url = f"https://news.google.com/rss/search?q={ticker}+stock&hl=en-US&gl=US&ceid=US:en"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            response = requests.get(url, headers=headers, timeout=5)
            
            headlines = []
            geo_flag = False
            
            if response.status_code == 200:
                root = ET.fromstring(response.content)
                items = root.findall(".//item")
                
                for item in items[:5]:  # Get top 5 headlines
                    title_elem = item.find("title")
                    if title_elem is not None:
                        headline = title_elem.text.split(" - ")[0]
                        headlines.append(headline)
            
            if not headlines:
                headlines = ["No recent news found"]
            
            # Analyze multiple headlines
            overall_sentiment, news_analysis = self.llm_analyst.analyze_multiple_headlines(headlines, ticker)
            
            # Geopolitical risk detection
            risk_keywords = ["war", "china", "tariff", "sanction", "supply chain", "ban", "strike", "regulation", "investigation"]
            all_text = " ".join(headlines).lower()
            geo_flag = any(word in all_text for word in risk_keywords)
            
            # Primary headline for display
            primary_headline = headlines[0] if headlines else "No news available"
            
            # Create reasoning summary
            reasoning = f"Analyzed {len(news_analysis)} recent headlines. "
            positive_count = sum(1 for analysis in news_analysis if analysis['score'] > 0)
            negative_count = sum(1 for analysis in news_analysis if analysis['score'] < 0)
            
            if positive_count > negative_count:
                reasoning += f"Predominantly positive coverage ({positive_count} positive vs {negative_count} negative)."
            elif negative_count > positive_count:
                reasoning += f"Predominantly negative coverage ({negative_count} negative vs {positive_count} positive)."
            else:
                reasoning += "Mixed sentiment in recent coverage."
            
            return overall_sentiment, primary_headline, geo_flag, reasoning, news_analysis
            
        except Exception as e:
            print(f"News analysis error for {ticker}: {e}")
            return 0.0, "Connection Error", False, "Analysis unavailable", []
    
    def fetch_market_data(self, progress_bar=None):
        """Enhanced market data fetching for 500+ stocks"""
        results = []
        total = len(self.tickers)
        batch_size = 50  # Process in batches for better performance
        
        print(f"Starting analysis of {total} stocks...")
        
        for batch_start in range(0, total, batch_size):
            batch_end = min(batch_start + batch_size, total)
            batch_tickers = self.tickers[batch_start:batch_end]
            
            if progress_bar:
                progress = batch_end / total
                progress_bar.progress(progress, text=f"Analyzing batch {batch_start//batch_size + 1}/{(total-1)//batch_size + 1}...")
            
            # Download batch data
            try:
                history = yf.download(batch_tickers, period="3mo", group_by='ticker', progress=False, threads=True)
            except:
                history = {}
            
            # Process each ticker in the batch
            for ticker in batch_tickers:
                try:
                    # Get price data
                    if len(batch_tickers) == 1:
                        df = history
                    elif ticker in history.columns.get_level_values(0):
                        df = history[ticker]
                    else:
                        # Fallback for individual ticker
                        df = yf.download(ticker, period="3mo", progress=False)
                    
                    if df.empty or len(df) < 10:  # Need at least 10 days of data
                        continue
                    
                    # Technical indicators
                    df['RSI'] = ta.rsi(df['Close'], length=14)
                    df['ATR'] = ta.atr(df['High'], df['Low'], df['Close'], length=14)
                    
                    # Current values
                    current_price = df['Close'].iloc[-1]
                    current_rsi = df['RSI'].iloc[-1]
                    current_atr = df['ATR'].iloc[-1]
                    
                    if pd.isna(current_rsi) or pd.isna(current_atr):
                        continue
                    
                    # Volume analysis
                    avg_volume = df['Volume'].rolling(20).mean().iloc[-1]
                    current_volume = df['Volume'].iloc[-1]
                    vol_relative = current_volume / avg_volume if avg_volume > 0 else 1.0
                    
                    # Get fundamental data
                    try:
                        stock_info = yf.Ticker(ticker).info
                        pe_ratio = stock_info.get('trailingPE', 0)
                        roe = stock_info.get('returnOnEquity', 0)
                        profit_margin = stock_info.get('profitMargins', 0)
                        
                        # Convert percentages
                        roe = roe * 100 if roe else 0
                        profit_margin = profit_margin * 100 if profit_margin else 0
                    except:
                        pe_ratio, roe, profit_margin = 0, 0, 0
                    
                    # Enhanced DCF Valuation
                    dcf_data = self.valuation_engine.calculate_dcf_value(ticker)
                    
                    # Advanced news analysis
                    sentiment, headline, geo_risk, reasoning, news_details = self.get_advanced_news_analysis(ticker)
                    
                    # Position sizing
                    position_data = self.risk_manager.calculate_position_size(current_price, current_atr)
                    
                    # Sector information
                    sector_row = self.universe[self.universe['Ticker'] == ticker]
                    sector = sector_row['Sector'].values[0] if not sector_row.empty else "Unknown"
                    
                    # Compile comprehensive data
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
                        "LLM_Reasoning": reasoning,
                        "Geo_Risk": geo_risk,
                        "news_details": json.dumps(news_details),  # Store as JSON string
                        "position_size": position_data["position_size"],
                        "risk_level": position_data["risk_level"],
                        "daily_volatility": position_data["daily_volatility"],
                        "stop_loss": position_data["stop_loss"],
                        "Last_Updated": datetime.now().strftime("%Y-%m-%d %H:%M")
                    }
                    
                    # Calculate Oracle Score v2.0
                    data_point['Oracle_Score'] = self.calculate_oracle_score_v2(data_point)
                    
                    # Generate AI verdict
                    data_point['AI_Verdict'] = self.generate_institutional_verdict(data_point)
                    
                    results.append(data_point)
                    
                except Exception as e:
                    print(f"Error processing {ticker}: {e}")
                    continue
            
            # Small delay between batches
            time.sleep(0.5)
        
        # Create final DataFrame
        final_df = pd.DataFrame(results)
        
        print(f"Successfully analyzed {len(final_df)} stocks")
        
        # Save to cache
        try:
            final_df.to_csv(CACHE_FILE, index=False)
            print("Data saved to cache")
        except Exception as e:
            print(f"Cache save error: {e}")
        
        return final_df
    
    def calculate_oracle_score_v2(self, row: Dict) -> float:
        """Enhanced Oracle algorithm with institutional weightings"""
        
        # Valuation Score (40% weight) - Based on Margin of Safety
        valuation_score = 0
        margin = row['margin_of_safety']
        
        if margin > 50:
            valuation_score = 40  # Deeply undervalued
        elif margin > 25:
            valuation_score = 35  # Significantly undervalued
        elif margin > 10:
            valuation_score = 25  # Moderately undervalued
        elif margin > 0:
            valuation_score = 15  # Fair value
        elif margin > -15:
            valuation_score = 10  # Slightly overvalued
        else:
            valuation_score = 0  # Significantly overvalued
        
        # Technical Score (30% weight) - RSI + Volume
        rsi = row['RSI']
        if rsi < 25:
            rsi_score = 25  # Deeply oversold
        elif rsi < 35:
            rsi_score = 20  # Oversold
        elif rsi < 45:
            rsi_score = 15  # Neutral-oversold
        elif rsi < 55:
            rsi_score = 10  # Neutral
        elif rsi < 70:
            rsi_score = 5   # Neutral-overbought
        else:
            rsi_score = 0   # Overbought
        
        volume_score = min(5, row.get('Vol_Rel', 1) * 2)  # Volume confirmation
        technical_score = rsi_score + volume_score
        
        # Quality Score (20% weight) - ROE and margins
        roe = row['ROE']
        if roe > 25:
            quality_score = 20
        elif roe > 15:
            quality_score = 15
        elif roe > 10:
            quality_score = 10
        elif roe > 5:
            quality_score = 5
        else:
            quality_score = 0
        
        # Sentiment Score (10% weight) - LLM analysis
        sentiment = row['Sentiment']
        if sentiment > 0.3:
            sentiment_score = 10
        elif sentiment > 0.1:
            sentiment_score = 7
        elif sentiment > -0.1:
            sentiment_score = 5
        elif sentiment > -0.3:
            sentiment_score = 3
        else:
            sentiment_score = 0
        
        total_score = valuation_score + technical_score + quality_score + sentiment_score
        return round(min(100, max(0, total_score)), 1)
    
    def generate_institutional_verdict(self, row: Dict) -> str:
        """Generate sophisticated investment thesis"""
        verdict_parts = []
        
        # Valuation assessment
        margin = row['margin_of_safety']
        if margin > 30:
            verdict_parts.append("🟢 STRONG VALUE: Significant discount to intrinsic value")
        elif margin > 15:
            verdict_parts.append("🟡 GOOD VALUE: Moderate upside potential")
        elif margin > 0:
            verdict_parts.append("⚪ FAIR VALUE: Trading near intrinsic value")
        else:
            verdict_parts.append("🔴 OVERVALUED: Trading above intrinsic value")
        
        # Technical assessment
        rsi = row['RSI']
        if rsi < 30:
            verdict_parts.append("📈 OVERSOLD: Strong technical buying opportunity")
        elif rsi > 70:
            verdict_parts.append("📉 OVERBOUGHT: Technical caution warranted")
        
        # Quality assessment
        roe = row['ROE']
        if roe > 20:
            verdict_parts.append("💎 HIGH QUALITY: Excellent profitability")
        elif roe < 5:
            verdict_parts.append("⚠️ QUALITY CONCERN: Below-average returns")
        
        # Risk factors
        if row['risk_level'] == 'High':
            verdict_parts.append("⚡ HIGH VOLATILITY: Large position size risk")
        
        if row['Geo_Risk']:
            verdict_parts.append("🌍 MACRO RISK: Geopolitical exposure")
        
        return " | ".join(verdict_parts) if verdict_parts else "Neutral outlook"
