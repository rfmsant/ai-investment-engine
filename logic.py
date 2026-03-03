"""
logic.py  –  Apex Markets Investment Engine v3.1
Fixes: DCF cash-flow key detection, Articles_JSON CSV round-trip,
       ATR/RSI Series flattening from bulk download, Oracle scoring.
"""
import yfinance as yf
import pandas as pd
import pandas_ta as ta
import numpy as np
import requests
import xml.etree.ElementTree as ET
from textblob import TextBlob
import os, json, time, warnings
from datetime import datetime
from typing import Dict, List, Tuple, Optional

warnings.filterwarnings('ignore')

CACHE_FILE = "market_cache.csv"
MOCK_MODE  = True   # set False + OPENAI_API_KEY env var to use GPT-4

# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL STOCK UNIVERSE  (200+ reliable tickers to start)
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_UNIVERSE = [
    # US Technology
    {"Ticker":"AAPL",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"MSFT",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"NVDA",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"GOOGL", "Sector":"Technology",  "Region":"US"},
    {"Ticker":"META",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"AMZN",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"TSLA",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"AMD",   "Sector":"Technology",  "Region":"US"},
    {"Ticker":"INTC",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"QCOM",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"AVGO",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"CRM",   "Sector":"Technology",  "Region":"US"},
    {"Ticker":"ORCL",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"NOW",   "Sector":"Technology",  "Region":"US"},
    {"Ticker":"ADBE",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"MU",    "Sector":"Technology",  "Region":"US"},
    {"Ticker":"AMAT",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"LRCX",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"KLAC",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"MRVL",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"TXN",   "Sector":"Technology",  "Region":"US"},
    {"Ticker":"ADI",   "Sector":"Technology",  "Region":"US"},
    {"Ticker":"NXPI",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"MPWR",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"ON",    "Sector":"Technology",  "Region":"US"},
    {"Ticker":"SWKS",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"ANSS",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"CDNS",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"SNPS",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"WDAY",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"VEEV",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"PLTR",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"PANW",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"CRWD",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"FTNT",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"ZS",    "Sector":"Technology",  "Region":"US"},
    {"Ticker":"DDOG",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"SNOW",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"NET",   "Sector":"Technology",  "Region":"US"},
    {"Ticker":"OKTA",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"MDB",   "Sector":"Technology",  "Region":"US"},
    {"Ticker":"TWLO",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"SHOP",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"TTD",   "Sector":"Technology",  "Region":"US"},
    {"Ticker":"UBER",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"SPOT",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"NFLX",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"SMCI",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"ARM",   "Sector":"Technology",  "Region":"US"},
    {"Ticker":"AKAM",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"AI",    "Sector":"Technology",  "Region":"US"},
    # US Financials
    {"Ticker":"JPM",   "Sector":"Financials",  "Region":"US"},
    {"Ticker":"BAC",   "Sector":"Financials",  "Region":"US"},
    {"Ticker":"GS",    "Sector":"Financials",  "Region":"US"},
    {"Ticker":"MS",    "Sector":"Financials",  "Region":"US"},
    {"Ticker":"WFC",   "Sector":"Financials",  "Region":"US"},
    {"Ticker":"C",     "Sector":"Financials",  "Region":"US"},
    {"Ticker":"BLK",   "Sector":"Financials",  "Region":"US"},
    {"Ticker":"SCHW",  "Sector":"Financials",  "Region":"US"},
    {"Ticker":"AXP",   "Sector":"Financials",  "Region":"US"},
    {"Ticker":"V",     "Sector":"Financials",  "Region":"US"},
    {"Ticker":"MA",    "Sector":"Financials",  "Region":"US"},
    {"Ticker":"PYPL",  "Sector":"Financials",  "Region":"US"},
    {"Ticker":"COF",   "Sector":"Financials",  "Region":"US"},
    {"Ticker":"USB",   "Sector":"Financials",  "Region":"US"},
    {"Ticker":"PNC",   "Sector":"Financials",  "Region":"US"},
    {"Ticker":"COIN",  "Sector":"Financials",  "Region":"US"},
    {"Ticker":"SQ",    "Sector":"Financials",  "Region":"US"},
    # US Healthcare
    {"Ticker":"LLY",   "Sector":"Healthcare",  "Region":"US"},
    {"Ticker":"JNJ",   "Sector":"Healthcare",  "Region":"US"},
    {"Ticker":"UNH",   "Sector":"Healthcare",  "Region":"US"},
    {"Ticker":"PFE",   "Sector":"Healthcare",  "Region":"US"},
    {"Ticker":"MRK",   "Sector":"Healthcare",  "Region":"US"},
    {"Ticker":"ABBV",  "Sector":"Healthcare",  "Region":"US"},
    {"Ticker":"TMO",   "Sector":"Healthcare",  "Region":"US"},
    {"Ticker":"DHR",   "Sector":"Healthcare",  "Region":"US"},
    {"Ticker":"AMGN",  "Sector":"Healthcare",  "Region":"US"},
    {"Ticker":"GILD",  "Sector":"Healthcare",  "Region":"US"},
    {"Ticker":"VRTX",  "Sector":"Healthcare",  "Region":"US"},
    {"Ticker":"REGN",  "Sector":"Healthcare",  "Region":"US"},
    {"Ticker":"ISRG",  "Sector":"Healthcare",  "Region":"US"},
    {"Ticker":"BSX",   "Sector":"Healthcare",  "Region":"US"},
    # US Energy
    {"Ticker":"XOM",   "Sector":"Energy",      "Region":"US"},
    {"Ticker":"CVX",   "Sector":"Energy",      "Region":"US"},
    {"Ticker":"COP",   "Sector":"Energy",      "Region":"US"},
    {"Ticker":"EOG",   "Sector":"Energy",      "Region":"US"},
    {"Ticker":"SLB",   "Sector":"Energy",      "Region":"US"},
    {"Ticker":"MPC",   "Sector":"Energy",      "Region":"US"},
    {"Ticker":"OXY",   "Sector":"Energy",      "Region":"US"},
    {"Ticker":"HAL",   "Sector":"Energy",      "Region":"US"},
    # US Consumer
    {"Ticker":"COST",  "Sector":"Consumer",    "Region":"US"},
    {"Ticker":"WMT",   "Sector":"Consumer",    "Region":"US"},
    {"Ticker":"TGT",   "Sector":"Consumer",    "Region":"US"},
    {"Ticker":"HD",    "Sector":"Consumer",    "Region":"US"},
    {"Ticker":"LOW",   "Sector":"Consumer",    "Region":"US"},
    {"Ticker":"NKE",   "Sector":"Consumer",    "Region":"US"},
    {"Ticker":"MCD",   "Sector":"Consumer",    "Region":"US"},
    {"Ticker":"SBUX",  "Sector":"Consumer",    "Region":"US"},
    {"Ticker":"PG",    "Sector":"Consumer",    "Region":"US"},
    {"Ticker":"KO",    "Sector":"Consumer",    "Region":"US"},
    {"Ticker":"PEP",   "Sector":"Consumer",    "Region":"US"},
    {"Ticker":"PM",    "Sector":"Consumer",    "Region":"US"},
    {"Ticker":"CL",    "Sector":"Consumer",    "Region":"US"},
    {"Ticker":"LULU",  "Sector":"Consumer",    "Region":"US"},
    {"Ticker":"ONON",  "Sector":"Consumer",    "Region":"US"},
    {"Ticker":"ABNB",  "Sector":"Consumer",    "Region":"US"},
    {"Ticker":"BKNG",  "Sector":"Consumer",    "Region":"US"},
    {"Ticker":"DIS",   "Sector":"Consumer",    "Region":"US"},
    {"Ticker":"DASH",  "Sector":"Consumer",    "Region":"US"},
    # US Industrials
    {"Ticker":"GE",    "Sector":"Industrials", "Region":"US"},
    {"Ticker":"HON",   "Sector":"Industrials", "Region":"US"},
    {"Ticker":"CAT",   "Sector":"Industrials", "Region":"US"},
    {"Ticker":"DE",    "Sector":"Industrials", "Region":"US"},
    {"Ticker":"BA",    "Sector":"Industrials", "Region":"US"},
    {"Ticker":"RTX",   "Sector":"Industrials", "Region":"US"},
    {"Ticker":"LMT",   "Sector":"Industrials", "Region":"US"},
    {"Ticker":"NOC",   "Sector":"Industrials", "Region":"US"},
    {"Ticker":"GD",    "Sector":"Industrials", "Region":"US"},
    {"Ticker":"UPS",   "Sector":"Industrials", "Region":"US"},
    {"Ticker":"FDX",   "Sector":"Industrials", "Region":"US"},
    {"Ticker":"MMM",   "Sector":"Industrials", "Region":"US"},
    {"Ticker":"ETN",   "Sector":"Industrials", "Region":"US"},
    {"Ticker":"ITW",   "Sector":"Industrials", "Region":"US"},
    {"Ticker":"AXON",  "Sector":"Industrials", "Region":"US"},
    # US Utilities
    {"Ticker":"NEE",   "Sector":"Utilities",   "Region":"US"},
    {"Ticker":"DUK",   "Sector":"Utilities",   "Region":"US"},
    {"Ticker":"SO",    "Sector":"Utilities",   "Region":"US"},
    {"Ticker":"AEP",   "Sector":"Utilities",   "Region":"US"},
    {"Ticker":"CEG",   "Sector":"Utilities",   "Region":"US"},
    {"Ticker":"VST",   "Sector":"Utilities",   "Region":"US"},
    # US Real Estate
    {"Ticker":"AMT",   "Sector":"Real Estate", "Region":"US"},
    {"Ticker":"PLD",   "Sector":"Real Estate", "Region":"US"},
    {"Ticker":"EQIX",  "Sector":"Real Estate", "Region":"US"},
    {"Ticker":"PSA",   "Sector":"Real Estate", "Region":"US"},
    {"Ticker":"SPG",   "Sector":"Real Estate", "Region":"US"},
    # US Materials
    {"Ticker":"LIN",   "Sector":"Materials",   "Region":"US"},
    {"Ticker":"SHW",   "Sector":"Materials",   "Region":"US"},
    {"Ticker":"NEM",   "Sector":"Materials",   "Region":"US"},
    {"Ticker":"FCX",   "Sector":"Materials",   "Region":"US"},
    {"Ticker":"DOW",   "Sector":"Materials",   "Region":"US"},
    {"Ticker":"GOLD",  "Sector":"Materials",   "Region":"US"},
    # US Telecom
    {"Ticker":"T",     "Sector":"Telecom",     "Region":"US"},
    {"Ticker":"VZ",    "Sector":"Telecom",     "Region":"US"},
    {"Ticker":"TMUS",  "Sector":"Telecom",     "Region":"US"},
    {"Ticker":"CMCSA", "Sector":"Telecom",     "Region":"US"},
    # European
    {"Ticker":"ASML",  "Sector":"Technology",  "Region":"Europe"},
    {"Ticker":"SAP",   "Sector":"Technology",  "Region":"Europe"},
    {"Ticker":"NOKIA", "Sector":"Technology",  "Region":"Europe"},
    {"Ticker":"ERIC",  "Sector":"Technology",  "Region":"Europe"},
    {"Ticker":"STM",   "Sector":"Technology",  "Region":"Europe"},
    {"Ticker":"NVO",   "Sector":"Healthcare",  "Region":"Europe"},
    {"Ticker":"AZN",   "Sector":"Healthcare",  "Region":"Europe"},
    {"Ticker":"GSK",   "Sector":"Healthcare",  "Region":"Europe"},
    {"Ticker":"RHHBY", "Sector":"Healthcare",  "Region":"Europe"},
    {"Ticker":"SHEL",  "Sector":"Energy",      "Region":"Europe"},
    {"Ticker":"TTE",   "Sector":"Energy",      "Region":"Europe"},
    {"Ticker":"BP",    "Sector":"Energy",      "Region":"Europe"},
    {"Ticker":"EQNR",  "Sector":"Energy",      "Region":"Europe"},
    {"Ticker":"LVMUY", "Sector":"Consumer",    "Region":"Europe"},
    {"Ticker":"NSRGY", "Sector":"Consumer",    "Region":"Europe"},
    {"Ticker":"UNLYY", "Sector":"Consumer",    "Region":"Europe"},
    {"Ticker":"RACE",  "Sector":"Consumer",    "Region":"Europe"},
    {"Ticker":"SIEGY", "Sector":"Industrials", "Region":"Europe"},
    {"Ticker":"DB",    "Sector":"Financials",  "Region":"Europe"},
    {"Ticker":"BNPQY", "Sector":"Financials",  "Region":"Europe"},
    {"Ticker":"ALIZY", "Sector":"Financials",  "Region":"Europe"},
    # Asian
    {"Ticker":"TSM",   "Sector":"Technology",  "Region":"Asia"},
    {"Ticker":"SONY",  "Sector":"Technology",  "Region":"Asia"},
    {"Ticker":"NTDOY", "Sector":"Technology",  "Region":"Asia"},
    {"Ticker":"TM",    "Sector":"Consumer",    "Region":"Asia"},
    {"Ticker":"HMC",   "Sector":"Consumer",    "Region":"Asia"},
    {"Ticker":"BABA",  "Sector":"Technology",  "Region":"Asia"},
    {"Ticker":"JD",    "Sector":"Consumer",    "Region":"Asia"},
    {"Ticker":"BIDU",  "Sector":"Technology",  "Region":"Asia"},
    {"Ticker":"INFY",  "Sector":"Technology",  "Region":"Asia"},
    {"Ticker":"HDB",   "Sector":"Financials",  "Region":"Asia"},
    {"Ticker":"WIT",   "Sector":"Technology",  "Region":"Asia"},
    {"Ticker":"MUFG",  "Sector":"Financials",  "Region":"Asia"},
    # Emerging Markets
    {"Ticker":"VALE",  "Sector":"Materials",   "Region":"EM"},
    {"Ticker":"PBR",   "Sector":"Energy",      "Region":"EM"},
    {"Ticker":"ITUB",  "Sector":"Financials",  "Region":"EM"},
    {"Ticker":"BBD",   "Sector":"Financials",  "Region":"EM"},
    {"Ticker":"ABEV",  "Sector":"Consumer",    "Region":"EM"},
    {"Ticker":"AMX",   "Sector":"Telecom",     "Region":"EM"},
    # Global ETFs
    {"Ticker":"SPY",   "Sector":"ETF",         "Region":"Global"},
    {"Ticker":"QQQ",   "Sector":"ETF",         "Region":"Global"},
    {"Ticker":"EEM",   "Sector":"ETF",         "Region":"Global"},
    {"Ticker":"EFA",   "Sector":"ETF",         "Region":"Global"},
    {"Ticker":"VTI",   "Sector":"ETF",         "Region":"Global"},
    {"Ticker":"GLD",   "Sector":"Commodities", "Region":"Global"},
    {"Ticker":"SLV",   "Sector":"Commodities", "Region":"Global"},
    {"Ticker":"USO",   "Sector":"Commodities", "Region":"Global"},
]


# ─────────────────────────────────────────────────────────────────────────────
# UTILITY HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _safe_float(val, default: float = 0.0) -> float:
    try:
        if val is None:
            return default
        f = float(val)
        return default if (f != f) else f  # NaN check
    except Exception:
        return default


def _extract_series(df: pd.DataFrame, col: str) -> pd.Series:
    """Handle both flat and MultiIndex columns from yfinance bulk download."""
    if col in df.columns:
        return df[col]
    for c in df.columns:
        if isinstance(c, tuple) and c[0] == col:
            return df[c]
    return pd.Series(dtype=float)


def _get_cashflow_fcf(stock: yf.Ticker) -> Optional[pd.Series]:
    """
    Extract annual Free Cash Flow from a yfinance Ticker object.
    Tries every known attribute + row name variant across yfinance 0.1.x – 0.2.x.
    Returns a pd.Series (most recent first) or None.
    """
    # Step 1: Get the cash-flow DataFrame
    cf_df = None
    for attr in ['cashflow', 'cash_flow', 'annual_cashflow']:
        try:
            obj = getattr(stock, attr, None)
            if obj is not None and isinstance(obj, pd.DataFrame) and not obj.empty:
                cf_df = obj
                break
        except Exception:
            continue

    # Step 2: Locate operating cash flow row
    ocf_candidates = [
        'Total Cash From Operating Activities',
        'Operating Cash Flow',
        'Cash From Operations',
        'CashFlowFromContinuingOperatingActivities',
        'operatingCashflow',
        'NetCashProvidedByOperatingActivities',
        'Cash Flows From Operating Activities',
    ]
    capex_candidates = [
        'Capital Expenditures',
        'Capital Expenditure',
        'CapEx',
        'capitalExpenditures',
        'PurchaseOfPropertyPlantAndEquipment',
        'Purchase Of Property Plant And Equipment',
        'Purchases Of Property Plant And Equipment',
        'Capital Expenditure Reported',
    ]

    operating_cf = None
    if cf_df is not None:
        for key in ocf_candidates:
            if key in cf_df.index:
                operating_cf = cf_df.loc[key].dropna()
                break
        # Case-insensitive scan as last resort
        if operating_cf is None:
            for idx in cf_df.index:
                idx_clean = str(idx).lower().replace(' ', '').replace('_', '')
                if 'operatingcash' in idx_clean or 'cashfromoperating' in idx_clean:
                    operating_cf = cf_df.loc[idx].dropna()
                    break

    # Step 3: Fallback to net income from financials
    if operating_cf is None or len(operating_cf) < 1:
        for attr in ['financials', 'income_stmt', 'annual_financials', 'quarterly_financials']:
            try:
                fin = getattr(stock, attr, None)
                if fin is None or fin.empty:
                    continue
                for key in ['Net Income', 'NetIncome', 'Net Income Common Stockholders',
                            'NetIncomeCommonStockholders']:
                    if key in fin.index:
                        operating_cf = fin.loc[key].dropna()
                        break
                if operating_cf is not None and len(operating_cf) >= 1:
                    break
            except Exception:
                continue

    if operating_cf is None or len(operating_cf) < 1:
        return None

    # Step 4: Subtract CapEx to get FCF
    capex = None
    if cf_df is not None:
        for key in capex_candidates:
            if key in cf_df.index:
                capex = cf_df.loc[key].dropna()
                break
        if capex is None:
            for idx in cf_df.index:
                idx_clean = str(idx).lower().replace(' ', '').replace('_', '')
                if 'capex' in idx_clean or 'capitalexpend' in idx_clean:
                    capex = cf_df.loc[idx].dropna()
                    break

    if capex is not None and len(capex) > 0:
        common_idx = operating_cf.index.intersection(capex.index)
        if len(common_idx) >= 1:
            fcf = operating_cf[common_idx] + capex[common_idx]  # capex stored negative
        else:
            fcf = operating_cf
    else:
        fcf = operating_cf

    # Clean up
    fcf = pd.to_numeric(fcf, errors='coerce').dropna()
    fcf = fcf[np.isfinite(fcf)]
    fcf = fcf.sort_index(ascending=False)  # most recent first
    return fcf if len(fcf) >= 1 else None


# ─────────────────────────────────────────────────────────────────────────────
# LLM ANALYST
# ─────────────────────────────────────────────────────────────────────────────

class LLMAnalyst:
    def __init__(self, mock_mode: bool = True):
        self.mock_mode = mock_mode
        self.openai_api_key = os.getenv("OPENAI_API_KEY")

    def analyze_sentiment(self, headline: str, ticker: str) -> Tuple[float, str]:
        if self.mock_mode or not self.openai_api_key:
            return self._mock_analysis(headline, ticker)
        return self._llm_analysis(headline, ticker)

    def _mock_analysis(self, headline: str, ticker: str) -> Tuple[float, str]:
        h = headline.lower()
        bullish = ['beat', 'exceed', 'growth', 'expansion', 'partnership', 'breakthrough',
                   'upgrade', 'record', 'surge', 'rally', 'strong', 'profit', 'revenue',
                   'dividend', 'buyback', 'deal', 'raises guidance', 'outperform', 'wins']
        bearish = ['miss', 'decline', 'loss', 'cut', 'probe', 'lawsuit', 'downgrade',
                   'warn', 'fall', 'drop', 'layoff', 'recall', 'fine', 'investigation',
                   'ban', 'tariff', 'sanction', 'bankruptcy', 'fraud', 'subpoena']
        score, pos, neg = 0.0, [], []
        for w in bullish:
            if w in h:
                score += 0.12
                pos.append(w)
        for w in bearish:
            if w in h:
                score -= 0.12
                neg.append(w)
        score = round(max(-1.0, min(1.0, score)), 3)
        if pos and not neg:
            reasoning = f"Positive signals: {', '.join(pos[:3])}"
        elif neg and not pos:
            reasoning = f"Risk signals: {', '.join(neg[:3])}"
        elif pos and neg:
            reasoning = f"Mixed – bullish ({', '.join(pos[:2])}) vs bearish ({', '.join(neg[:2])})"
        else:
            reasoning = "No strong directional signals detected"
        return score, reasoning

    def _llm_analysis(self, headline: str, ticker: str) -> Tuple[float, str]:
        try:
            headers = {"Authorization": f"Bearer {self.openai_api_key}",
                       "Content-Type": "application/json"}
            prompt = (f'Analyze for {ticker}: "{headline}"\n'
                      'Return exactly: score|reasoning  (score -1.0 to 1.0, max 80 chars reasoning)')
            data = {"model": "gpt-4", "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 80, "temperature": 0.2}
            r = requests.post("https://api.openai.com/v1/chat/completions",
                              headers=headers, json=data, timeout=10)
            if r.status_code == 200:
                txt = r.json()['choices'][0]['message']['content'].strip()
                parts = txt.split('|')
                return float(parts[0].strip()), (parts[1].strip() if len(parts) > 1 else "OK")
        except Exception as e:
            print(f"LLM error: {e}")
        score = TextBlob(headline).sentiment.polarity
        return round(score, 3), "TextBlob fallback (LLM unavailable)"


# ─────────────────────────────────────────────────────────────────────────────
# VALUATION ENGINE
# ─────────────────────────────────────────────────────────────────────────────

class ValuationEngine:
    @staticmethod
    def calculate_dcf_value(ticker: str, discount_rate: float = 0.10) -> Dict:
        empty = {"intrinsic_value": 0, "margin_of_safety": 0,
                 "dcf_confidence": "Low", "growth_rate": 0}
        try:
            stock = yf.Ticker(ticker)
            info  = stock.info or {}

            fcf = _get_cashflow_fcf(stock)
            if fcf is None:
                return empty

            # Use most-recent positive FCF as base
            current_fcf = _safe_float(fcf.iloc[0])
            if current_fcf <= 0:
                positive = fcf[fcf > 0]
                if positive.empty:
                    return empty
                current_fcf = _safe_float(positive.iloc[0])

            # Historical CAGR growth rate
            if len(fcf) >= 3:
                oldest = _safe_float(fcf.iloc[-1])
                years  = len(fcf) - 1
                if oldest > 0 and current_fcf > 0:
                    growth_rate = (current_fcf / oldest) ** (1.0 / years) - 1
                else:
                    growth_rate = 0.05
            else:
                rev_growth = _safe_float(info.get('revenueGrowth'), 0.05)
                ear_growth = _safe_float(info.get('earningsGrowth'), 0.05)
                growth_rate = (rev_growth + ear_growth) / 2

            growth_rate    = max(-0.25, min(0.40, growth_rate))
            terminal_growth = 0.025

            # 5-year DCF
            projected  = [current_fcf * (1 + growth_rate) ** yr for yr in range(1, 6)]
            term_val   = projected[-1] * (1 + terminal_growth) / (discount_rate - terminal_growth)
            pv_fcf     = sum(f / (1 + discount_rate) ** yr for yr, f in enumerate(projected, 1))
            pv_term    = term_val / (1 + discount_rate) ** 5
            ev         = pv_fcf + pv_term

            # Net debt adjustment
            net_debt   = (_safe_float(info.get('totalDebt'))
                          - _safe_float(info.get('totalCash') or info.get('cashAndCashEquivalents')))
            equity_val = ev - net_debt
            if equity_val <= 0:
                equity_val = ev  # ignore if net debt > EV

            shares = _safe_float(info.get('sharesOutstanding') or info.get('impliedSharesOutstanding'))
            if shares <= 0:
                return empty

            iv = equity_val / shares
            cp = _safe_float(info.get('currentPrice') or info.get('regularMarketPrice'))
            mos = ((iv - cp) / cp * 100) if cp > 0 else 0

            confidence = "High" if len(fcf) >= 4 else ("Medium" if len(fcf) >= 2 else "Low")
            return {
                "intrinsic_value":  round(iv, 2),
                "margin_of_safety": round(mos, 2),
                "dcf_confidence":   confidence,
                "growth_rate":      round(growth_rate * 100, 2),
            }
        except Exception as e:
            print(f"DCF error [{ticker}]: {e}")
            return empty


# ─────────────────────────────────────────────────────────────────────────────
# RISK MANAGER
# ─────────────────────────────────────────────────────────────────────────────

class RiskManager:
    @staticmethod
    def calculate_position_size(price: float, atr: float,
                                 account_size: float = 100_000,
                                 risk_per_trade: float = 0.02) -> Dict:
        if atr <= 0 or price <= 0:
            return {"position_size": 0, "max_shares": 0,
                    "risk_level": "Unknown", "daily_volatility": 0}
        daily_risk     = atr / price
        stop_dist      = atr * 2
        risk_amount    = account_size * risk_per_trade
        max_shares     = max(1, int(risk_amount / stop_dist))
        pos_size       = min((max_shares * price / account_size) * 100, 50.0)
        risk_level     = ("Low"    if daily_risk < 0.02 else
                          "Medium" if daily_risk < 0.05 else "High")
        return {
            "position_size":    round(pos_size, 2),
            "max_shares":       max_shares,
            "risk_level":       risk_level,
            "daily_volatility": round(daily_risk * 100, 2),
        }


# ─────────────────────────────────────────────────────────────────────────────
# INVESTMENT ENGINE
# ─────────────────────────────────────────────────────────────────────────────

class InvestmentEngine:
    def __init__(self):
        self.llm_analyst      = LLMAnalyst(mock_mode=MOCK_MODE)
        self.valuation_engine = ValuationEngine()
        self.risk_manager     = RiskManager()

        try:
            user_univ = pd.read_csv('universe.csv')
            if 'Ticker' not in user_univ.columns:
                raise ValueError("No Ticker column")
            if 'Sector' not in user_univ.columns:
                user_univ['Sector'] = 'Unknown'
            if 'Region' not in user_univ.columns:
                user_univ['Region'] = 'Unknown'
            self.universe = user_univ
        except Exception:
            self.universe = pd.DataFrame(DEFAULT_UNIVERSE)

        self.tickers = list(dict.fromkeys(self.universe['Ticker'].tolist()))

    # ── cache ───────────────────────────────────────────────────────────────

    def load_data(self) -> Tuple[pd.DataFrame, bool]:
        if os.path.exists(CACHE_FILE):
            try:
                df = pd.read_csv(CACHE_FILE)
                if 'Articles_JSON' in df.columns:
                    df['Articles_JSON'] = df['Articles_JSON'].fillna('[]').astype(str)
                return df, True
            except Exception as e:
                print(f"Cache load error: {e}")
        return pd.DataFrame(), False

    # ── news ────────────────────────────────────────────────────────────────

    def get_news_articles(self, ticker: str, max_articles: int = 5) -> List[Dict]:
        articles: List[Dict] = []
        try:
            url = (f"https://news.google.com/rss/search?q={ticker}+stock"
                   f"&hl=en-US&gl=US&ceid=US:en")
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=6)
            if r.status_code == 200:
                root = ET.fromstring(r.content)
                for item in root.findall(".//item")[:max_articles]:
                    title_el = item.find("title")
                    date_el  = item.find("pubDate")
                    headline = ""
                    if title_el is not None and title_el.text:
                        headline = title_el.text.split(" - ")[0].strip()
                    if not headline:
                        continue
                    pub_date = ""
                    if date_el is not None and date_el.text:
                        pub_date = date_el.text[:16]
                    score, reasoning = self.llm_analyst.analyze_sentiment(headline, ticker)
                    geo = any(w in headline.lower() for w in
                              ["war", "china", "tariff", "sanction", "supply chain",
                               "ban", "strike", "geopolit", "regulat", "opec"])
                    label = ("Bullish" if score >  0.10 else
                             "Bearish" if score < -0.10 else "Neutral")
                    articles.append({
                        "headline":  headline,
                        "score":     round(score, 3),
                        "label":     label,
                        "reasoning": reasoning,
                        "geo_risk":  bool(geo),
                        "pub_date":  pub_date,
                    })
        except Exception as e:
            print(f"News error [{ticker}]: {e}")

        if not articles:
            articles.append({"headline": "No recent news found", "score": 0.0,
                              "label": "Neutral", "reasoning": "News feed unavailable",
                              "geo_risk": False, "pub_date": ""})
        return articles

    # ── scoring ─────────────────────────────────────────────────────────────

    def calculate_oracle_score(self, row: Dict) -> float:
        mos = _safe_float(row.get('margin_of_safety'))
        # Valuation component (0–40 pts)
        if   mos > 50:  v = 40
        elif mos > 20:  v = 30
        elif mos > 0:   v = 20
        elif mos > -20: v = 10
        else:           v = max(0.0, 10 + mos * 0.1)

        # Technical component (0–28 pts)
        rsi = _safe_float(row.get('RSI'), 50)
        rsi_pts = max(0.0, (70 - rsi) * 0.4)
        vol_pts = min(10.0, _safe_float(row.get('Vol_Rel'), 1) * 4)
        t = rsi_pts + vol_pts

        # Quality component (0–20 pts)
        roe = _safe_float(row.get('ROE'))
        pm  = _safe_float(row.get('profit_margin'))
        q   = min(18.0, roe * 0.4)
        if pm > 20:
            q = min(20.0, q + 5)

        # Sentiment component (0–10 pts)
        s = max(0.0, min(10.0, _safe_float(row.get('Sentiment')) * 10))

        return round(min(100.0, max(0.0, v + t + q + s)), 1)

    def generate_verdict(self, row: Dict) -> str:
        parts = []
        mos = _safe_float(row.get('margin_of_safety'))
        if   mos > 30:  parts.append("🟢 STRONG VALUE: Significant discount to intrinsic value")
        elif mos > 10:  parts.append("🟡 FAIR VALUE: Modest upside to intrinsic value")
        elif mos < -20: parts.append("🔴 OVERVALUED: Trading above intrinsic value")
        rsi = _safe_float(row.get('RSI'), 50)
        if rsi < 30:   parts.append("📈 OVERSOLD: Potential mean-reversion entry")
        elif rsi > 70: parts.append("⚠️ OVERBOUGHT: Extended technically")
        roe = _safe_float(row.get('ROE'))
        pm  = _safe_float(row.get('profit_margin'))
        if roe > 20 and pm > 15:
            parts.append("💎 HIGH QUALITY: Strong returns & margins")
        si = _safe_float(row.get('short_interest'))
        if si > 15:
            parts.append(f"🐻 SHORT SQUEEZE RISK: {si:.1f}% float short")
        if row.get('Geo_Risk'):
            parts.append("🌍 GEOPOLITICAL: Macro risk in recent news")
        reasoning = str(row.get('LLM_Reasoning', ''))[:120]
        if reasoning and reasoning not in ("No data", "News feed unavailable",
                                           "No strong directional signals detected"):
            parts.append(f"🤖 {reasoning}")
        return " | ".join(parts) or "Neutral – no strong signals"

    # ── main fetch ───────────────────────────────────────────────────────────

    def fetch_market_data(self, progress_bar=None) -> pd.DataFrame:
        results = []
        tickers = self.tickers
        total   = len(tickers)

        print(f"Starting scan of {total} tickers…")

        # Bulk OHLCV download (much faster than individual calls)
        print("Bulk price download…")
        bulk = None
        try:
            bulk = yf.download(
                tickers, period="6mo", group_by='ticker',
                auto_adjust=True, progress=False, threads=True
            )
        except Exception as e:
            print(f"Bulk download failed: {e}")

        for i, ticker in enumerate(tickers):
            try:
                if progress_bar:
                    progress_bar.progress((i + 1) / total,
                                          text=f"Scanning {ticker} ({i+1}/{total})…")

                # ── Extract per-ticker OHLCV from bulk result ────────────
                df = None
                if bulk is not None and not bulk.empty:
                    try:
                        if ticker in bulk.columns.get_level_values(0):
                            raw = bulk[ticker]
                            # Flatten MultiIndex if present
                            if isinstance(raw.columns, pd.MultiIndex):
                                raw = raw.copy()
                                raw.columns = [c[0] if isinstance(c, tuple) else c
                                               for c in raw.columns]
                            df = raw.dropna(how='all')
                    except Exception:
                        pass

                # Individual fallback
                if df is None or df.empty:
                    try:
                        raw = yf.download(ticker, period="6mo",
                                          auto_adjust=True, progress=False)
                        if raw is not None and not raw.empty:
                            if isinstance(raw.columns, pd.MultiIndex):
                                raw.columns = [c[0] if isinstance(c, tuple) else c
                                               for c in raw.columns]
                            df = raw
                    except Exception:
                        pass

                if df is None or df.empty or len(df) < 20:
                    continue

                # Normalise column names
                df.columns = [str(c).strip() for c in df.columns]
                close_col = next((c for c in df.columns if c.lower() == 'close'),  None)
                high_col  = next((c for c in df.columns if c.lower() == 'high'),   None)
                low_col   = next((c for c in df.columns if c.lower() == 'low'),    None)
                vol_col   = next((c for c in df.columns if c.lower() == 'volume'), None)

                if not close_col:
                    continue

                close_s = df[close_col].squeeze().dropna()
                if len(close_s) == 0:
                    continue
                current_price = _safe_float(close_s.iloc[-1])
                if current_price <= 0:
                    continue

                high_s  = df[high_col].squeeze()  if high_col  else close_s
                low_s   = df[low_col].squeeze()   if low_col   else close_s
                vol_s   = df[vol_col].squeeze()   if vol_col   else pd.Series(dtype=float)

                # ── Technical indicators ─────────────────────────────────
                rsi_s = ta.rsi(close_s, length=14)
                atr_s = ta.atr(high_s, low_s, close_s, length=14)

                rsi_clean = rsi_s.dropna() if rsi_s is not None else pd.Series(dtype=float)
                atr_clean = atr_s.dropna() if atr_s is not None else pd.Series(dtype=float)

                current_rsi = _safe_float(rsi_clean.iloc[-1] if len(rsi_clean) else None, 50.0)
                # Fallback ATR: 2% of price (realistic estimate)
                current_atr = _safe_float(atr_clean.iloc[-1] if len(atr_clean) else None,
                                          current_price * 0.02)

                # Volume ratio
                if vol_s is not None and len(vol_s) >= 20:
                    avg_vol = _safe_float(vol_s.rolling(20).mean().iloc[-1], 1.0)
                    cur_vol = _safe_float(vol_s.iloc[-1], avg_vol)
                    vol_rel = (cur_vol / avg_vol) if avg_vol > 0 else 1.0
                else:
                    vol_rel = 1.0
                vol_rel = min(vol_rel, 20.0)

                # ── Fundamental data ─────────────────────────────────────
                info = {}
                try:
                    info = yf.Ticker(ticker).info or {}
                except Exception:
                    pass
                pe_ratio      = _safe_float(info.get('trailingPE'))
                roe           = _safe_float(info.get('returnOnEquity'))   * 100
                profit_margin = _safe_float(info.get('profitMargins'))    * 100

                # ── DCF ──────────────────────────────────────────────────
                dcf = self.valuation_engine.calculate_dcf_value(ticker)

                # ── News ─────────────────────────────────────────────────
                articles      = self.get_news_articles(ticker)
                avg_sentiment = float(np.mean([a['score'] for a in articles]))
                geo_flag      = any(a['geo_risk'] for a in articles)
                # Robust JSON serialisation (ensure_ascii avoids CSV quoting issues)
                articles_json = json.dumps(articles, ensure_ascii=True, separators=(',', ':'))

                # ── Alt data ─────────────────────────────────────────────
                short_pct  = _safe_float(info.get('shortPercentOfFloat')) * 100
                short_sent = ("Bearish (High Short Interest)" if short_pct > 20 else
                              ("Bullish (Low Short Interest)"  if short_pct < 5  else "Neutral"))

                # ── Position sizing ──────────────────────────────────────
                pos = self.risk_manager.calculate_position_size(current_price, current_atr)

                # ── Universe metadata ────────────────────────────────────
                u_row  = self.universe[self.universe['Ticker'] == ticker]
                sector = u_row['Sector'].values[0] if not u_row.empty else "Unknown"
                region = (u_row['Region'].values[0]
                          if ('Region' in u_row.columns and not u_row.empty) else "Unknown")

                record = {
                    "Ticker":           ticker,
                    "Sector":           sector,
                    "Region":           region,
                    "Price":            round(current_price, 2),
                    "RSI":              round(current_rsi, 1),
                    "ATR":              round(current_atr, 4),
                    "Vol_Rel":          round(vol_rel, 2),
                    "PE":               round(pe_ratio, 2),
                    "ROE":              round(roe, 2),
                    "profit_margin":    round(profit_margin, 2),
                    "intrinsic_value":  dcf["intrinsic_value"],
                    "margin_of_safety": dcf["margin_of_safety"],
                    "dcf_confidence":   dcf["dcf_confidence"],
                    "growth_rate":      dcf.get("growth_rate", 0),
                    "Sentiment":        round(avg_sentiment, 3),
                    "Headline":         articles[0]["headline"],
                    "LLM_Reasoning":    articles[0]["reasoning"],
                    "Articles_JSON":    articles_json,
                    "Geo_Risk":         geo_flag,
                    "short_interest":   round(short_pct, 2),
                    "insider_sentiment":short_sent,
                    "position_size":    pos["position_size"],
                    "risk_level":       pos["risk_level"],
                    "daily_volatility": pos["daily_volatility"],
                    "Last_Updated":     datetime.now().strftime("%Y-%m-%d %H:%M"),
                }

                record["Oracle_Score"] = self.calculate_oracle_score(record)
                record["AI_Verdict"]   = self.generate_verdict(record)
                results.append(record)

                time.sleep(0.08)

            except Exception as e:
                print(f"Error processing {ticker}: {e}")
                continue

        final_df = pd.DataFrame(results)
        if not final_df.empty:
            try:
                final_df.to_csv(CACHE_FILE, index=False)
                print(f"Saved {len(final_df)} records.")
            except Exception as e:
                print(f"Cache save error: {e}")
        else:
            print("Warning: no results collected.")

        return final_df
