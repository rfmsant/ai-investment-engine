"""
logic.py  –  Apex Markets v4.0
Self-diagnosing DCF engine: discovers yfinance API shape at runtime.
"""
import yfinance as yf
import pandas as pd
import numpy as np
import requests
import xml.etree.ElementTree as ET
from textblob import TextBlob
import os, json, time, warnings
from datetime import datetime
from typing import Dict, List, Tuple, Optional

warnings.filterwarnings("ignore")

try:
    import pandas_ta as ta
    HAS_TA = True
except ImportError:
    HAS_TA = False

CACHE_FILE = "market_cache.csv"
MOCK_MODE  = True

# ─────────────────────────────────────────────────────────────────────────────
# UNIVERSE
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
    {"Ticker":"MDB",   "Sector":"Technology",  "Region":"US"},
    {"Ticker":"TWLO",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"SHOP",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"TTD",   "Sector":"Technology",  "Region":"US"},
    {"Ticker":"UBER",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"SPOT",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"NFLX",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"SMCI",  "Sector":"Technology",  "Region":"US"},
    {"Ticker":"ARM",   "Sector":"Technology",  "Region":"US"},
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
    {"Ticker":"LULU",  "Sector":"Consumer",    "Region":"US"},
    {"Ticker":"ABNB",  "Sector":"Consumer",    "Region":"US"},
    {"Ticker":"BKNG",  "Sector":"Consumer",    "Region":"US"},
    {"Ticker":"DIS",   "Sector":"Consumer",    "Region":"US"},
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
    {"Ticker":"ETN",   "Sector":"Industrials", "Region":"US"},
    {"Ticker":"AXON",  "Sector":"Industrials", "Region":"US"},
    # US Utilities
    {"Ticker":"NEE",   "Sector":"Utilities",   "Region":"US"},
    {"Ticker":"DUK",   "Sector":"Utilities",   "Region":"US"},
    {"Ticker":"SO",    "Sector":"Utilities",   "Region":"US"},
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
    {"Ticker":"ERIC",  "Sector":"Technology",  "Region":"Europe"},
    {"Ticker":"STM",   "Sector":"Technology",  "Region":"Europe"},
    {"Ticker":"NVO",   "Sector":"Healthcare",  "Region":"Europe"},
    {"Ticker":"AZN",   "Sector":"Healthcare",  "Region":"Europe"},
    {"Ticker":"GSK",   "Sector":"Healthcare",  "Region":"Europe"},
    {"Ticker":"RHHBY", "Sector":"Healthcare",  "Region":"Europe"},
    {"Ticker":"SHEL",  "Sector":"Energy",      "Region":"Europe"},
    {"Ticker":"TTE",   "Sector":"Energy",      "Region":"Europe"},
    {"Ticker":"BP",    "Sector":"Energy",      "Region":"Europe"},
    {"Ticker":"LVMUY", "Sector":"Consumer",    "Region":"Europe"},
    {"Ticker":"NSRGY", "Sector":"Consumer",    "Region":"Europe"},
    {"Ticker":"RACE",  "Sector":"Consumer",    "Region":"Europe"},
    {"Ticker":"SIEGY", "Sector":"Industrials", "Region":"Europe"},
    {"Ticker":"DB",    "Sector":"Financials",  "Region":"Europe"},
    {"Ticker":"BNPQY", "Sector":"Financials",  "Region":"Europe"},
    # Asian
    {"Ticker":"TSM",   "Sector":"Technology",  "Region":"Asia"},
    {"Ticker":"SONY",  "Sector":"Technology",  "Region":"Asia"},
    {"Ticker":"NTDOY", "Sector":"Technology",  "Region":"Asia"},
    {"Ticker":"TM",    "Sector":"Consumer",    "Region":"Asia"},
    {"Ticker":"BABA",  "Sector":"Technology",  "Region":"Asia"},
    {"Ticker":"JD",    "Sector":"Consumer",    "Region":"Asia"},
    {"Ticker":"BIDU",  "Sector":"Technology",  "Region":"Asia"},
    {"Ticker":"INFY",  "Sector":"Technology",  "Region":"Asia"},
    {"Ticker":"HDB",   "Sector":"Financials",  "Region":"Asia"},
    {"Ticker":"MUFG",  "Sector":"Financials",  "Region":"Asia"},
    # Emerging Markets
    {"Ticker":"VALE",  "Sector":"Materials",   "Region":"EM"},
    {"Ticker":"PBR",   "Sector":"Energy",      "Region":"EM"},
    {"Ticker":"ITUB",  "Sector":"Financials",  "Region":"EM"},
    {"Ticker":"ABEV",  "Sector":"Consumer",    "Region":"EM"},
    {"Ticker":"AMX",   "Sector":"Telecom",     "Region":"EM"},
    # ETFs
    {"Ticker":"SPY",   "Sector":"ETF",         "Region":"Global"},
    {"Ticker":"QQQ",   "Sector":"ETF",         "Region":"Global"},
    {"Ticker":"EEM",   "Sector":"ETF",         "Region":"Global"},
    {"Ticker":"EFA",   "Sector":"ETF",         "Region":"Global"},
    {"Ticker":"GLD",   "Sector":"Commodities", "Region":"Global"},
    {"Ticker":"SLV",   "Sector":"Commodities", "Region":"Global"},
]


# ─────────────────────────────────────────────────────────────────────────────
# SAFE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _safe(val, default=0.0):
    try:
        if val is None:
            return default
        f = float(val)
        return default if (f != f or not np.isfinite(f)) else f
    except Exception:
        return default


def _flatten_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse MultiIndex columns to their first level string."""
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [c[0] if isinstance(c, tuple) else str(c) for c in df.columns]
    return df


# ─────────────────────────────────────────────────────────────────────────────
# SELF-DIAGNOSING DCF  ─  discovers yfinance API shape at runtime
# ─────────────────────────────────────────────────────────────────────────────

# Keywords that identify operating cash flow rows (case-insensitive, no spaces)
_OCF_KEYWORDS  = ["operatingcash", "cashfromoperating", "cashprovidedbyoperating",
                   "netcashfromoperating", "totalcashfromoperating"]
# Keywords that identify capital expenditure rows
_CAPEX_KEYWORDS = ["capitalexpend", "capex", "purchaseofproperty", "acquisitionofproperty"]


def _find_row(df: pd.DataFrame, keywords: List[str]) -> Optional[pd.Series]:
    """Return first row whose index (cleaned) contains any keyword."""
    for idx in df.index:
        cleaned = str(idx).lower().replace(" ", "").replace("_", "").replace("-", "")
        if any(kw in cleaned for kw in keywords):
            series = df.loc[idx]
            series = pd.to_numeric(series, errors="coerce").dropna()
            if len(series) >= 1:
                return series
    return None


def _get_df_from_ticker(stock: yf.Ticker, attr: str) -> Optional[pd.DataFrame]:
    """Safely get a DataFrame attribute from a yfinance Ticker."""
    try:
        obj = getattr(stock, attr, None)
        if obj is None:
            return None
        if callable(obj):
            obj = obj()
        if isinstance(obj, pd.DataFrame) and not obj.empty:
            return obj
    except Exception:
        pass
    return None


def _extract_fcf(stock: yf.Ticker, debug_ticker: str = "") -> Optional[pd.Series]:
    """
    Runtime-adaptive FCF extraction.
    Strategy:
      1. Try every cashflow attribute yfinance has ever used
      2. Scan ALL rows by keyword (not hardcoded names) — handles any version
      3. Fall back to net income from income statement
    """
    # All cashflow attribute names ever used across yfinance versions
    CF_ATTRS = [
        "cashflow",           # yfinance 0.1.x annual
        "cash_flow",          # alias
        "annual_cashflow",    # yfinance 0.2.x
        "get_cashflow",       # callable in some versions
        "quarterly_cashflow", # quarterly fallback
    ]
    FIN_ATTRS = [
        "financials",
        "income_stmt",
        "annual_financials",
        "get_financials",
    ]
    NI_KEYWORDS = ["netincome", "netearnings", "profitaftertax"]

    cf_df = None
    for attr in CF_ATTRS:
        cf_df = _get_df_from_ticker(stock, attr)
        if cf_df is not None:
            if debug_ticker:
                print(f"  [{debug_ticker}] CF attr='{attr}', rows={list(cf_df.index)[:5]}")
            break

    operating_cf = None
    capex        = None

    if cf_df is not None:
        operating_cf = _find_row(cf_df, _OCF_KEYWORDS)
        capex        = _find_row(cf_df, _CAPEX_KEYWORDS)

    # Fallback: net income as proxy for operating CF
    if operating_cf is None:
        for attr in FIN_ATTRS:
            fin_df = _get_df_from_ticker(stock, attr)
            if fin_df is None:
                continue
            operating_cf = _find_row(fin_df, NI_KEYWORDS)
            if operating_cf is not None:
                if debug_ticker:
                    print(f"  [{debug_ticker}] Using net income fallback from '{attr}'")
                break

    if operating_cf is None or len(operating_cf) < 1:
        if debug_ticker:
            print(f"  [{debug_ticker}] No cash flow data found")
        return None

    # Compute FCF = Operating CF + CapEx (CapEx is negative in yfinance)
    if capex is not None and len(capex) >= 1:
        common = operating_cf.index.intersection(capex.index)
        if len(common) >= 1:
            fcf = operating_cf[common] + capex[common]
        else:
            fcf = operating_cf
    else:
        fcf = operating_cf

    fcf = pd.to_numeric(fcf, errors="coerce").dropna()
    fcf = fcf[np.isfinite(fcf)]
    fcf = fcf.sort_index(ascending=False)  # most recent first

    if debug_ticker:
        print(f"  [{debug_ticker}] FCF series: {fcf.tolist()[:4]}")

    return fcf if len(fcf) >= 1 else None


# ─────────────────────────────────────────────────────────────────────────────
# VALUATION ENGINE
# ─────────────────────────────────────────────────────────────────────────────

_EMPTY_DCF = {"intrinsic_value": 0.0, "margin_of_safety": 0.0,
               "dcf_confidence": "N/A", "growth_rate": 0.0, "dcf_method": "none"}


class ValuationEngine:
    @staticmethod
    def calculate(ticker: str, discount_rate: float = 0.10,
                  debug: bool = False) -> Dict:
        try:
            stock = yf.Ticker(ticker)

            # Use debug=True for first run to diagnose issues
            fcf = _extract_fcf(stock, debug_ticker=ticker if debug else "")

            if fcf is None:
                return {**_EMPTY_DCF, "dcf_method": "no_cf_data"}

            info = stock.info or {}

            # Find a positive base FCF (most recent positive year)
            positive_fcf = fcf[fcf > 0]
            if positive_fcf.empty:
                return {**_EMPTY_DCF, "dcf_method": "all_negative_cf"}

            current_fcf = _safe(positive_fcf.iloc[0])

            # Growth rate from CAGR
            if len(fcf) >= 3:
                oldest = _safe(fcf.iloc[-1])
                years  = len(fcf) - 1
                if oldest > 0 and current_fcf > 0:
                    raw_cagr = (current_fcf / oldest) ** (1.0 / years) - 1
                    growth_rate = max(-0.25, min(0.35, raw_cagr))
                else:
                    growth_rate = 0.05
            elif len(fcf) == 2:
                yoy = (fcf.iloc[0] / fcf.iloc[1] - 1) if fcf.iloc[1] != 0 else 0
                growth_rate = max(-0.20, min(0.30, yoy * 0.5))  # regress toward mean
            else:
                # Single year: use analyst estimates from info
                rev_g = _safe(info.get("revenueGrowth"), 0.05)
                ear_g = _safe(info.get("earningsGrowth"), 0.05)
                growth_rate = max(0.0, min(0.25, (rev_g + ear_g) / 2))

            terminal_g = 0.025
            # Safety: discount rate must exceed terminal growth
            if discount_rate <= terminal_g:
                discount_rate = terminal_g + 0.01

            # 5-year DCF
            proj    = [current_fcf * (1 + growth_rate) ** yr for yr in range(1, 6)]
            tv      = proj[-1] * (1 + terminal_g) / (discount_rate - terminal_g)
            pv_fcfs = sum(f / (1 + discount_rate) ** yr for yr, f in enumerate(proj, 1))
            pv_tv   = tv / (1 + discount_rate) ** 5
            ev      = pv_fcfs + pv_tv

            # Equity bridge: subtract net debt
            total_debt  = _safe(info.get("totalDebt"))
            total_cash  = _safe(info.get("totalCash") or info.get("cashAndCashEquivalents"))
            net_debt    = total_debt - total_cash
            equity_val  = ev - net_debt
            if equity_val <= 0:
                equity_val = ev  # if net debt > EV just use EV

            shares = _safe(info.get("sharesOutstanding") or
                           info.get("impliedSharesOutstanding"))
            if shares <= 0:
                return {**_EMPTY_DCF, "dcf_method": "no_shares_data"}

            iv = equity_val / shares
            if iv <= 0 or iv > 1_000_000:  # sanity cap
                iv = ev / shares

            cp = _safe(info.get("currentPrice") or info.get("regularMarketPrice"))
            mos = ((iv - cp) / cp * 100) if cp > 0 else 0.0

            confidence = ("High"   if len(fcf) >= 4 else
                          "Medium" if len(fcf) >= 2 else "Low")
            method     = ("dcf_fcf" if "no_cf_data" not in str(fcf.name) else "dcf_ni")

            return {
                "intrinsic_value":  round(iv, 2),
                "margin_of_safety": round(mos, 1),
                "dcf_confidence":   confidence,
                "growth_rate":      round(growth_rate * 100, 1),
                "dcf_method":       method,
            }

        except Exception as e:
            print(f"[DCF] {ticker}: {e}")
            return {**_EMPTY_DCF, "dcf_method": f"error:{e}"}


# ─────────────────────────────────────────────────────────────────────────────
# TECHNICAL INDICATORS  (pandas-ta with pure-numpy fallback)
# ─────────────────────────────────────────────────────────────────────────────

def _calc_rsi(close: pd.Series, length: int = 14) -> float:
    if HAS_TA:
        try:
            result = ta.rsi(close, length=length)
            if result is not None:
                clean = result.dropna()
                if len(clean) > 0:
                    return _safe(clean.iloc[-1], 50.0)
        except Exception:
            pass
    # Pure numpy fallback
    try:
        delta  = close.diff().dropna()
        gains  = delta.clip(lower=0)
        losses = (-delta).clip(lower=0)
        avg_g  = gains.ewm(com=length - 1, min_periods=length).mean().iloc[-1]
        avg_l  = losses.ewm(com=length - 1, min_periods=length).mean().iloc[-1]
        if avg_l == 0:
            return 100.0
        rs  = avg_g / avg_l
        rsi = 100 - (100 / (1 + rs))
        return _safe(rsi, 50.0)
    except Exception:
        return 50.0


def _calc_atr(high: pd.Series, low: pd.Series, close: pd.Series,
              length: int = 14) -> float:
    if HAS_TA:
        try:
            result = ta.atr(high, low, close, length=length)
            if result is not None:
                clean = result.dropna()
                if len(clean) > 0:
                    return _safe(clean.iloc[-1])
        except Exception:
            pass
    # Pure numpy fallback
    try:
        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low  - close.shift()).abs()
        ], axis=1).max(axis=1)
        atr = tr.rolling(length).mean().iloc[-1]
        return _safe(atr, _safe(close.iloc[-1]) * 0.02)
    except Exception:
        return _safe(close.iloc[-1]) * 0.02 if len(close) > 0 else 1.0


# ─────────────────────────────────────────────────────────────────────────────
# LLM / SENTIMENT
# ─────────────────────────────────────────────────────────────────────────────

class LLMAnalyst:
    def __init__(self, mock_mode: bool = True):
        self.mock_mode      = mock_mode
        self.openai_api_key = os.getenv("OPENAI_API_KEY")

    def analyze(self, headline: str, ticker: str) -> Tuple[float, str]:
        if not self.mock_mode and self.openai_api_key:
            return self._gpt(headline, ticker)
        return self._mock(headline, ticker)

    def _mock(self, headline: str, ticker: str) -> Tuple[float, str]:
        h = headline.lower()
        pos_words = ["beat", "exceed", "record", "growth", "surge", "rally", "upgrade",
                     "profit", "revenue", "dividend", "buyback", "deal", "partnership",
                     "breakthrough", "strong", "outperform", "raises guidance", "wins",
                     "expands", "launches", "approval", "acquires"]
        neg_words = ["miss", "decline", "loss", "cut", "probe", "lawsuit", "downgrade",
                     "warn", "fall", "drop", "layoff", "recall", "fine", "investigation",
                     "ban", "tariff", "sanction", "bankrupt", "fraud", "subpoena",
                     "misses", "reduces", "delays", "halts", "suspend"]
        score, pos_hits, neg_hits = 0.0, [], []
        for w in pos_words:
            if w in h:
                score += 0.12
                pos_hits.append(w)
        for w in neg_words:
            if w in h:
                score -= 0.12
                neg_hits.append(w)

        # TextBlob as secondary signal
        try:
            tb_score = TextBlob(headline).sentiment.polarity
            score = score * 0.7 + tb_score * 0.3
        except Exception:
            pass

        score = round(max(-1.0, min(1.0, score)), 3)

        if pos_hits and not neg_hits:
            reason = f"Positive signals detected: {', '.join(pos_hits[:3])}"
        elif neg_hits and not pos_hits:
            reason = f"Risk signals detected: {', '.join(neg_hits[:3])}"
        elif pos_hits and neg_hits:
            reason = f"Mixed signals — bullish ({', '.join(pos_hits[:2])}) vs bearish ({', '.join(neg_hits[:2])})"
        else:
            # Use TextBlob description
            try:
                tb = TextBlob(headline).sentiment
                if tb.polarity > 0.1:
                    reason = "Mildly positive language tone"
                elif tb.polarity < -0.1:
                    reason = "Mildly negative language tone"
                else:
                    reason = "Neutral tone — no strong directional signals"
            except Exception:
                reason = "Neutral — no directional signals found"
        return score, reason

    def _gpt(self, headline: str, ticker: str) -> Tuple[float, str]:
        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {self.openai_api_key}",
                         "Content-Type": "application/json"},
                json={"model": "gpt-4", "temperature": 0.2, "max_tokens": 80,
                      "messages": [{"role": "user",
                                    "content": (f'For {ticker}: "{headline}"\n'
                                                'Reply: score|reason  (score=-1 to 1, reason<80 chars)')}]},
                timeout=10
            )
            if r.status_code == 200:
                txt = r.json()["choices"][0]["message"]["content"].strip()
                p   = txt.split("|")
                return float(p[0].strip()), (p[1].strip() if len(p) > 1 else "OK")
        except Exception as e:
            print(f"GPT error: {e}")
        return self._mock(headline, ticker)


# ─────────────────────────────────────────────────────────────────────────────
# RISK MANAGER
# ─────────────────────────────────────────────────────────────────────────────

class RiskManager:
    @staticmethod
    def size(price: float, atr: float,
             account: float = 100_000, risk_pct: float = 0.02) -> Dict:
        if atr <= 0 or price <= 0:
            daily_vol = 2.0  # assume 2% as default
            atr = price * 0.02 if price > 0 else 1.0
        else:
            daily_vol = (atr / price) * 100

        stop_dist  = atr * 2
        risk_amt   = account * risk_pct
        max_shares = max(1, int(risk_amt / stop_dist))
        pos_pct    = min((max_shares * price / account) * 100, 50.0)
        level      = "Low" if daily_vol < 2 else ("Medium" if daily_vol < 5 else "High")
        return {
            "position_size":    round(pos_pct, 1),
            "max_shares":       max_shares,
            "risk_level":       level,
            "daily_volatility": round(daily_vol, 2),
        }


# ─────────────────────────────────────────────────────────────────────────────
# NEWS FETCHER
# ─────────────────────────────────────────────────────────────────────────────

GEO_KEYWORDS = ["war", "china", "tariff", "sanction", "supply chain", "ban",
                 "strike", "geopolit", "regulat", "opec", "trade war", "embargo",
                 "conflict", "nato", "russia", "ukraine", "middle east"]


class NewsFetcher:
    def __init__(self, analyst: LLMAnalyst):
        self.analyst = analyst

    def fetch(self, ticker: str, max_items: int = 5) -> List[Dict]:
        articles: List[Dict] = []
        try:
            url = (f"https://news.google.com/rss/search"
                   f"?q={ticker}+stock&hl=en-US&gl=US&ceid=US:en")
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (compatible)"}, timeout=7)
            if r.status_code == 200:
                root = ET.fromstring(r.content)
                for item in root.findall(".//item")[:max_items]:
                    title_el = item.find("title")
                    date_el  = item.find("pubDate")
                    link_el  = item.find("link")

                    raw_title = title_el.text if title_el is not None and title_el.text else ""
                    headline  = raw_title.split(" - ")[0].strip()
                    if not headline or len(headline) < 5:
                        continue

                    pub_date = (date_el.text or "")[:16] if date_el is not None else ""
                    link     = (link_el.text or "") if link_el is not None else ""

                    score, reason = self.analyst.analyze(headline, ticker)
                    geo           = any(kw in headline.lower() for kw in GEO_KEYWORDS)
                    label         = ("Bullish" if score >  0.10 else
                                     "Bearish" if score < -0.10 else "Neutral")

                    articles.append({
                        "headline": headline,
                        "score":    round(score, 3),
                        "label":    label,
                        "reason":   reason,
                        "geo_risk": bool(geo),
                        "pub_date": pub_date,
                        "link":     link,
                    })
        except Exception as e:
            print(f"[News] {ticker}: {e}")

        if not articles:
            articles.append({
                "headline": "No recent news available",
                "score":    0.0,
                "label":    "Neutral",
                "reason":   "News feed temporarily unavailable",
                "geo_risk": False,
                "pub_date": "",
                "link":     "",
            })
        return articles


# ─────────────────────────────────────────────────────────────────────────────
# ORACLE SCORE
# ─────────────────────────────────────────────────────────────────────────────

def oracle_score(row: Dict) -> float:
    mos = _safe(row.get("margin_of_safety"))
    rsi = _safe(row.get("RSI"), 50.0)
    vol = _safe(row.get("Vol_Rel"), 1.0)
    roe = _safe(row.get("ROE"))
    pm  = _safe(row.get("profit_margin"))
    sen = _safe(row.get("Sentiment"))

    # Valuation (0-40 pts) — partial credit even without DCF
    if   mos > 50:  v = 40
    elif mos > 20:  v = 30
    elif mos > 0:   v = 20
    elif mos > -20: v = 10
    else:           v = max(0.0, 10 + mos * 0.08)

    # Technical (0-30 pts)
    rsi_pts = max(0.0, (70 - rsi) * 0.5)  # RSI 30→20pts, RSI 70→0pts
    vol_pts = min(10.0, vol * 4.0)
    t = min(30.0, rsi_pts + vol_pts)

    # Quality (0-20 pts)
    q = min(15.0, roe * 0.4)
    if pm > 20: q = min(20.0, q + 5)

    # Sentiment (0-10 pts)
    s = max(0.0, min(10.0, sen * 10))

    return round(min(100.0, max(0.0, v + t + q + s)), 1)


def verdict(row: Dict) -> str:
    parts = []
    mos = _safe(row.get("margin_of_safety"))
    rsi = _safe(row.get("RSI"), 50.0)
    roe = _safe(row.get("ROE"))
    pm  = _safe(row.get("profit_margin"))
    si  = _safe(row.get("short_interest"))
    dcf = row.get("dcf_confidence", "N/A")

    if   mos > 30:  parts.append("🟢 STRONG VALUE: Significant discount to intrinsic value")
    elif mos > 10:  parts.append("🟡 FAIR VALUE: Modest upside to intrinsic value")
    elif mos < -20: parts.append("🔴 OVERVALUED: Trading above intrinsic value")
    elif dcf == "N/A": parts.append("⚪ VALUATION: DCF not available for this asset type")

    if rsi < 30:   parts.append("📈 OVERSOLD: RSI below 30 — potential mean-reversion entry")
    elif rsi > 70: parts.append("⚠️ OVERBOUGHT: RSI above 70 — momentum extended")

    if roe > 20 and pm > 15:
        parts.append("💎 HIGH QUALITY: Strong returns on equity & profit margins")
    elif roe > 10:
        parts.append("✅ DECENT QUALITY: Above-average returns on equity")

    if si > 15:
        parts.append(f"🐻 SHORT PRESSURE: {si:.1f}% of float sold short")

    if row.get("Geo_Risk"):
        parts.append("🌍 GEOPOLITICAL: Macro risk signals in recent headlines")

    reason = str(row.get("LLM_Reasoning", "")).strip()
    if reason and "unavailable" not in reason.lower() and "no strong" not in reason.lower():
        parts.append(f"📰 {reason[:100]}")

    return " | ".join(parts) if parts else "Neutral outlook — monitoring"


# ─────────────────────────────────────────────────────────────────────────────
# INVESTMENT ENGINE
# ─────────────────────────────────────────────────────────────────────────────

class InvestmentEngine:
    def __init__(self):
        self.analyst    = LLMAnalyst(mock_mode=MOCK_MODE)
        self.valuation  = ValuationEngine()
        self.risk       = RiskManager()
        self.news       = NewsFetcher(self.analyst)

        try:
            u = pd.read_csv("universe.csv")
            if "Ticker" not in u.columns:
                raise ValueError
            if "Sector" not in u.columns:
                u["Sector"] = "Unknown"
            if "Region" not in u.columns:
                u["Region"] = "Unknown"
            self.universe = u
        except Exception:
            self.universe = pd.DataFrame(DEFAULT_UNIVERSE)

        self.tickers = list(dict.fromkeys(self.universe["Ticker"].tolist()))

    def load_data(self) -> Tuple[pd.DataFrame, bool]:
        if os.path.exists(CACHE_FILE):
            try:
                df = pd.read_csv(CACHE_FILE)
                if "Articles_JSON" in df.columns:
                    df["Articles_JSON"] = df["Articles_JSON"].fillna("[]").astype(str)
                return df, True
            except Exception as e:
                print(f"Cache load error: {e}")
        return pd.DataFrame(), False

    def _lookup(self, ticker: str, col: str, default):
        row = self.universe[self.universe["Ticker"] == ticker]
        if not row.empty and col in row.columns:
            return row[col].values[0]
        return default

    # ── MAIN FETCH ───────────────────────────────────────────────────────────
    def fetch_market_data(self, progress_bar=None) -> pd.DataFrame:
        results  = []
        tickers  = self.tickers
        total    = len(tickers)

        # Print yfinance version to help diagnose
        try:
            print(f"yfinance version: {yf.__version__}")
        except Exception:
            pass

        # Bulk OHLCV
        print(f"Bulk download for {total} tickers…")
        bulk = None
        try:
            bulk = yf.download(
                tickers, period="6mo", group_by="ticker",
                auto_adjust=True, progress=False, threads=True
            )
        except Exception as e:
            print(f"Bulk download failed ({e}), will use individual downloads")

        # Debug DCF on first two US equities
        debug_tickers = {"AAPL", "MSFT"}

        for i, ticker in enumerate(tickers):
            try:
                if progress_bar:
                    progress_bar.progress(
                        (i + 1) / total,
                        text=f"Analysing {ticker}  ({i+1}/{total})"
                    )

                # ── OHLCV ────────────────────────────────────────────────
                df = None
                if bulk is not None and not bulk.empty:
                    try:
                        lvl0 = bulk.columns.get_level_values(0)
                        if ticker in lvl0:
                            raw = bulk[ticker]
                            raw = _flatten_cols(raw)
                            raw = raw.dropna(how="all")
                            if len(raw) >= 20:
                                df = raw
                    except Exception:
                        pass

                if df is None or len(df) < 20:
                    try:
                        raw = yf.download(ticker, period="6mo",
                                          auto_adjust=True, progress=False)
                        if raw is not None and not raw.empty:
                            raw = _flatten_cols(raw)
                            if len(raw) >= 20:
                                df = raw
                    except Exception:
                        pass

                if df is None or len(df) < 20:
                    continue

                # Normalise column names to title-case
                df.columns = [str(c).strip().title() for c in df.columns]
                close = df.get("Close", df.iloc[:, 3]).squeeze().dropna()
                high  = df.get("High",  close).squeeze()
                low   = df.get("Low",   close).squeeze()
                vol_s = df.get("Volume", pd.Series(dtype=float)).squeeze()

                if len(close) < 20:
                    continue

                price = _safe(close.iloc[-1])
                if price <= 0:
                    continue

                rsi = _calc_rsi(close)
                atr = _calc_atr(high, low, close)

                # Volume ratio (20-day)
                if len(vol_s) >= 20:
                    avg_vol = _safe(vol_s.rolling(20).mean().iloc[-1], 1.0)
                    cur_vol = _safe(vol_s.iloc[-1], avg_vol)
                    vol_rel = min((cur_vol / avg_vol) if avg_vol > 0 else 1.0, 20.0)
                else:
                    vol_rel = 1.0

                # ── FUNDAMENTALS ─────────────────────────────────────────
                info = {}
                try:
                    info = yf.Ticker(ticker).info or {}
                except Exception:
                    pass
                pe_ratio = _safe(info.get("trailingPE"))
                roe      = _safe(info.get("returnOnEquity")) * 100
                pm       = _safe(info.get("profitMargins"))  * 100

                # ── DCF ──────────────────────────────────────────────────
                dcf = self.valuation.calculate(ticker,
                                               debug=(ticker in debug_tickers))

                # ── NEWS ─────────────────────────────────────────────────
                articles     = self.news.fetch(ticker)
                avg_sent     = float(np.mean([a["score"] for a in articles]))
                geo_flag     = any(a["geo_risk"] for a in articles)
                articles_str = json.dumps(articles, ensure_ascii=True, separators=(",", ":"))

                # ── RISK ─────────────────────────────────────────────────
                pos = self.risk.size(price, atr)

                # ── SHORT INTEREST ────────────────────────────────────────
                short_pct  = _safe(info.get("shortPercentOfFloat")) * 100
                short_sent = ("Bearish – High Short Interest" if short_pct > 20 else
                              "Bullish – Low Short Interest"  if short_pct < 5  else "Neutral")

                record = {
                    "Ticker":           ticker,
                    "Sector":           self._lookup(ticker, "Sector", "Unknown"),
                    "Region":           self._lookup(ticker, "Region", "Unknown"),
                    "Price":            round(price, 2),
                    "RSI":              round(rsi, 1),
                    "ATR":              round(atr, 4),
                    "Vol_Rel":          round(vol_rel, 2),
                    "PE":               round(pe_ratio, 2),
                    "ROE":              round(roe, 2),
                    "profit_margin":    round(pm, 2),
                    "intrinsic_value":  dcf["intrinsic_value"],
                    "margin_of_safety": dcf["margin_of_safety"],
                    "dcf_confidence":   dcf["dcf_confidence"],
                    "dcf_method":       dcf["dcf_method"],
                    "growth_rate":      dcf["growth_rate"],
                    "Sentiment":        round(avg_sent, 3),
                    "Headline":         articles[0]["headline"],
                    "LLM_Reasoning":    articles[0]["reason"],
                    "Articles_JSON":    articles_str,
                    "Geo_Risk":         geo_flag,
                    "short_interest":   round(short_pct, 2),
                    "insider_sentiment":short_sent,
                    "position_size":    pos["position_size"],
                    "risk_level":       pos["risk_level"],
                    "daily_volatility": pos["daily_volatility"],
                    "Last_Updated":     datetime.now().strftime("%Y-%m-%d %H:%M"),
                }
                record["Oracle_Score"] = oracle_score(record)
                record["AI_Verdict"]   = verdict(record)
                results.append(record)

                time.sleep(0.05)

            except Exception as e:
                print(f"[Error] {ticker}: {e}")
                continue

        df_out = pd.DataFrame(results)
        if not df_out.empty:
            df_out.to_csv(CACHE_FILE, index=False)
            print(f"Saved {len(df_out)} records to {CACHE_FILE}")
            # DCF diagnostic summary
            dcf_ok = (df_out["intrinsic_value"] > 0).sum()
            print(f"DCF success rate: {dcf_ok}/{len(df_out)} stocks")
        else:
            print("WARNING: No results collected!")
        return df_out
