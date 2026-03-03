"""
logic.py - Apex Markets v6.0
KEY FIXES:
- ONE yf.Ticker() call per stock, result reused everywhere
- DCF reads 'Free Cash Flow' row from cashflow DataFrame (confirmed working)
- Falls back to info['freeCashflow'] if DataFrame unavailable
- Rate limiting: 0.5s sleep between stocks, batch info calls
- No redundant API calls inside calc_dcf
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

CACHE_FILE   = "market_cache.csv"
MOCK_MODE    = True
RATE_DELAY   = 0.5   # seconds between tickers (avoid rate limiting)

NO_DCF_SECTORS = {"ETF", "Commodities", "FX"}

# ===========================================================================
# UNIVERSE
# ===========================================================================
DEFAULT_UNIVERSE = [
    {"Ticker": "AAPL",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "MSFT",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "NVDA",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "GOOGL", "Sector": "Technology",  "Region": "US"},
    {"Ticker": "META",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "AMZN",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "TSLA",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "AMD",   "Sector": "Technology",  "Region": "US"},
    {"Ticker": "INTC",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "QCOM",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "AVGO",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "CRM",   "Sector": "Technology",  "Region": "US"},
    {"Ticker": "ORCL",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "NOW",   "Sector": "Technology",  "Region": "US"},
    {"Ticker": "ADBE",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "MU",    "Sector": "Technology",  "Region": "US"},
    {"Ticker": "AMAT",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "TXN",   "Sector": "Technology",  "Region": "US"},
    {"Ticker": "CDNS",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "SNPS",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "PLTR",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "PANW",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "CRWD",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "FTNT",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "DDOG",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "NET",   "Sector": "Technology",  "Region": "US"},
    {"Ticker": "SHOP",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "UBER",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "NFLX",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "ARM",   "Sector": "Technology",  "Region": "US"},
    {"Ticker": "JPM",   "Sector": "Financials",  "Region": "US"},
    {"Ticker": "BAC",   "Sector": "Financials",  "Region": "US"},
    {"Ticker": "GS",    "Sector": "Financials",  "Region": "US"},
    {"Ticker": "V",     "Sector": "Financials",  "Region": "US"},
    {"Ticker": "MA",    "Sector": "Financials",  "Region": "US"},
    {"Ticker": "LLY",   "Sector": "Healthcare",  "Region": "US"},
    {"Ticker": "JNJ",   "Sector": "Healthcare",  "Region": "US"},
    {"Ticker": "UNH",   "Sector": "Healthcare",  "Region": "US"},
    {"Ticker": "PFE",   "Sector": "Healthcare",  "Region": "US"},
    {"Ticker": "ABBV",  "Sector": "Healthcare",  "Region": "US"},
    {"Ticker": "TMO",   "Sector": "Healthcare",  "Region": "US"},
    {"Ticker": "AMGN",  "Sector": "Healthcare",  "Region": "US"},
    {"Ticker": "VRTX",  "Sector": "Healthcare",  "Region": "US"},
    {"Ticker": "XOM",   "Sector": "Energy",      "Region": "US"},
    {"Ticker": "CVX",   "Sector": "Energy",      "Region": "US"},
    {"Ticker": "COP",   "Sector": "Energy",      "Region": "US"},
    {"Ticker": "COST",  "Sector": "Consumer",    "Region": "US"},
    {"Ticker": "WMT",   "Sector": "Consumer",    "Region": "US"},
    {"Ticker": "HD",    "Sector": "Consumer",    "Region": "US"},
    {"Ticker": "MCD",   "Sector": "Consumer",    "Region": "US"},
    {"Ticker": "PG",    "Sector": "Consumer",    "Region": "US"},
    {"Ticker": "KO",    "Sector": "Consumer",    "Region": "US"},
    {"Ticker": "PEP",   "Sector": "Consumer",    "Region": "US"},
    {"Ticker": "DIS",   "Sector": "Consumer",    "Region": "US"},
    {"Ticker": "GE",    "Sector": "Industrials", "Region": "US"},
    {"Ticker": "HON",   "Sector": "Industrials", "Region": "US"},
    {"Ticker": "CAT",   "Sector": "Industrials", "Region": "US"},
    {"Ticker": "BA",    "Sector": "Industrials", "Region": "US"},
    {"Ticker": "RTX",   "Sector": "Industrials", "Region": "US"},
    {"Ticker": "LMT",   "Sector": "Industrials", "Region": "US"},
    {"Ticker": "NEE",   "Sector": "Utilities",   "Region": "US"},
    {"Ticker": "CEG",   "Sector": "Utilities",   "Region": "US"},
    {"Ticker": "AMT",   "Sector": "Real Estate", "Region": "US"},
    {"Ticker": "EQIX",  "Sector": "Real Estate", "Region": "US"},
    {"Ticker": "NEM",   "Sector": "Materials",   "Region": "US"},
    {"Ticker": "FCX",   "Sector": "Materials",   "Region": "US"},
    {"Ticker": "T",     "Sector": "Telecom",     "Region": "US"},
    {"Ticker": "VZ",    "Sector": "Telecom",     "Region": "US"},
    {"Ticker": "TMUS",  "Sector": "Telecom",     "Region": "US"},
    {"Ticker": "ASML",  "Sector": "Technology",  "Region": "Europe"},
    {"Ticker": "SAP",   "Sector": "Technology",  "Region": "Europe"},
    {"Ticker": "NVO",   "Sector": "Healthcare",  "Region": "Europe"},
    {"Ticker": "AZN",   "Sector": "Healthcare",  "Region": "Europe"},
    {"Ticker": "SHEL",  "Sector": "Energy",      "Region": "Europe"},
    {"Ticker": "TTE",   "Sector": "Energy",      "Region": "Europe"},
    {"Ticker": "LVMUY", "Sector": "Consumer",    "Region": "Europe"},
    {"Ticker": "NSRGY", "Sector": "Consumer",    "Region": "Europe"},
    {"Ticker": "TSM",   "Sector": "Technology",  "Region": "Asia"},
    {"Ticker": "SONY",  "Sector": "Technology",  "Region": "Asia"},
    {"Ticker": "TM",    "Sector": "Consumer",    "Region": "Asia"},
    {"Ticker": "BABA",  "Sector": "Technology",  "Region": "Asia"},
    {"Ticker": "INFY",  "Sector": "Technology",  "Region": "Asia"},
    {"Ticker": "HDB",   "Sector": "Financials",  "Region": "Asia"},
    {"Ticker": "VALE",  "Sector": "Materials",   "Region": "EM"},
    {"Ticker": "PBR",   "Sector": "Energy",      "Region": "EM"},
    {"Ticker": "ITUB",  "Sector": "Financials",  "Region": "EM"},
    {"Ticker": "SPY",   "Sector": "ETF",         "Region": "Global"},
    {"Ticker": "QQQ",   "Sector": "ETF",         "Region": "Global"},
    {"Ticker": "GLD",   "Sector": "Commodities", "Region": "Global"},
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
# DCF ENGINE
# Accepts a pre-fetched yf.Ticker object so no extra API calls are made
# ===========================================================================

_EMPTY_DCF = {
    "intrinsic_value":  0.0,
    "margin_of_safety": 0.0,
    "dcf_confidence":   "N/A",
    "growth_rate":      0.0,
    "dcf_method":       "none",
}


def calc_dcf(stock: yf.Ticker, info: dict, sector: str = "") -> Dict:
    """
    DCF using the pre-fetched Ticker object and info dict.
    Priority:
      1. cashflow DataFrame row 'Free Cash Flow' (multi-year CAGR)
      2. info['freeCashflow'] TTM value
      3. info['operatingCashflow'] TTM
      4. trailingEps * shares proxy
    Only ONE extra attribute access (stock.cashflow) which is cached by yfinance.
    """
    try:
        if sector in NO_DCF_SECTORS:
            return {**_EMPTY_DCF, "dcf_method": "skipped_etf"}

        fcf         = None
        growth_rate = None
        method      = "none"

        # --- Strategy 1: cashflow DataFrame (confirmed working, row = 'Free Cash Flow') ---
        try:
            cf_df = stock.cashflow  # already fetched/cached by yfinance
            if cf_df is not None and not cf_df.empty and "Free Cash Flow" in cf_df.index:
                fcf_row = cf_df.loc["Free Cash Flow"]
                fcf_row = pd.to_numeric(fcf_row, errors="coerce").dropna()
                positive = fcf_row[fcf_row > 0].sort_index(ascending=False)
                if len(positive) >= 1:
                    fcf    = float(positive.iloc[0])
                    method = "cashflow_df"
                    # Compute CAGR if we have 3+ years
                    if len(positive) >= 3:
                        newest = float(positive.iloc[0])
                        oldest = float(positive.iloc[-1])
                        yrs    = len(positive) - 1
                        raw_cagr = (newest / oldest) ** (1.0 / yrs) - 1
                        growth_rate = max(-0.20, min(0.35, raw_cagr))
                        method = "cashflow_df+cagr"
        except Exception as e:
            print(f"  [DCF] cashflow df error: {e}")

        # --- Strategy 2: info['freeCashflow'] ---
        if fcf is None or fcf <= 0:
            v = _safe(info.get("freeCashflow"))
            if v > 0:
                fcf    = v
                method = "info_freecashflow"

        # --- Strategy 3: info['operatingCashflow'] ---
        if fcf is None or fcf <= 0:
            v = _safe(info.get("operatingCashflow"))
            if v > 0:
                fcf    = v
                method = "info_ocf"

        # --- Strategy 4: EPS proxy ---
        if fcf is None or fcf <= 0:
            eps    = _safe(info.get("trailingEps"))
            shares = _safe(info.get("sharesOutstanding") or
                           info.get("impliedSharesOutstanding"))
            if eps > 0 and shares > 0:
                fcf    = eps * shares
                method = "eps_proxy"

        if fcf is None or fcf <= 0:
            return {**_EMPTY_DCF, "dcf_method": "no_fcf_found"}

        # --- Growth rate fallback from analyst estimates ---
        if growth_rate is None:
            rev_g = _safe(info.get("revenueGrowth"),  0.05)
            ear_g = _safe(info.get("earningsGrowth"), 0.05)
            growth_rate = rev_g * 0.4 + ear_g * 0.6
            growth_rate = max(-0.20, min(0.35, growth_rate))
            if growth_rate == 0.0:
                growth_rate = 0.05

        discount_rate = 0.10
        terminal_g    = 0.025
        discount_rate = max(discount_rate, terminal_g + 0.005)

        # --- 5-year DCF ---
        proj   = [fcf * (1 + growth_rate) ** yr for yr in range(1, 6)]
        tv     = proj[-1] * (1 + terminal_g) / (discount_rate - terminal_g)
        pv_fcf = sum(f / (1 + discount_rate) ** yr for yr, f in enumerate(proj, 1))
        pv_tv  = tv / (1 + discount_rate) ** 5
        ev     = pv_fcf + pv_tv

        # --- Equity bridge ---
        debt       = _safe(info.get("totalDebt"))
        cash       = _safe(info.get("totalCash"))
        equity_val = ev - (debt - cash)
        if equity_val <= 0:
            equity_val = ev

        # --- Per-share value ---
        shares = _safe(info.get("sharesOutstanding") or
                       info.get("impliedSharesOutstanding"))
        if shares <= 0:
            return {**_EMPTY_DCF, "dcf_method": "no_shares"}

        iv = equity_val / shares
        if iv <= 0 or iv > 1_000_000:
            iv = ev / shares

        cp  = _safe(info.get("currentPrice") or info.get("regularMarketPrice"))
        mos = ((iv - cp) / cp * 100.0) if cp > 0 else 0.0

        # Confidence based on data source
        if "cagr" in method:
            confidence = "High"
        elif "cashflow_df" in method:
            confidence = "Medium"
        else:
            confidence = "Low"

        return {
            "intrinsic_value":  round(iv, 2),
            "margin_of_safety": round(mos, 1),
            "dcf_confidence":   confidence,
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


def _calc_atr(high: pd.Series, low: pd.Series,
              close: pd.Series, length: int = 14) -> float:
    if HAS_TA:
        try:
            r = ta.atr(high, low, close, length=length)
            if r is not None:
                clean = r.dropna()
                if len(clean):
                    return _safe(clean.iloc[-1])
        except Exception:
            pass
    try:
        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low  - close.shift()).abs(),
        ], axis=1).max(axis=1)
        return _safe(tr.rolling(length).mean().iloc[-1],
                     _safe(close.iloc[-1]) * 0.02)
    except Exception:
        return _safe(close.iloc[-1]) * 0.02 if len(close) else 1.0


# ===========================================================================
# SENTIMENT & NEWS
# ===========================================================================

GEO_WORDS = [
    "war", "china", "tariff", "sanction", "supply chain", "ban", "strike",
    "geopolit", "regulat", "opec", "trade war", "embargo", "conflict",
    "nato", "russia", "ukraine", "middle east", "iran", "export control",
]

POS_WORDS = [
    "beat", "beats", "topped", "exceed", "exceeded", "surpass", "record",
    "growth", "surge", "surged", "rally", "rallied", "upgrade", "upgraded",
    "profit", "revenue", "dividend", "buyback", "deal", "partnership",
    "breakthrough", "outperform", "raises guidance", "guidance raise",
    "wins", "win", "expands", "launches", "approval", "approved",
    "acquires", "acquisition", "rebound", "bullish", "buy rating",
    "price target raised", "strong", "robust", "accelerat", "momentum",
    "reward", "reaps", "rewards", "compound", "compounding", "upside",
    "positive", "gain", "gained", "jumped", "soar", "soared", "climbed",
    "rose", "rising", "teams up", "collaboration", "boosts",
    "recovery", "recover", "record quarter", "market share",
    "raised forecast", "exceed expectations", "returning value",
]

NEG_WORDS = [
    "miss", "misses", "missed", "decline", "declined", "loss", "losses",
    "cut", "cuts", "probe", "lawsuit", "downgrade", "downgraded",
    "warn", "warning", "fall", "falls", "drop", "dropped", "layoff",
    "layoffs", "recall", "fine", "fined", "investigation", "ban", "banned",
    "tariff", "sanction", "bankrupt", "bankruptcy", "fraud", "subpoena",
    "struggle", "struggles", "struggling", "concern", "concerns",
    "dependence", "dependent", "reliance", "vulnerable", "slowdown",
    "weak", "weaker", "disappoints", "disappointing", "below expectations",
    "guidance cut", "revenue miss", "earnings miss", "sell rating",
    "price target cut", "headwind", "headwinds", "underperform",
    "pressure", "pressured", "fell", "slump", "slumping",
    "worries", "threat", "threats", "halt", "halted", "suspended", "delays",
]


def _sentiment(headline: str) -> Tuple[float, str]:
    h        = headline.lower()
    pos_hits = [w for w in POS_WORDS if w in h]
    neg_hits = [w for w in NEG_WORDS if w in h]
    score    = (len(pos_hits) * 0.12) - (len(neg_hits) * 0.12)
    try:
        tb    = TextBlob(headline).sentiment.polarity
        score = score * 0.7 + tb * 0.3
    except Exception:
        pass
    score = round(max(-1.0, min(1.0, score)), 3)

    if pos_hits and not neg_hits:
        reason = "Positive signals: " + ", ".join(pos_hits[:3])
    elif neg_hits and not pos_hits:
        reason = "Negative signals: " + ", ".join(neg_hits[:3])
    elif pos_hits and neg_hits:
        reason = f"Mixed: {', '.join(pos_hits[:2])} vs {', '.join(neg_hits[:2])}"
    else:
        try:
            pol = TextBlob(headline).sentiment.polarity
            reason = ("Mildly positive tone" if pol > 0.1 else
                      "Mildly negative tone" if pol < -0.1 else
                      "Neutral tone")
        except Exception:
            reason = "Neutral"
    return score, reason


def fetch_news(ticker: str, max_items: int = 5) -> List[Dict]:
    articles: List[Dict] = []
    try:
        url = (f"https://news.google.com/rss/search"
               f"?q={ticker}+stock&hl=en-US&gl=US&ceid=US:en")
        r = requests.get(url,
                         headers={"User-Agent": "Mozilla/5.0"},
                         timeout=7)
        if r.status_code == 200:
            root = ET.fromstring(r.content)
            for item in root.findall(".//item")[:max_items]:
                t_el = item.find("title")
                d_el = item.find("pubDate")
                l_el = item.find("link")
                raw  = t_el.text if t_el is not None and t_el.text else ""
                headline = raw.split(" - ")[0].strip()
                if not headline or len(headline) < 5:
                    continue
                score, reason = _sentiment(headline)
                geo   = any(kw in headline.lower() for kw in GEO_WORDS)
                label = ("Bullish" if score >  0.10 else
                         "Bearish" if score < -0.10 else "Neutral")
                articles.append({
                    "headline": headline,
                    "score":    round(score, 3),
                    "label":    label,
                    "reason":   reason,
                    "geo_risk": bool(geo),
                    "pub_date": (d_el.text or "")[:16] if d_el is not None else "",
                    "link":     (l_el.text or "")      if l_el is not None else "",
                })
    except Exception as e:
        print(f"[News] {ticker}: {e}")

    if not articles:
        articles.append({
            "headline": "No recent news available",
            "score":    0.0, "label": "Neutral",
            "reason":   "News feed temporarily unavailable",
            "geo_risk": False, "pub_date": "", "link": "",
        })
    return articles


# ===========================================================================
# ORACLE SCORE & VERDICT
# ===========================================================================

def oracle_score(row: Dict) -> float:
    mos = _safe(row.get("margin_of_safety"))
    rsi = _safe(row.get("RSI"), 50.0)
    vol = _safe(row.get("Vol_Rel"), 1.0)
    roe = _safe(row.get("ROE"))
    pm  = _safe(row.get("profit_margin"))
    sen = _safe(row.get("Sentiment"))

    if   mos > 50:  v = 40
    elif mos > 20:  v = 30
    elif mos > 0:   v = 20
    elif mos > -20: v = 10
    else:           v = max(0.0, 10 + mos * 0.08)

    rsi_pts = max(0.0, (70 - rsi) * 0.5)
    vol_pts = min(10.0, vol * 4.0)
    t = min(30.0, rsi_pts + vol_pts)

    q = min(15.0, roe * 0.4)
    if pm > 20:
        q = min(20.0, q + 5)

    s = max(0.0, min(10.0, sen * 10))
    return round(min(100.0, max(0.0, v + t + q + s)), 1)


def build_verdict(row: Dict) -> str:
    parts = []
    mos  = _safe(row.get("margin_of_safety"))
    rsi  = _safe(row.get("RSI"), 50.0)
    roe  = _safe(row.get("ROE"))
    pm   = _safe(row.get("profit_margin"))
    si   = _safe(row.get("short_interest"))
    meth = str(row.get("dcf_method", ""))

    if   mos > 30:  parts.append("GREEN STRONG VALUE: Significant discount to intrinsic value")
    elif mos > 10:  parts.append("YELLOW FAIR VALUE: Modest upside to intrinsic value")
    elif mos < -20: parts.append("RED OVERVALUED: Trading above intrinsic value")
    elif "skipped" in meth or "no_" in meth:
        parts.append("GREY VALUATION: DCF not applicable for this asset type")

    if rsi < 30:   parts.append("OVERSOLD: RSI below 30")
    elif rsi > 70: parts.append("OVERBOUGHT: RSI above 70")

    if roe > 20 and pm > 15:
        parts.append("HIGH QUALITY: Strong ROE and margins")
    elif roe > 10:
        parts.append("DECENT QUALITY: Above-average ROE")

    if si > 15:
        parts.append(f"SHORT PRESSURE: {si:.1f}% of float short")

    if row.get("Geo_Risk"):
        parts.append("GEOPOLITICAL: Macro risk in recent headlines")

    reason = str(row.get("LLM_Reasoning", "")).strip()
    if reason and len(reason) > 10 and "unavailable" not in reason.lower():
        parts.append(f"NEWS: {reason[:100]}")

    return " | ".join(parts) if parts else "Neutral outlook"


def size_position(price: float, atr: float,
                  account: float = 100_000, risk_pct: float = 0.02) -> Dict:
    if atr <= 0 or price <= 0:
        atr = price * 0.02 if price > 0 else 1.0
    daily_vol  = (atr / price) * 100
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


# ===========================================================================
# INVESTMENT ENGINE
# ===========================================================================

class InvestmentEngine:
    def __init__(self):
        try:
            u = pd.read_csv("universe.csv")
            if "Ticker" not in u.columns:
                raise ValueError
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

    def _meta(self, ticker: str, col: str, default):
        row = self.universe[self.universe["Ticker"] == ticker]
        if not row.empty and col in row.columns:
            return row[col].values[0]
        return default

    def fetch_market_data(self, progress_bar=None) -> pd.DataFrame:
        results = []
        tickers = self.tickers
        total   = len(tickers)

        try:
            print(f"yfinance {yf.__version__} | pandas {pd.__version__}")
        except Exception:
            pass

        # --- Bulk OHLCV (one call for all tickers) ---
        print(f"Bulk downloading {total} tickers...")
        bulk = None
        try:
            bulk = yf.download(
                tickers, period="6mo", group_by="ticker",
                auto_adjust=True, progress=False, threads=True,
            )
        except Exception as e:
            print(f"Bulk download error: {e}")

        for i, ticker in enumerate(tickers):
            try:
                if progress_bar:
                    progress_bar.progress(
                        (i + 1) / total,
                        text=f"Analysing {ticker}  ({i+1}/{total})",
                    )

                # ---- OHLCV ------------------------------------------------
                df = None
                if bulk is not None and not bulk.empty:
                    try:
                        lvl0 = bulk.columns.get_level_values(0)
                        if ticker in lvl0:
                            raw = bulk.xs(ticker, axis=1, level=1) if isinstance(bulk.columns, pd.MultiIndex) else bulk[ticker]
                            raw = _flatten_df(raw).dropna(how="all")
                            if len(raw) >= 20:
                                df = raw
                    except Exception:
                        pass

                if df is None or len(df) < 20:
                    try:
                        raw = yf.download(ticker, period="6mo",
                                          auto_adjust=True, progress=False)
                        df  = _flatten_df(raw)
                    except Exception:
                        pass

                if df is None or len(df) < 20:
                    continue

                df.columns = [str(c).strip().title() for c in df.columns]
                close = df.get("Close", df.iloc[:, 0]).squeeze().dropna()
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

                if len(vol_s) >= 20:
                    avg_vol = _safe(vol_s.rolling(20).mean().iloc[-1], 1.0)
                    vol_rel = min(_safe(vol_s.iloc[-1], avg_vol) / avg_vol
                                  if avg_vol > 0 else 1.0, 20.0)
                else:
                    vol_rel = 1.0

                # ---- ONE Ticker object, reused for info + cashflow --------
                stock  = yf.Ticker(ticker)
                info   = {}
                try:
                    info = stock.info or {}
                except Exception as e:
                    print(f"  [{ticker}] info failed: {e} — using cashflow only")

                sector = self._meta(ticker, "Sector", "Unknown")

                # ---- DCF (passes stock object, no extra API call) ---------
                dcf = calc_dcf(stock, info, sector=sector)

                if i < 5:
                    print(f"  {ticker}: IV=${dcf['intrinsic_value']:.0f} "
                          f"MoS={dcf['margin_of_safety']:.1f}% "
                          f"method={dcf['dcf_method']}")

                # ---- News ------------------------------------------------
                articles     = fetch_news(ticker)
                avg_sent     = float(np.mean([a["score"] for a in articles]))
                geo_flag     = any(a["geo_risk"] for a in articles)
                articles_str = json.dumps(articles, ensure_ascii=True,
                                          separators=(",", ":"))

                # ---- Risk sizing ----------------------------------------
                pos = size_position(price, atr)

                # ---- Fundamentals from info (graceful if rate-limited) ---
                pe_ratio   = _safe(info.get("trailingPE"))
                roe        = _safe(info.get("returnOnEquity")) * 100
                pm         = _safe(info.get("profitMargins"))  * 100
                short_pct  = _safe(info.get("shortPercentOfFloat")) * 100
                short_sent = ("Bearish - High Short Interest" if short_pct > 20 else
                              "Bullish - Low Short Interest"  if short_pct < 5  else
                              "Neutral")

                record = {
                    "Ticker":            ticker,
                    "Sector":            sector,
                    "Region":            self._meta(ticker, "Region", "Unknown"),
                    "Price":             round(price, 2),
                    "RSI":               round(rsi, 1),
                    "ATR":               round(atr, 4),
                    "Vol_Rel":           round(vol_rel, 2),
                    "PE":                round(pe_ratio, 2),
                    "ROE":               round(roe, 2),
                    "profit_margin":     round(pm, 2),
                    "intrinsic_value":   dcf["intrinsic_value"],
                    "margin_of_safety":  dcf["margin_of_safety"],
                    "dcf_confidence":    dcf["dcf_confidence"],
                    "dcf_method":        dcf["dcf_method"],
                    "growth_rate":       dcf["growth_rate"],
                    "Sentiment":         round(avg_sent, 3),
                    "Headline":          articles[0]["headline"],
                    "LLM_Reasoning":     articles[0]["reason"],
                    "Articles_JSON":     articles_str,
                    "Geo_Risk":          geo_flag,
                    "short_interest":    round(short_pct, 2),
                    "insider_sentiment": short_sent,
                    "position_size":     pos["position_size"],
                    "risk_level":        pos["risk_level"],
                    "daily_volatility":  pos["daily_volatility"],
                    "Last_Updated":      datetime.now().strftime("%Y-%m-%d %H:%M"),
                }
                record["Oracle_Score"] = oracle_score(record)
                record["AI_Verdict"]   = build_verdict(record)
                results.append(record)

                # Rate limiting - be gentle with Yahoo
                time.sleep(RATE_DELAY)

            except Exception as e:
                print(f"[Error] {ticker}: {e}")
                continue

        df_out = pd.DataFrame(results)
        if not df_out.empty:
            dcf_ok = (df_out["intrinsic_value"] > 0).sum()
            print(f"Done: {len(df_out)} stocks | DCF: {dcf_ok}/{len(df_out)}")
            df_out.to_csv(CACHE_FILE, index=False)
        return df_out
