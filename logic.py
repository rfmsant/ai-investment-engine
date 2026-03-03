"""
logic.py - Apex Markets v5.0
DCF uses stock.info dict directly (freeCashflow, operatingCashflow keys).
No cash flow statement DataFrame parsing - works on every yfinance version.
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

# ETF/commodity sectors - skip DCF
NO_DCF_SECTORS = {"ETF", "Commodities", "FX"}

# ===========================================================================
# UNIVERSE
# ===========================================================================
DEFAULT_UNIVERSE = [
    # US Technology
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
    {"Ticker": "LRCX",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "KLAC",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "TXN",   "Sector": "Technology",  "Region": "US"},
    {"Ticker": "ADI",   "Sector": "Technology",  "Region": "US"},
    {"Ticker": "CDNS",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "SNPS",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "WDAY",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "PLTR",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "PANW",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "CRWD",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "FTNT",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "DDOG",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "SNOW",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "NET",   "Sector": "Technology",  "Region": "US"},
    {"Ticker": "SHOP",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "TTD",   "Sector": "Technology",  "Region": "US"},
    {"Ticker": "UBER",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "SPOT",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "NFLX",  "Sector": "Technology",  "Region": "US"},
    {"Ticker": "ARM",   "Sector": "Technology",  "Region": "US"},
    {"Ticker": "AI",    "Sector": "Technology",  "Region": "US"},
    # US Financials
    {"Ticker": "JPM",   "Sector": "Financials",  "Region": "US"},
    {"Ticker": "BAC",   "Sector": "Financials",  "Region": "US"},
    {"Ticker": "GS",    "Sector": "Financials",  "Region": "US"},
    {"Ticker": "MS",    "Sector": "Financials",  "Region": "US"},
    {"Ticker": "WFC",   "Sector": "Financials",  "Region": "US"},
    {"Ticker": "C",     "Sector": "Financials",  "Region": "US"},
    {"Ticker": "BLK",   "Sector": "Financials",  "Region": "US"},
    {"Ticker": "V",     "Sector": "Financials",  "Region": "US"},
    {"Ticker": "MA",    "Sector": "Financials",  "Region": "US"},
    {"Ticker": "PYPL",  "Sector": "Financials",  "Region": "US"},
    {"Ticker": "COIN",  "Sector": "Financials",  "Region": "US"},
    # US Healthcare
    {"Ticker": "LLY",   "Sector": "Healthcare",  "Region": "US"},
    {"Ticker": "JNJ",   "Sector": "Healthcare",  "Region": "US"},
    {"Ticker": "UNH",   "Sector": "Healthcare",  "Region": "US"},
    {"Ticker": "PFE",   "Sector": "Healthcare",  "Region": "US"},
    {"Ticker": "MRK",   "Sector": "Healthcare",  "Region": "US"},
    {"Ticker": "ABBV",  "Sector": "Healthcare",  "Region": "US"},
    {"Ticker": "TMO",   "Sector": "Healthcare",  "Region": "US"},
    {"Ticker": "AMGN",  "Sector": "Healthcare",  "Region": "US"},
    {"Ticker": "GILD",  "Sector": "Healthcare",  "Region": "US"},
    {"Ticker": "VRTX",  "Sector": "Healthcare",  "Region": "US"},
    {"Ticker": "REGN",  "Sector": "Healthcare",  "Region": "US"},
    {"Ticker": "ISRG",  "Sector": "Healthcare",  "Region": "US"},
    # US Energy
    {"Ticker": "XOM",   "Sector": "Energy",      "Region": "US"},
    {"Ticker": "CVX",   "Sector": "Energy",      "Region": "US"},
    {"Ticker": "COP",   "Sector": "Energy",      "Region": "US"},
    {"Ticker": "EOG",   "Sector": "Energy",      "Region": "US"},
    {"Ticker": "SLB",   "Sector": "Energy",      "Region": "US"},
    {"Ticker": "OXY",   "Sector": "Energy",      "Region": "US"},
    # US Consumer
    {"Ticker": "COST",  "Sector": "Consumer",    "Region": "US"},
    {"Ticker": "WMT",   "Sector": "Consumer",    "Region": "US"},
    {"Ticker": "TGT",   "Sector": "Consumer",    "Region": "US"},
    {"Ticker": "HD",    "Sector": "Consumer",    "Region": "US"},
    {"Ticker": "NKE",   "Sector": "Consumer",    "Region": "US"},
    {"Ticker": "MCD",   "Sector": "Consumer",    "Region": "US"},
    {"Ticker": "SBUX",  "Sector": "Consumer",    "Region": "US"},
    {"Ticker": "PG",    "Sector": "Consumer",    "Region": "US"},
    {"Ticker": "KO",    "Sector": "Consumer",    "Region": "US"},
    {"Ticker": "PEP",   "Sector": "Consumer",    "Region": "US"},
    {"Ticker": "LULU",  "Sector": "Consumer",    "Region": "US"},
    {"Ticker": "ABNB",  "Sector": "Consumer",    "Region": "US"},
    {"Ticker": "BKNG",  "Sector": "Consumer",    "Region": "US"},
    {"Ticker": "DIS",   "Sector": "Consumer",    "Region": "US"},
    # US Industrials
    {"Ticker": "GE",    "Sector": "Industrials", "Region": "US"},
    {"Ticker": "HON",   "Sector": "Industrials", "Region": "US"},
    {"Ticker": "CAT",   "Sector": "Industrials", "Region": "US"},
    {"Ticker": "BA",    "Sector": "Industrials", "Region": "US"},
    {"Ticker": "RTX",   "Sector": "Industrials", "Region": "US"},
    {"Ticker": "LMT",   "Sector": "Industrials", "Region": "US"},
    {"Ticker": "UPS",   "Sector": "Industrials", "Region": "US"},
    {"Ticker": "ETN",   "Sector": "Industrials", "Region": "US"},
    {"Ticker": "AXON",  "Sector": "Industrials", "Region": "US"},
    # US Utilities
    {"Ticker": "NEE",   "Sector": "Utilities",   "Region": "US"},
    {"Ticker": "DUK",   "Sector": "Utilities",   "Region": "US"},
    {"Ticker": "CEG",   "Sector": "Utilities",   "Region": "US"},
    {"Ticker": "VST",   "Sector": "Utilities",   "Region": "US"},
    # US Real Estate
    {"Ticker": "AMT",   "Sector": "Real Estate", "Region": "US"},
    {"Ticker": "PLD",   "Sector": "Real Estate", "Region": "US"},
    {"Ticker": "EQIX",  "Sector": "Real Estate", "Region": "US"},
    # US Materials
    {"Ticker": "LIN",   "Sector": "Materials",   "Region": "US"},
    {"Ticker": "NEM",   "Sector": "Materials",   "Region": "US"},
    {"Ticker": "FCX",   "Sector": "Materials",   "Region": "US"},
    # US Telecom
    {"Ticker": "T",     "Sector": "Telecom",     "Region": "US"},
    {"Ticker": "VZ",    "Sector": "Telecom",     "Region": "US"},
    {"Ticker": "TMUS",  "Sector": "Telecom",     "Region": "US"},
    # Europe
    {"Ticker": "ASML",  "Sector": "Technology",  "Region": "Europe"},
    {"Ticker": "SAP",   "Sector": "Technology",  "Region": "Europe"},
    {"Ticker": "ERIC",  "Sector": "Technology",  "Region": "Europe"},
    {"Ticker": "NVO",   "Sector": "Healthcare",  "Region": "Europe"},
    {"Ticker": "AZN",   "Sector": "Healthcare",  "Region": "Europe"},
    {"Ticker": "GSK",   "Sector": "Healthcare",  "Region": "Europe"},
    {"Ticker": "SHEL",  "Sector": "Energy",      "Region": "Europe"},
    {"Ticker": "TTE",   "Sector": "Energy",      "Region": "Europe"},
    {"Ticker": "BP",    "Sector": "Energy",      "Region": "Europe"},
    {"Ticker": "LVMUY", "Sector": "Consumer",    "Region": "Europe"},
    {"Ticker": "NSRGY", "Sector": "Consumer",    "Region": "Europe"},
    {"Ticker": "RACE",  "Sector": "Consumer",    "Region": "Europe"},
    {"Ticker": "SIEGY", "Sector": "Industrials", "Region": "Europe"},
    {"Ticker": "DB",    "Sector": "Financials",  "Region": "Europe"},
    # Asia
    {"Ticker": "TSM",   "Sector": "Technology",  "Region": "Asia"},
    {"Ticker": "SONY",  "Sector": "Technology",  "Region": "Asia"},
    {"Ticker": "NTDOY", "Sector": "Technology",  "Region": "Asia"},
    {"Ticker": "TM",    "Sector": "Consumer",    "Region": "Asia"},
    {"Ticker": "BABA",  "Sector": "Technology",  "Region": "Asia"},
    {"Ticker": "BIDU",  "Sector": "Technology",  "Region": "Asia"},
    {"Ticker": "INFY",  "Sector": "Technology",  "Region": "Asia"},
    {"Ticker": "HDB",   "Sector": "Financials",  "Region": "Asia"},
    {"Ticker": "MUFG",  "Sector": "Financials",  "Region": "Asia"},
    # Emerging Markets
    {"Ticker": "VALE",  "Sector": "Materials",   "Region": "EM"},
    {"Ticker": "PBR",   "Sector": "Energy",      "Region": "EM"},
    {"Ticker": "ITUB",  "Sector": "Financials",  "Region": "EM"},
    {"Ticker": "ABEV",  "Sector": "Consumer",    "Region": "EM"},
    # ETFs
    {"Ticker": "SPY",   "Sector": "ETF",         "Region": "Global"},
    {"Ticker": "QQQ",   "Sector": "ETF",         "Region": "Global"},
    {"Ticker": "EEM",   "Sector": "ETF",         "Region": "Global"},
    {"Ticker": "GLD",   "Sector": "Commodities", "Region": "Global"},
]


# ===========================================================================
# HELPERS
# ===========================================================================

def _safe(val, default: float = 0.0) -> float:
    """Safely convert any value to float."""
    try:
        if val is None:
            return default
        f = float(val)
        return default if (f != f or not np.isfinite(f)) else f
    except Exception:
        return default


def _flatten_df(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten MultiIndex columns from yfinance bulk download."""
    if df is None:
        return pd.DataFrame()
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [c[0] if isinstance(c, tuple) else str(c) for c in df.columns]
    return df


# ===========================================================================
# DCF ENGINE - uses stock.info dict, no DataFrame parsing needed
# ===========================================================================

_EMPTY_DCF = {
    "intrinsic_value":  0.0,
    "margin_of_safety": 0.0,
    "dcf_confidence":   "N/A",
    "growth_rate":      0.0,
    "dcf_method":       "none",
}


def calc_dcf(info: Dict, sector: str = "", discount_rate: float = 0.10) -> Dict:
    """
    DCF valuation using only stock.info - works on ALL yfinance versions.

    Key info fields used:
      freeCashflow      - TTM free cash flow (best source)
      operatingCashflow - TTM operating cash flow (fallback)
      trailingEps       - earnings per share (last resort proxy)
      revenueGrowth     - analyst forward revenue growth estimate
      earningsGrowth    - analyst forward earnings growth estimate
      sharesOutstanding - share count
      totalDebt/totalCash - net debt bridge
      currentPrice      - current market price
    """
    try:
        # Skip DCF for assets that don't have earnings
        if sector in NO_DCF_SECTORS:
            return {**_EMPTY_DCF, "dcf_method": "skipped_no_earnings"}

        # --- Step 1: find best available FCF figure ---
        fcf    = _safe(info.get("freeCashflow"))
        method = "ttm_fcf"

        if fcf == 0.0:
            fcf    = _safe(info.get("operatingCashflow"))
            method = "ttm_ocf"

        if fcf == 0.0:
            # Use EPS * shares as earnings proxy
            eps    = _safe(info.get("trailingEps"))
            shares_tmp = _safe(info.get("sharesOutstanding") or
                               info.get("impliedSharesOutstanding"))
            if eps > 0 and shares_tmp > 0:
                fcf    = eps * shares_tmp
                method = "eps_proxy"

        if fcf <= 0:
            return {**_EMPTY_DCF, "dcf_method": "no_cashflow_in_info"}

        # --- Step 2: growth rate from analyst estimates ---
        rev_g = _safe(info.get("revenueGrowth"),  0.05)
        ear_g = _safe(info.get("earningsGrowth"), 0.05)

        # Weighted blend, clamp to realistic range
        growth_rate = rev_g * 0.4 + ear_g * 0.6
        growth_rate = max(-0.20, min(0.35, growth_rate))
        if growth_rate == 0.0:
            growth_rate = 0.05

        terminal_g    = 0.025
        discount_rate = max(discount_rate, terminal_g + 0.005)

        # --- Step 3: 5-year DCF ---
        proj   = [fcf * (1 + growth_rate) ** yr for yr in range(1, 6)]
        tv     = proj[-1] * (1 + terminal_g) / (discount_rate - terminal_g)
        pv_fcf = sum(f / (1 + discount_rate) ** yr for yr, f in enumerate(proj, 1))
        pv_tv  = tv / (1 + discount_rate) ** 5
        ev     = pv_fcf + pv_tv

        # --- Step 4: equity value (subtract net debt) ---
        debt       = _safe(info.get("totalDebt"))
        cash       = _safe(info.get("totalCash"))
        equity_val = ev - (debt - cash)
        if equity_val <= 0:
            equity_val = ev  # ignore net-debt if it makes equity negative

        # --- Step 5: per-share intrinsic value ---
        shares = _safe(info.get("sharesOutstanding") or
                       info.get("impliedSharesOutstanding"))
        if shares <= 0:
            return {**_EMPTY_DCF, "dcf_method": "no_shares_in_info"}

        iv = equity_val / shares
        if iv <= 0 or iv > 1_000_000:
            iv = ev / shares  # fallback ignoring net debt

        cp  = _safe(info.get("currentPrice") or info.get("regularMarketPrice"))
        mos = ((iv - cp) / cp * 100.0) if cp > 0 else 0.0

        return {
            "intrinsic_value":  round(iv, 2),
            "margin_of_safety": round(mos, 1),
            "dcf_confidence":   "Medium",
            "growth_rate":      round(growth_rate * 100.0, 1),
            "dcf_method":       method,
        }

    except Exception as e:
        return {**_EMPTY_DCF, "dcf_method": f"error:{e}"}


# ===========================================================================
# TECHNICAL INDICATORS (pandas-ta with numpy fallback)
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
# SENTIMENT / NEWS
# ===========================================================================

GEO_WORDS = [
    "war", "china", "tariff", "sanction", "supply chain", "ban", "strike",
    "geopolit", "regulat", "opec", "trade war", "embargo", "conflict",
    "nato", "russia", "ukraine", "middle east",
]

POS_WORDS = [
    "beat", "exceed", "record", "growth", "surge", "rally", "upgrade",
    "profit", "revenue", "dividend", "buyback", "deal", "partnership",
    "breakthrough", "outperform", "raises guidance", "wins", "expands",
    "launches", "approval", "acquires", "strong earnings",
]

NEG_WORDS = [
    "miss", "decline", "loss", "cut", "probe", "lawsuit", "downgrade",
    "warn", "fall", "drop", "layoff", "recall", "fine", "investigation",
    "ban", "tariff", "sanction", "bankrupt", "fraud", "subpoena",
    "misses estimates", "reduces outlook", "delays",
]


def _mock_sentiment(headline: str) -> Tuple[float, str]:
    h = headline.lower()
    pos_hits = [w for w in POS_WORDS if w in h]
    neg_hits = [w for w in NEG_WORDS if w in h]

    score = (len(pos_hits) * 0.12) - (len(neg_hits) * 0.12)
    try:
        tb_score = TextBlob(headline).sentiment.polarity
        score    = score * 0.7 + tb_score * 0.3
    except Exception:
        pass

    score = round(max(-1.0, min(1.0, score)), 3)

    if pos_hits and not neg_hits:
        reason = "Positive: " + ", ".join(pos_hits[:3])
    elif neg_hits and not pos_hits:
        reason = "Negative: " + ", ".join(neg_hits[:3])
    elif pos_hits and neg_hits:
        reason = f"Mixed: {', '.join(pos_hits[:2])} vs {', '.join(neg_hits[:2])}"
    else:
        try:
            pol = TextBlob(headline).sentiment.polarity
            if pol > 0.1:
                reason = "Mildly positive language tone"
            elif pol < -0.1:
                reason = "Mildly negative language tone"
            else:
                reason = "Neutral tone - no strong directional signals"
        except Exception:
            reason = "Neutral - no directional signals found"
    return score, reason


def fetch_news(ticker: str, max_items: int = 5) -> List[Dict]:
    articles: List[Dict] = []
    try:
        url = (
            f"https://news.google.com/rss/search"
            f"?q={ticker}+stock&hl=en-US&gl=US&ceid=US:en"
        )
        r = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; ApexMarkets/1.0)"},
            timeout=7,
        )
        if r.status_code == 200:
            root = ET.fromstring(r.content)
            for item in root.findall(".//item")[:max_items]:
                title_el = item.find("title")
                date_el  = item.find("pubDate")
                link_el  = item.find("link")

                raw = title_el.text if title_el is not None and title_el.text else ""
                headline = raw.split(" - ")[0].strip()
                if not headline or len(headline) < 5:
                    continue

                pub_date = (date_el.text or "")[:16] if date_el is not None else ""
                link     = (link_el.text or "")      if link_el  is not None else ""

                score, reason = _mock_sentiment(headline)
                geo   = any(kw in headline.lower() for kw in GEO_WORDS)
                label = ("Bullish" if score >  0.10 else
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

    # Valuation 0-40 pts
    if   mos > 50:  v = 40
    elif mos > 20:  v = 30
    elif mos > 0:   v = 20
    elif mos > -20: v = 10
    else:           v = max(0.0, 10 + mos * 0.08)

    # Technical 0-30 pts
    rsi_pts = max(0.0, (70 - rsi) * 0.5)
    vol_pts = min(10.0, vol * 4.0)
    t = min(30.0, rsi_pts + vol_pts)

    # Quality 0-20 pts
    q = min(15.0, roe * 0.4)
    if pm > 20:
        q = min(20.0, q + 5)

    # Sentiment 0-10 pts
    s = max(0.0, min(10.0, sen * 10))

    return round(min(100.0, max(0.0, v + t + q + s)), 1)


def build_verdict(row: Dict) -> str:
    parts = []
    mos  = _safe(row.get("margin_of_safety"))
    rsi  = _safe(row.get("RSI"), 50.0)
    roe  = _safe(row.get("ROE"))
    pm   = _safe(row.get("profit_margin"))
    si   = _safe(row.get("short_interest"))
    meth = row.get("dcf_method", "")

    if   mos > 30:  parts.append("GREEN STRONG VALUE: Significant discount to intrinsic value")
    elif mos > 10:  parts.append("YELLOW FAIR VALUE: Modest upside to intrinsic value")
    elif mos < -20: parts.append("RED OVERVALUED: Trading above intrinsic value")
    elif "skipped" in str(meth) or "no_" in str(meth):
        parts.append("GREY VALUATION: DCF not applicable for this asset type")

    if rsi < 30:   parts.append("OVERSOLD: RSI below 30 - potential mean-reversion entry")
    elif rsi > 70: parts.append("OVERBOUGHT: RSI above 70 - momentum extended")

    if roe > 20 and pm > 15:
        parts.append("HIGH QUALITY: Strong returns on equity and profit margins")
    elif roe > 10:
        parts.append("DECENT QUALITY: Above-average returns on equity")

    if si > 15:
        parts.append(f"SHORT PRESSURE: {si:.1f}% of float sold short")

    if row.get("Geo_Risk"):
        parts.append("GEOPOLITICAL: Macro risk signals in recent headlines")

    reason = str(row.get("LLM_Reasoning", "")).strip()
    if reason and "unavailable" not in reason.lower() and len(reason) > 10:
        parts.append(f"NEWS: {reason[:100]}")

    return " | ".join(parts) if parts else "Neutral outlook - monitoring"


# ===========================================================================
# RISK MANAGER
# ===========================================================================

def size_position(price: float, atr: float,
                  account: float = 100_000,
                  risk_pct: float = 0.02) -> Dict:
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
                raise ValueError("No Ticker column")
            u.setdefault("Sector", "Unknown")
            u.setdefault("Region", "Unknown")
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

        # Bulk OHLCV download
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

                # --- OHLCV ---
                df = None
                if bulk is not None and not bulk.empty:
                    try:
                        if ticker in bulk.columns.get_level_values(0):
                            df = _flatten_df(bulk[ticker]).dropna(how="all")
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

                # --- Fundamentals + DCF (single info call) ---
                info = {}
                try:
                    info = yf.Ticker(ticker).info or {}
                except Exception:
                    pass

                pe_ratio = _safe(info.get("trailingPE"))
                roe      = _safe(info.get("returnOnEquity"))   * 100
                pm       = _safe(info.get("profitMargins"))    * 100
                sector   = self._meta(ticker, "Sector", "Unknown")

                dcf = calc_dcf(info, sector=sector)

                # Log DCF result for first few tickers
                if i < 5:
                    print(f"  {ticker}: FCF={info.get('freeCashflow')} "
                          f"OCF={info.get('operatingCashflow')} "
                          f"IV={dcf['intrinsic_value']} "
                          f"MoS={dcf['margin_of_safety']}% "
                          f"method={dcf['dcf_method']}")

                # --- News ---
                articles     = fetch_news(ticker)
                avg_sent     = float(np.mean([a["score"] for a in articles]))
                geo_flag     = any(a["geo_risk"] for a in articles)
                articles_str = json.dumps(articles, ensure_ascii=True,
                                          separators=(",", ":"))

                # --- Risk sizing ---
                pos = size_position(price, atr)

                # --- Short interest ---
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

                time.sleep(0.05)

            except Exception as e:
                print(f"[Error] {ticker}: {e}")
                continue

        df_out = pd.DataFrame(results)

        if not df_out.empty:
            dcf_ok = (df_out["intrinsic_value"] > 0).sum()
            print(f"Done: {len(df_out)} stocks | DCF success: {dcf_ok}/{len(df_out)}")
            df_out.to_csv(CACHE_FILE, index=False)

        return df_out
