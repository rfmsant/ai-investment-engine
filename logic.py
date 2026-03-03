"""
logic.py - Apex Markets v7.0
- Reads universe from universe.csv (your 477 tickers)
- Single yf.Ticker() per stock, reused for info + cashflow
- DCF: cashflow DataFrame 'Free Cash Flow' row -> info fallbacks
- Rate-limit safe: sequential with 0.3s sleep
- Finance-specific sentiment keywords
- Backward-compatible: keeps generate_ai_report() for your app.py
"""
import yfinance as yf
import pandas as pd
import numpy as np
import requests
import xml.etree.ElementTree as ET
from textblob import TextBlob
import os, json, time, warnings
from datetime import datetime
from typing import Dict, List, Tuple

warnings.filterwarnings("ignore")

CACHE_FILE = "market_cache.csv"
RATE_DELAY = 0.3
NO_DCF     = {"ETF", "Commodities", "FX", "REIT"}

# ── Universe ────────────────────────────────────────────────────────────────
_BUILTIN_SECTORS = {
    "AI_Tech":       ["NVDA","MSFT","GOOGL","AMD","PLTR","SMCI","TSM","META","TSLA","CRM","ADBE","NFLX","AMZN","ORCL","IBM","INTC","QCOM","AVGO","NOW","SNOW","PANW","CRWD","FTNT","ZS","NET","SHOP","UBER","DDOG","TEAM","WDAY","MDB","SQ","PYPL","AFRM","COIN","MSTR","AI","PATH","IOT","ASML","TXN","AMAT","MU","LRCX","KLAC","CDNS","SNPS","ARM","AAPL"],
    "War_Defense":   ["LMT","RTX","NOC","GD","BA","HII","LHX","TXT","KTOS","AVAV","LDOS","CACI","SAIC","BWXT","TDG","HEI","BAESY","OSK","AXON"],
    "Nuclear_Energy":["CCJ","NEE","DUK","SO","AEP","EXC","PEG","ETR","D","PCG","CEG","SMR","UUUU","LEU","NXE","DNN","VST"],
    "Energy":        ["XOM","CVX","SHEL","TTE","COP","SLB","EOG","VLO","MPC","BP","OXY","KMI","WMB","PSX","HES","DVN","FANG","MRO","HAL","BKR","CTRA","APA","OVV","PBR"],
    "Food_Agri":     ["ADM","CTVA","CF","MOS","NTR","FMC","DE","CAT","CNH","AGCO","TSN","BG","ANDE","SMG"],
    "Food_Staples":  ["KO","PEP","MDLZ","GIS","K","CPB","KHC","HSY","STZ","CL","PG","COST","WMT","KR","SYY","TGT","HD","LOW","EL","CLX","SBUX","MCD","YUM","CMG","NKE"],
    "Raw_Materials": ["FCX","SCCO","RIO","VALE","BHP","AA","NUE","CLF","X","STLD","RS","CMC","VMC","MLM","LIN","APD","SHW","ECL","DD","ALB","SQM"],
    "Gold_Minerals": ["NEM","GOLD","AEM","KGC","FNV","WPM","RGLD","PAAS","AU","GFI","HMY","BTG","SA","NGD"],
    "Biochem_Pharma":["LLY","NVO","JNJ","PFE","MRK","ABBV","BMY","GILD","AMGN","BIIB","REGN","VRTX","MRNA","BNTX","AZN","SNY","NVS","ZTS","ISRG","SYK","BSX","MDT","EW","BAX","UNH"],
    "Financials":    ["MA","V","AXP","JPM","BAC","WFC","C","GS","MS","BLK","SCHW","TROW","PYPL","ITUB","HDB","MUFG","DB"],
    "Industrials":   ["UPS","FDX","UNP","HON","GE","MMM","EMR","ETN","PH","ITW","NSC","CSX","WM","RSG"],
    "Real_Estate":   ["AMT","PLD","CCI","EQIX","DLR","PSA","O","VICI","SBAC","WELL","AVB","EQR","VTR","BXP","ARE","MAA"],
    "Telecom":       ["T","VZ","TMUS","CMCSA","CHTR"],
    "Consumer":      ["DIS","WBD","MAR","HLT","RCL","CCL","MGM","WYNN","LVS","DRI","LEN","DHI","PHM","TJX","ROST","DLTR","DG","AZO","ORLY","TSCO","ULTA","BBY","EBAY","ETSY"],
    "Asia_EM":       ["BABA","JD","PDD","BIDU","NTES","XPEV","LI","NIO","BYDDY","SONY","NTDOY","TM","INFY","SE","CPNG","MELI"],
}

def load_universe() -> pd.DataFrame:
    if os.path.exists("universe.csv"):
        try:
            u = pd.read_csv("universe.csv")
            if "Ticker" in u.columns:
                if "Sector" not in u.columns:
                    u["Sector"] = "Market"
                return u[["Ticker","Sector"]].drop_duplicates("Ticker").reset_index(drop=True)
        except Exception as e:
            print(f"universe.csv error: {e}")
    rows = []
    for sec, tickers in _BUILTIN_SECTORS.items():
        for t in tickers:
            rows.append({"Ticker": t, "Sector": sec})
    return pd.DataFrame(rows).drop_duplicates("Ticker").reset_index(drop=True)

# ── Helpers ──────────────────────────────────────────────────────────────────
def _safe(val, default: float = 0.0) -> float:
    try:
        if val is None: return default
        f = float(val)
        return default if (f != f or not np.isfinite(f)) else f
    except Exception:
        return default

# ── DCF ──────────────────────────────────────────────────────────────────────
_EMPTY_DCF = {"intrinsic_value":0.0,"margin_of_safety":0.0,
               "dcf_confidence":"N/A","growth_rate":0.0,"dcf_method":"none"}

def calc_dcf(stock: yf.Ticker, info: dict, sector: str = "") -> dict:
    """
    Proper 5-year DCF. Accepts pre-fetched Ticker + info dict (no extra API calls).
    Data priority:
      1. cashflow DataFrame 'Free Cash Flow' row (confirmed in yfinance 1.2.0)
      2. info['freeCashflow']
      3. info['operatingCashflow']
      4. trailingEps * shares
    """
    try:
        if sector in NO_DCF:
            return {**_EMPTY_DCF, "dcf_method": "skipped"}

        fcf = None
        growth_rate = None
        method = "none"

        # 1. Cashflow DataFrame
        try:
            cf = stock.cashflow
            if cf is not None and not cf.empty and "Free Cash Flow" in cf.index:
                row = pd.to_numeric(cf.loc["Free Cash Flow"], errors="coerce").dropna()
                pos = row[row > 0].sort_index(ascending=False)
                if len(pos) >= 1:
                    fcf    = float(pos.iloc[0])
                    method = "cf_df"
                    if len(pos) >= 3:
                        cagr = (float(pos.iloc[0]) / float(pos.iloc[-1])) ** (1/(len(pos)-1)) - 1
                        growth_rate = max(-0.20, min(0.35, cagr))
                        method = "cf_df+cagr"
        except Exception as e:
            print(f"    [DCF] cashflow err: {e}")

        # 2. info['freeCashflow']
        if not fcf or fcf <= 0:
            v = _safe(info.get("freeCashflow"))
            if v > 0: fcf, method = v, "info_fcf"

        # 3. info['operatingCashflow']
        if not fcf or fcf <= 0:
            v = _safe(info.get("operatingCashflow"))
            if v > 0: fcf, method = v, "info_ocf"

        # 4. EPS proxy
        if not fcf or fcf <= 0:
            eps    = _safe(info.get("trailingEps"))
            shares = _safe(info.get("sharesOutstanding") or info.get("impliedSharesOutstanding"))
            if eps > 0 and shares > 0:
                fcf, method = eps * shares, "eps_proxy"

        if not fcf or fcf <= 0:
            return {**_EMPTY_DCF, "dcf_method": "no_fcf"}

        # Growth rate
        if growth_rate is None:
            rg = _safe(info.get("revenueGrowth"),  0.05)
            eg = _safe(info.get("earningsGrowth"), 0.05)
            growth_rate = max(-0.20, min(0.35, rg*0.4 + eg*0.6))
            if growth_rate == 0.0: growth_rate = 0.05

        dr = max(0.10, 0.025 + 0.005)
        tg = 0.025

        proj   = [fcf * (1+growth_rate)**y for y in range(1,6)]
        tv     = proj[-1] * (1+tg) / (dr-tg)
        pv     = sum(f/(1+dr)**y for y,f in enumerate(proj,1))
        ev     = pv + tv/(1+dr)**5

        debt   = _safe(info.get("totalDebt"))
        cash   = _safe(info.get("totalCash"))
        eq     = max(ev - (debt - cash), ev * 0.1)  # floor at 10% of EV

        shares = _safe(info.get("sharesOutstanding") or info.get("impliedSharesOutstanding"))
        if shares <= 0: return {**_EMPTY_DCF, "dcf_method": "no_shares"}

        iv  = eq / shares
        if iv <= 0 or iv > 1_000_000: iv = ev / shares
        cp  = _safe(info.get("currentPrice") or info.get("regularMarketPrice"))
        mos = ((iv - cp) / cp * 100.0) if cp > 0 else 0.0

        return {
            "intrinsic_value":  round(iv, 2),
            "margin_of_safety": round(mos, 1),
            "dcf_confidence":   "High" if "cagr" in method else "Medium" if "cf_df" in method else "Low",
            "growth_rate":      round(growth_rate*100, 1),
            "dcf_method":       method,
        }
    except Exception as e:
        return {**_EMPTY_DCF, "dcf_method": f"error:{e}"}

# ── Technical ─────────────────────────────────────────────────────────────────
def _rsi(close: pd.Series, n: int = 14) -> float:
    try:
        d    = close.diff().dropna()
        gain = d.clip(lower=0).ewm(com=n-1, min_periods=n).mean()
        loss = (-d).clip(lower=0).ewm(com=n-1, min_periods=n).mean()
        rs   = gain.iloc[-1] / loss.iloc[-1] if loss.iloc[-1] != 0 else 100
        return round(_safe(100 - 100/(1+rs), 50.0), 1)
    except Exception:
        return 50.0

def _atr(high, low, close, n: int = 14) -> float:
    try:
        tr = pd.concat([high-low,(high-close.shift()).abs(),(low-close.shift()).abs()],axis=1).max(axis=1)
        return _safe(tr.rolling(n).mean().iloc[-1], _safe(close.iloc[-1])*0.02)
    except Exception:
        return _safe(close.iloc[-1])*0.02

# ── Sentiment ─────────────────────────────────────────────────────────────────
POS_WORDS = [
    "beat","beats","topped","exceed","exceeded","surpass","record","growth","surge",
    "surged","rally","rallied","upgrade","upgraded","profit","dividend","buyback",
    "deal","partnership","breakthrough","outperform","raises guidance","wins","win",
    "expands","launches","approval","approved","acquires","acquisition","rebound",
    "bullish","strong","robust","accelerat","momentum","reward","reaps","rewards",
    "compound","upside","gain","gained","jumped","soar","soared","climbed","rose",
    "rising","teams up","collaboration","boosts","recovery","market share",
    "raised forecast","record quarter","beat estimates",
]
NEG_WORDS = [
    "miss","misses","missed","decline","declined","loss","losses","cut","cuts",
    "probe","lawsuit","downgrade","downgraded","warn","warning","fall","falls",
    "drop","dropped","layoff","layoffs","recall","fine","fined","investigation",
    "ban","banned","tariff","sanction","bankrupt","fraud","subpoena","struggle",
    "struggles","concern","concerns","dependence","vulnerable","slowdown","weak",
    "weaker","disappoints","below expectations","guidance cut","revenue miss",
    "earnings miss","sell rating","price target cut","headwind","headwinds",
    "underperform","pressure","pressured","fell","slump","slumping","worries",
    "threat","threats","halt","halted","suspended","delays",
]
GEO_WORDS = [
    "war","china","tariff","sanction","supply chain","ban","strike","geopolit",
    "regulat","opec","trade war","embargo","conflict","nato","russia","ukraine",
    "middle east","iran","export control","retaliatory",
]

def _score_headline(h: str) -> Tuple[float, str]:
    hl    = h.lower()
    pos_h = [w for w in POS_WORDS if w in hl]
    neg_h = [w for w in NEG_WORDS if w in hl]
    score = (len(pos_h)*0.12) - (len(neg_h)*0.12)
    try:
        score = score*0.7 + TextBlob(h).sentiment.polarity*0.3
    except Exception:
        pass
    score = round(max(-1.0, min(1.0, score)), 3)
    if pos_h and not neg_h:
        reason = "Positive: " + ", ".join(pos_h[:3])
    elif neg_h and not pos_h:
        reason = "Negative: " + ", ".join(neg_h[:3])
    elif pos_h and neg_h:
        reason = f"Mixed: {', '.join(pos_h[:2])} vs {', '.join(neg_h[:2])}"
    else:
        try:
            p = TextBlob(h).sentiment.polarity
            reason = "Mildly positive" if p > 0.1 else "Mildly negative" if p < -0.1 else "Neutral tone"
        except Exception:
            reason = "Neutral"
    return score, reason

def fetch_news(ticker: str, n: int = 5) -> List[Dict]:
    articles = []
    try:
        url = f"https://news.google.com/rss/search?q={ticker}+stock&hl=en-US&gl=US&ceid=US:en"
        r   = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=7)
        if r.status_code == 200:
            root = ET.fromstring(r.content)
            for item in root.findall(".//item")[:n]:
                t_el = item.find("title")
                d_el = item.find("pubDate")
                l_el = item.find("link")
                raw  = (t_el.text or "") if t_el is not None else ""
                headline = raw.split(" - ")[0].strip()
                if not headline or len(headline) < 5: continue
                score, reason = _score_headline(headline)
                geo   = any(kw in headline.lower() for kw in GEO_WORDS)
                label = "Bullish" if score > 0.1 else ("Bearish" if score < -0.1 else "Neutral")
                articles.append({
                    "headline": headline, "score": score, "label": label,
                    "reason":   reason,   "geo_risk": bool(geo),
                    "pub_date": (d_el.text or "")[:16] if d_el is not None else "",
                    "link":     (l_el.text or "")      if l_el is not None else "",
                })
    except Exception as e:
        print(f"  [news] {ticker}: {e}")
    if not articles:
        articles.append({"headline":"No recent news","score":0.0,"label":"Neutral",
                         "reason":"Feed unavailable","geo_risk":False,"pub_date":"","link":""})
    return articles

# ── Oracle + Verdict ─────────────────────────────────────────────────────────
def oracle_score(mos, rsi, vol_rel, roe, pm, sentiment) -> float:
    if   mos > 50:  v = 40
    elif mos > 20:  v = 30
    elif mos > 0:   v = 20
    elif mos > -20: v = 10
    else:           v = max(0.0, 10 + mos*0.08)
    t = min(30.0, max(0.0,(70-rsi)*0.5) + min(10.0, vol_rel*4))
    q = min(20.0, min(15.0, roe*0.4) + (5 if pm > 20 else 0))
    s = max(0.0, min(10.0, sentiment*10))
    return round(min(100.0, max(0.0, v+t+q+s)), 1)

def build_verdict(row: dict) -> str:
    parts = []
    mos  = _safe(row.get("margin_of_safety"))
    rsi  = _safe(row.get("RSI"), 50)
    roe  = _safe(row.get("ROE"))
    pm   = _safe(row.get("profit_margin"))
    si   = _safe(row.get("short_interest"))
    meth = str(row.get("dcf_method",""))
    if   mos > 30:  parts.append("GREEN STRONG VALUE: Deep discount to intrinsic value")
    elif mos > 10:  parts.append("YELLOW FAIR VALUE: Modest upside")
    elif mos < -20: parts.append("RED OVERVALUED: Above intrinsic value")
    elif "skipped" in meth or "no_" in meth:
        parts.append("GREY: DCF not applicable")
    if rsi < 30:   parts.append("OVERSOLD: RSI < 30")
    elif rsi > 70: parts.append("OVERBOUGHT: RSI > 70")
    if roe > 20 and pm > 15: parts.append("HIGH QUALITY: Strong ROE & margins")
    if si > 15: parts.append(f"SHORT PRESSURE: {si:.1f}% of float")
    if row.get("Geo_Risk"): parts.append("GEO RISK: Macro signals")
    r = str(row.get("LLM_Reasoning","")).strip()
    if r and len(r) > 10: parts.append(f"NEWS: {r[:100]}")
    return " | ".join(parts) if parts else "Neutral outlook"

# ── AI Report (OpenAI, optional) ─────────────────────────────────────────────
def generate_ai_report(ticker: str, data: dict, news: list, api_key: str) -> str:
    try:
        from openai import OpenAI
        client    = OpenAI(api_key=api_key)
        news_text = "\n".join([f"- {a['headline']} ({a['label']} {a['score']:+.2f})" for a in news[:3]])
        prompt    = (
            f"Write a concise institutional analyst memo for {ticker}.\n"
            f"Price: ${data.get('Price','N/A')} | Fair Value: ${data.get('intrinsic_value','N/A')} "
            f"| MoS: {data.get('margin_of_safety','N/A')}% | Oracle: {data.get('Oracle_Score','N/A')}\n"
            f"DCF Method: {data.get('dcf_method','N/A')} | RSI: {data.get('RSI','N/A')}\n"
            f"News:\n{news_text}\n\nBe direct. Valuation verdict, key risk, recommendation. Max 200 words."
        )
        resp = client.chat.completions.create(
            model="gpt-4o", messages=[{"role":"user","content":prompt}], max_tokens=300)
        return resp.choices[0].message.content
    except Exception as e:
        return f"AI report unavailable: {e}"

# ── Investment Engine ─────────────────────────────────────────────────────────
class InvestmentEngine:
    def __init__(self):
        self.universe = load_universe()
        self.tickers  = self.universe["Ticker"].tolist()
        print(f"Universe loaded: {len(self.tickers)} tickers")

    def _sector(self, ticker: str) -> str:
        r = self.universe[self.universe["Ticker"] == ticker]
        return str(r["Sector"].values[0]) if not r.empty else "Market"

    def load_data(self):
        if os.path.exists(CACHE_FILE):
            try:
                df = pd.read_csv(CACHE_FILE)
                if "Articles_JSON" in df.columns:
                    df["Articles_JSON"] = df["Articles_JSON"].fillna("[]").astype(str)
                return df, True
            except Exception as e:
                print(f"Cache load error: {e}")
        return pd.DataFrame(), False

    def fetch_market_data(self, progress_bar=None) -> pd.DataFrame:
        results = []
        total   = len(self.tickers)
        dcf_ok  = 0

        # Bulk OHLCV — one call for all tickers
        print(f"Bulk downloading {total} tickers...")
        bulk = None
        try:
            bulk = yf.download(
                self.tickers, period="6mo", group_by="ticker",
                auto_adjust=True, progress=False, threads=True,
            )
        except Exception as e:
            print(f"Bulk download error: {e}")

        for i, ticker in enumerate(self.tickers):
            try:
                if progress_bar:
                    progress_bar.progress((i+1)/total,
                                          text=f"Scanning {ticker}  ({i+1}/{total})")

                # ── OHLCV ──
                df = None
                if bulk is not None and not bulk.empty:
                    try:
                        if isinstance(bulk.columns, pd.MultiIndex):
                            lvl1 = bulk.columns.get_level_values(1)
                            if ticker in lvl1:
                                df = bulk.xs(ticker, axis=1, level=1).dropna(how="all")
                        else:
                            lvl0 = bulk.columns.get_level_values(0)
                            if ticker in lvl0:
                                df = bulk[ticker].dropna(how="all")
                    except Exception:
                        pass

                if df is None or len(df) < 20:
                    try:
                        raw = yf.download(ticker, period="6mo", auto_adjust=True, progress=False)
                        if isinstance(raw.columns, pd.MultiIndex):
                            raw.columns = [c[0] for c in raw.columns]
                        df = raw
                    except Exception:
                        pass

                if df is None or len(df) < 20:
                    print(f"  SKIP {ticker}: no price data")
                    continue

                df.columns    = [str(c).strip().title() for c in df.columns]
                close         = df.get("Close", df.iloc[:,0]).squeeze().dropna()
                high          = df.get("High",  close).squeeze()
                low           = df.get("Low",   close).squeeze()
                vol_s         = df.get("Volume", pd.Series(dtype=float)).squeeze()

                if len(close) < 20: continue
                price = _safe(close.iloc[-1])
                if price <= 0: continue

                rsi     = _rsi(close)
                atr     = _atr(high, low, close)
                avg_vol = _safe(vol_s.rolling(20).mean().iloc[-1], 1.0) if len(vol_s) >= 20 else 1.0
                vol_rel = min(_safe(vol_s.iloc[-1], avg_vol)/avg_vol if avg_vol > 0 else 1.0, 20.0)
                dv      = round((atr/price)*100, 2) if price > 0 else 2.0
                rlvl    = "Low" if dv < 2 else ("Medium" if dv < 5 else "High")

                # ── Single Ticker object ──
                stock = yf.Ticker(ticker)
                info  = {}
                try:
                    info = stock.info or {}
                except Exception as e:
                    print(f"  [{ticker}] info failed: {e}")

                sector = self._sector(ticker)
                dcf    = calc_dcf(stock, info, sector=sector)
                if dcf["intrinsic_value"] > 0:
                    dcf_ok += 1

                articles  = fetch_news(ticker)
                avg_sent  = float(np.mean([a["score"] for a in articles]))
                geo_flag  = any(a["geo_risk"] for a in articles)
                arts_json = json.dumps(articles, ensure_ascii=True, separators=(",",":"))

                pe        = _safe(info.get("trailingPE"))
                roe       = _safe(info.get("returnOnEquity"))    * 100
                pm        = _safe(info.get("profitMargins"))     * 100
                short_pct = _safe(info.get("shortPercentOfFloat")) * 100
                pos_size  = min(round(100_000*0.02/max(atr*2,0.01)*price/100_000*100, 1), 50.0)

                record = {
                    "Ticker":           ticker,
                    "Sector":           sector,
                    "Price":            round(price, 2),
                    "RSI":              rsi,
                    "ATR":              round(atr, 4),
                    "Vol_Rel":          round(vol_rel, 2),
                    "PE":               round(pe, 2),
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
                    "Articles_JSON":    arts_json,
                    "Geo_Risk":         geo_flag,
                    "short_interest":   round(short_pct, 2),
                    "position_size":    pos_size,
                    "risk_level":       rlvl,
                    "daily_volatility": dv,
                    "Last_Updated":     datetime.now().strftime("%Y-%m-%d %H:%M"),
                }
                record["Oracle_Score"] = oracle_score(
                    dcf["margin_of_safety"], rsi, vol_rel, roe, pm, avg_sent)
                record["AI_Verdict"]   = build_verdict(record)
                results.append(record)

                time.sleep(RATE_DELAY)

            except Exception as e:
                print(f"  [ERR] {ticker}: {e}")
                continue

        df_out = pd.DataFrame(results)
        if not df_out.empty:
            print(f"Done: {len(df_out)}/{total} | DCF: {dcf_ok}/{len(df_out)}")
            df_out.to_csv(CACHE_FILE, index=False)
        return df_out
