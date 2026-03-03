"""
app.py  –  Apex Markets v4.0
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import json, os, ast
from logic import InvestmentEngine

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Apex Markets",
    layout="wide",
    page_icon="◈",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# CSS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,wght@0,300;0,400;0,500;0,600;0,700;1,400&family=DM+Mono:wght@400;500&display=swap');

html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
.stApp { background: #09090b; color: #e4e4e7; }

/* Sidebar */
section[data-testid="stSidebar"] {
  background: #0f0f12 !important;
  border-right: 1px solid #1f1f23 !important;
}
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] span { color: #71717a !important; }
section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3 { color: #a1a1aa !important; }

/* Primary button */
.stButton>button[kind="primary"] {
  background: #2563eb !important; color: #fff !important;
  border: none; border-radius: 8px; font-weight: 600;
  width: 100%; padding: 10px; font-size: 13px;
  letter-spacing: .03em; transition: background .2s;
}
.stButton>button[kind="primary"]:hover { background: #1d4ed8 !important; }

/* Secondary button */
.stButton>button:not([kind="primary"]) {
  background: #18181b !important; color: #71717a !important;
  border: 1px solid #27272a !important; border-radius: 8px;
  width: 100%; padding: 9px; font-size: 12px; transition: all .2s;
}
.stButton>button:not([kind="primary"]):hover {
  border-color: #3b82f6 !important; color: #3b82f6 !important;
}

/* Tabs */
.stTabs [data-baseweb="tab-list"] {
  background: transparent; border-bottom: 1px solid #27272a; gap: 0;
}
.stTabs [data-baseweb="tab"] {
  background: transparent; color: #52525b; font-size: 12px;
  font-weight: 500; letter-spacing: .05em; padding: 12px 22px;
  border-bottom: 2px solid transparent; transition: all .2s;
}
.stTabs [aria-selected="true"] {
  color: #3b82f6 !important; border-bottom-color: #3b82f6 !important;
  background: transparent !important;
}

/* Metrics */
[data-testid="metric-container"] {
  background: #111113; border: 1px solid #1f1f23;
  border-radius: 10px; padding: 14px 18px; transition: border-color .2s;
}
[data-testid="metric-container"]:hover { border-color: #3b82f6; }
[data-testid="metric-container"] label {
  color: #52525b !important; font-size: 10px !important;
  text-transform: uppercase; letter-spacing: .08em;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
  color: #f4f4f5 !important; font-size: 24px !important; font-weight: 700 !important;
}

/* DataFrames */
[data-testid="stDataFrame"] {
  border-radius: 10px; overflow: hidden; border: 1px solid #1f1f23;
}

/* Expanders */
.streamlit-expanderHeader {
  background: #111113 !important; border: 1px solid #1f1f23 !important;
  border-radius: 8px !important; color: #e4e4e7 !important;
  font-size: 13px !important; font-weight: 500 !important;
}
.streamlit-expanderContent {
  background: #0d0d10 !important; border: 1px solid #1f1f23 !important;
  border-top: none !important; border-radius: 0 0 8px 8px !important;
}

/* Article cards */
.art-card {
  background: #111113; border: 1px solid #1f1f23;
  border-radius: 8px; padding: 12px 14px; margin-bottom: 7px;
  display: flex; gap: 12px; align-items: flex-start;
}
.art-bar { width: 3px; min-height: 100%; border-radius: 2px; flex-shrink: 0; }
.art-bull { background: #22c55e; }
.art-bear { background: #ef4444; }
.art-neut { background: #52525b; }
.art-body { flex: 1; min-width: 0; }
.art-title {
  font-size: 13px; font-weight: 500; color: #e4e4e7;
  line-height: 1.4; margin-bottom: 5px;
}
.art-meta { font-size: 11px; color: #71717a; display: flex; gap: 8px; flex-wrap: wrap; }
.art-score { font-family: 'DM Mono', monospace; }
.badge {
  display: inline-block; font-size: 9px; font-weight: 700;
  letter-spacing: .06em; padding: 2px 7px; border-radius: 100px; text-transform: uppercase;
}
.badge-bull { background: #14532d; color: #4ade80; }
.badge-bear { background: #450a0a; color: #f87171; }
.badge-neut { background: #27272a; color: #a1a1aa; }

/* Ticker chips */
.chip-wrap { display: flex; flex-wrap: wrap; gap: 5px; padding: 4px 0; max-height: 200px; overflow-y: auto; }
.chip {
  background: #18181b; border: 1px solid #27272a; border-radius: 5px;
  padding: 2px 9px; font-size: 11px; font-family: 'DM Mono', monospace; color: #a1a1aa;
}

/* Header */
.apex-hdr { padding: 2px 0 18px; border-bottom: 1px solid #1f1f23; margin-bottom: 22px; }
.apex-title { font-size: 20px; font-weight: 700; color: #f4f4f5; letter-spacing: -.02em; }
.apex-sub   { font-size: 11px; color: #3f3f46; letter-spacing: .06em; margin-left: 10px; }

/* Divider */
hr { border-color: #1f1f23 !important; }
h2, h3 { color: #e4e4e7 !important; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def dark_layout(fig, title=""):
    fig.update_layout(
        title=dict(text=title, font=dict(size=13, color="#a1a1aa", family="DM Sans"), x=0.0),
        paper_bgcolor="#111113", plot_bgcolor="#0d0d10",
        font=dict(family="DM Sans", color="#71717a", size=11),
        margin=dict(l=10, r=10, t=36, b=10),
        legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="#27272a", borderwidth=1,
                    font=dict(size=10)),
        xaxis=dict(gridcolor="#1a1a1f", zerolinecolor="#27272a", linecolor="#27272a"),
        yaxis=dict(gridcolor="#1a1a1f", zerolinecolor="#27272a", linecolor="#27272a"),
    )
    return fig


def parse_articles(raw) -> list:
    """Parse articles JSON — handles CSV round-trip mangling."""
    if not raw or not isinstance(raw, str):
        return []
    raw = raw.strip()
    if raw in ("[]", "", "nan"):
        return []
    for fn in (json.loads, lambda s: json.loads(s.strip('"').replace('""', '"')),
               ast.literal_eval):
        try:
            r = fn(raw)
            if isinstance(r, list) and r:
                return r
        except Exception:
            continue
    return []


def art_html(articles: list) -> str:
    if not articles:
        return '<p style="color:#52525b;font-size:12px">No news data — run a fresh scan.</p>'
    out = []
    for a in articles:
        label  = a.get("label", "Neutral")
        score  = a.get("score", 0.0)
        reason = a.get("reason", a.get("reasoning", ""))[:90]
        date   = a.get("pub_date", "")[:12]
        link   = a.get("link", "")
        headline = a.get("headline", "")

        bar_cls   = {"Bullish": "art-bull", "Bearish": "art-bear"}.get(label, "art-neut")
        badge_cls = {"Bullish": "badge-bull","Bearish": "badge-bear"}.get(label, "badge-neut")
        score_col = "#22c55e" if score > 0.1 else ("#ef4444" if score < -0.1 else "#71717a")

        title_html = (f'<a href="{link}" target="_blank" style="color:#e4e4e7;text-decoration:none">'
                      f'{headline}</a>' if link else headline)
        out.append(
            f'<div class="art-card">'
            f'  <div class="art-bar {bar_cls}"></div>'
            f'  <div class="art-body">'
            f'    <div class="art-title">{title_html}</div>'
            f'    <div class="art-meta">'
            f'      <span class="badge {badge_cls}">{label}</span>'
            f'      <span class="art-score" style="color:{score_col}">{score:+.2f}</span>'
            f'      {f"<span>{date}</span>" if date else ""}'
            f'      {f"<span style=color:#52525b>{reason}</span>" if reason else ""}'
            f'    </div>'
            f'  </div>'
            f'</div>'
        )
    return "".join(out)


def chips_html(tickers: list) -> str:
    body = "".join(f'<span class="chip">{t}</span>' for t in sorted(tickers))
    return f'<div class="chip-wrap">{body}</div>'


def _safe(val, default=0.0):
    try:
        f = float(val)
        return default if (f != f or not np.isfinite(f)) else f
    except Exception:
        return default


# ─────────────────────────────────────────────────────────────────────────────
# ENGINE & SESSION
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_resource
def get_engine():
    return InvestmentEngine()

engine = get_engine()

for key, default in [("data", pd.DataFrame()), ("cache_loaded", False)]:
    if key not in st.session_state:
        st.session_state[key] = default

if st.session_state["data"].empty and not st.session_state["cache_loaded"]:
    df_c, ok = engine.load_data()
    st.session_state["data"]         = df_c
    st.session_state["cache_loaded"] = True


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ◈ Apex Markets")
    st.markdown("---")

    if st.button("⟳  Run Global Scan", type="primary"):
        with st.spinner("Running full market scan…"):
            pb = st.progress(0, "Starting…")
            st.session_state["data"] = engine.fetch_market_data(pb)
            pb.empty()
        st.success(f"Scan complete — {len(st.session_state['data'])} assets analysed")
        st.rerun()

    if st.button("🗑  Clear Cache"):
        for f in ["market_cache.csv"]:
            if os.path.exists(f):
                os.remove(f)
        st.session_state["data"]         = pd.DataFrame()
        st.session_state["cache_loaded"] = False
        st.rerun()

    st.markdown("---")

    df_raw = st.session_state["data"]
    if not df_raw.empty:
        st.markdown("**Filters**")
        all_sectors = sorted(df_raw["Sector"].dropna().unique()) if "Sector" in df_raw.columns else []
        all_regions = sorted(df_raw["Region"].dropna().unique()) if "Region" in df_raw.columns else []
        all_risks   = sorted(df_raw["risk_level"].dropna().unique()) if "risk_level" in df_raw.columns else []

        sel_sectors = st.multiselect("Sector",     all_sectors, default=all_sectors)
        sel_regions = st.multiselect("Region",     all_regions, default=all_regions)
        sel_risks   = st.multiselect("Risk Level", all_risks,   default=all_risks)
        min_score   = st.slider("Min Oracle Score", 0, 100, 0)
    else:
        sel_sectors = sel_regions = sel_risks = []
        min_score   = 0

    st.markdown("---")
    st.caption("Apex Markets v4.0")
    if not df_raw.empty and "Last_Updated" in df_raw.columns:
        st.caption(f"Last scan: {df_raw['Last_Updated'].iloc[0]}")


# ─────────────────────────────────────────────────────────────────────────────
# FILTER
# ─────────────────────────────────────────────────────────────────────────────
df = st.session_state["data"]
if not df.empty:
    fdf = df.copy()
    if sel_sectors and "Sector" in fdf.columns:
        fdf = fdf[fdf["Sector"].isin(sel_sectors)]
    if sel_regions and "Region" in fdf.columns:
        fdf = fdf[fdf["Region"].isin(sel_regions)]
    if sel_risks and "risk_level" in fdf.columns:
        fdf = fdf[fdf["risk_level"].isin(sel_risks)]
    if "Oracle_Score" in fdf.columns:
        fdf = fdf[fdf["Oracle_Score"] >= min_score]
    fdf = fdf.reset_index(drop=True)
else:
    fdf = df


# ─────────────────────────────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(
    '<div class="apex-hdr">'
    '<span class="apex-title">◈ Apex Markets</span>'
    '<span class="apex-sub">GLOBAL INVESTMENT INTELLIGENCE</span>'
    '</div>',
    unsafe_allow_html=True
)


# ─────────────────────────────────────────────────────────────────────────────
# KPI BAR
# ─────────────────────────────────────────────────────────────────────────────
if not fdf.empty:
    total   = len(fdf)
    hc_df   = fdf[fdf["Oracle_Score"] > 75]   if "Oracle_Score"    in fdf.columns else fdf.iloc[:0]
    uv_df   = fdf[fdf["margin_of_safety"] > 20] if "margin_of_safety" in fdf.columns else fdf.iloc[:0]
    geo_df  = fdf[fdf["Geo_Risk"] == True]      if "Geo_Risk"         in fdf.columns else fdf.iloc[:0]
    hvol_df = fdf[fdf["risk_level"] == "High"]  if "risk_level"       in fdf.columns else fdf.iloc[:0]
    dcf_df  = fdf[fdf["intrinsic_value"] > 0]   if "intrinsic_value"  in fdf.columns else fdf.iloc[:0]

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Scanned",     total,        help="Assets passing current filters")
    c2.metric("High Conviction",   len(hc_df),   help="Oracle Score > 75")
    c3.metric("Undervalued",       len(uv_df),   help="DCF Margin of Safety > 20%")
    c4.metric("Geo-Risk Alerts",   len(geo_df),  help="Recent news has geopolitical keywords")
    c5.metric("DCF Available",     len(dcf_df),  help="Stocks with a calculated intrinsic value")

    # Expandable ticker lists
    exp1, exp2, exp3 = st.columns(3)
    with exp1:
        if len(hc_df) > 0:
            with st.expander(f"▸ {len(hc_df)} High Conviction tickers"):
                st.markdown(chips_html(hc_df["Ticker"].tolist()), unsafe_allow_html=True)
    with exp2:
        if len(uv_df) > 0:
            with st.expander(f"▸ {len(uv_df)} Undervalued tickers"):
                st.markdown(chips_html(uv_df["Ticker"].tolist()), unsafe_allow_html=True)
    with exp3:
        if len(dcf_df) > 0:
            with st.expander(f"▸ {len(dcf_df)} tickers with DCF data"):
                st.markdown(chips_html(dcf_df["Ticker"].tolist()), unsafe_allow_html=True)

    if len(dcf_df) == 0 and total > 0:
        st.warning(
            "⚠️ **No DCF data yet.** The previous cache had 0 intrinsic values. "
            "Click **🗑 Clear Cache** then **⟳ Run Global Scan** to recalculate from scratch. "
            "The new engine prints diagnostic logs to the terminal so you can verify which "
            "yfinance attributes it finds."
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ─────────────────────────────────────────────────────────────────────────
    # TABS
    # ─────────────────────────────────────────────────────────────────────────
    tab1, tab2, tab3, tab4 = st.tabs([
        "◈  Terminal",
        "⬡  Risk Lab",
        "◎  Deep Dive",
        "◻  Market Intel",
    ])

    # ═══════════════════════════════════════════════════════════════════════
    # TAB 1 — TERMINAL
    # ═══════════════════════════════════════════════════════════════════════
    with tab1:
        st.markdown("### Investment Terminal")
        sorted_df = fdf.sort_values("Oracle_Score", ascending=False) if "Oracle_Score" in fdf.columns else fdf

        show_cols = [c for c in ["Ticker","Region","Sector","Price","intrinsic_value",
                                  "margin_of_safety","RSI","Oracle_Score","risk_level",
                                  "dcf_confidence"] if c in sorted_df.columns]
        cfg = {
            "Price":            st.column_config.NumberColumn("Price",      format="$%.2f"),
            "intrinsic_value":  st.column_config.NumberColumn("Fair Value", format="$%.2f",
                                    help="DCF intrinsic value per share. N/A for ETFs/commodities."),
            "margin_of_safety": st.column_config.NumberColumn("MoS %",
                                    help="+ve = undervalued vs DCF fair value. 0 = no DCF data."),
            "Oracle_Score":     st.column_config.ProgressColumn("Oracle",
                                    min_value=0, max_value=100, format="%d",
                                    help="Valuation 40% + Technical 30% + Quality 20% + Sentiment 10%"),
            "RSI":              st.column_config.NumberColumn("RSI",
                                    help="<30 oversold · >70 overbought"),
            "risk_level":       st.column_config.TextColumn("Risk",
                                    help="Based on ATR / Price ratio"),
            "dcf_confidence":   st.column_config.TextColumn("DCF",
                                    help="High=4+ yrs data · Medium=2-3 yrs · N/A=no cash-flow data"),
        }
        st.dataframe(sorted_df[show_cols], column_config=cfg,
                     use_container_width=True, hide_index=True, height=400)

        st.markdown("---")
        st.markdown("### 🏆 Top 5 Picks")
        for _, row in sorted_df.head(5).iterrows():
            with st.expander(
                f"**{row['Ticker']}** — Score {row.get('Oracle_Score','–')} · "
                f"MoS {_safe(row.get('margin_of_safety')):.1f}% · {row.get('risk_level','–')} risk"
            ):
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Price",    f"${_safe(row['Price']):.2f}")
                iv = _safe(row.get("intrinsic_value"))
                m2.metric("Fair Value", f"${iv:.2f}" if iv > 0 else "N/A")
                m3.metric("RSI",      f"{_safe(row.get('RSI'), 50):.1f}")
                m4.metric("MoS %",    f"{_safe(row.get('margin_of_safety')):.1f}%")
                st.info(row.get("AI_Verdict", "Analysis pending"))
                arts = parse_articles(row.get("Articles_JSON", "[]"))
                if arts:
                    st.markdown("**Latest News**")
                    st.markdown(art_html(arts[:3]), unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════════════════
    # TAB 2 — RISK LAB
    # ═══════════════════════════════════════════════════════════════════════
    with tab2:
        st.markdown("### Risk Laboratory")

        # ── 2.1 Position Sizing Matrix ────────────────────────────────────
        if {"daily_volatility", "position_size", "Oracle_Score", "risk_level"}.issubset(fdf.columns):
            st.markdown("#### Position Sizing Matrix")
            st.caption(
                "Each bubble = one stock. **X axis**: daily volatility (lower = safer). "
                "**Y axis**: recommended allocation % of a $100k portfolio. "
                "**Bubble size**: Oracle Score (bigger = better opportunity). "
                "**Colour**: 🟢 Low / 🟡 Medium / 🔴 High risk. "
                "Ideal stocks sit **bottom-left** (calm & appropriately sized)."
            )
            pxm = fdf.copy()
            pxm["bubble"] = pxm["Oracle_Score"].clip(lower=5)
            fig_pos = px.scatter(
                pxm, x="daily_volatility", y="position_size",
                size="bubble", color="risk_level",
                hover_name="Ticker",
                hover_data={"Oracle_Score": True, "margin_of_safety": True,
                            "Sector": True, "bubble": False},
                color_discrete_map={"Low": "#22c55e", "Medium": "#f59e0b",
                                    "High": "#ef4444", "Unknown": "#52525b"},
                labels={"daily_volatility": "Daily Volatility % (ATR/Price × 100)",
                        "position_size":    "Recommended Allocation %"},
            )
            xmid = fdf["daily_volatility"].median()
            ymid = fdf["position_size"].median()
            fig_pos.add_vline(x=xmid, line_dash="dot", line_color="#3f3f46")
            fig_pos.add_hline(y=ymid, line_dash="dot", line_color="#3f3f46")
            # Zone labels at corners
            xmax = fdf["daily_volatility"].quantile(0.98) * 1.05
            ymax = fdf["position_size"].max() * 1.05
            for txt, ax, ay, col in [
                ("✅ Sweet spot", xmax * 0.04, ymax * 0.06, "#22c55e"),
                ("⚠️ Large, low-risk", xmax * 0.04, ymax * 0.93, "#f59e0b"),
                ("🔴 High risk", xmax * 0.73, ymax * 0.93, "#ef4444"),
                ("⚡ Volatile, small", xmax * 0.73, ymax * 0.06, "#a1a1aa"),
            ]:
                fig_pos.add_annotation(x=ax, y=ay, text=txt, showarrow=False,
                                       font=dict(size=10, color=col),
                                       bgcolor="rgba(0,0,0,0.6)", borderpad=3)
            fig_pos = dark_layout(fig_pos)
            st.plotly_chart(fig_pos, use_container_width=True)

        # ── 2.2 Opportunity Heatmap ───────────────────────────────────────
        if {"RSI", "margin_of_safety", "Oracle_Score"}.issubset(fdf.columns):
            st.markdown("#### Opportunity Heatmap")
            st.caption(
                "**X axis**: DCF valuation upside % — positive = undervalued, negative = overvalued. "
                "**Y axis**: RSI momentum — below 30 = oversold, above 70 = overbought. "
                "**Ideal zone** (green shading): positive X + RSI below 70. "
                "**Colour gradient**: red = low Oracle Score → green = high Oracle Score."
            )
            dcf_count = (fdf["intrinsic_value"] > 0).sum() if "intrinsic_value" in fdf.columns else 0
            if dcf_count == 0:
                st.error("🔴 All stocks show 0% margin of safety — DCF data is missing. "
                         "Use **🗑 Clear Cache** → **⟳ Run Global Scan** to fix.")
            else:
                st.success(f"✅ {dcf_count}/{len(fdf)} stocks have DCF valuations.")

            mos_vals = fdf["margin_of_safety"].dropna()
            x_lo = max(mos_vals.quantile(0.02) * 1.1, -300)
            x_hi = min(mos_vals.quantile(0.98) * 1.1,  500)
            if abs(x_hi - x_lo) < 5:
                x_lo, x_hi = -10, 10  # fallback when all zeros

            heat = fdf.copy()
            heat["bubble"] = heat["Oracle_Score"].clip(lower=5)
            fig_heat = px.scatter(
                heat, x="margin_of_safety", y="RSI",
                size="bubble", color="Oracle_Score",
                hover_name="Ticker",
                hover_data={"Sector": True, "Price": True, "intrinsic_value": True,
                            "dcf_confidence": True, "risk_level": True, "bubble": False},
                color_continuous_scale=[(0,"#ef4444"),(0.5,"#f59e0b"),(1,"#22c55e")],
                range_color=[0, 100],
                labels={"margin_of_safety": "Valuation Upside %",
                        "RSI":              "Momentum (RSI)"},
            )
            # Green ideal zone
            fig_heat.add_shape(type="rect", x0=0, x1=x_hi*1.2, y0=0, y1=70,
                               fillcolor="rgba(34,197,94,0.05)", line_width=0)
            # Red danger zone
            fig_heat.add_shape(type="rect", x0=x_lo*1.2, x1=0, y0=70, y1=100,
                               fillcolor="rgba(239,68,68,0.05)", line_width=0)
            fig_heat.add_hline(y=70, line_dash="dot", line_color="#ef4444", line_width=1.5,
                               annotation_text="Overbought RSI 70",
                               annotation_font=dict(color="#ef4444", size=10),
                               annotation_position="right")
            fig_heat.add_hline(y=30, line_dash="dot", line_color="#22c55e", line_width=1.5,
                               annotation_text="Oversold RSI 30",
                               annotation_font=dict(color="#22c55e", size=10),
                               annotation_position="right")
            fig_heat.add_vline(x=0, line_dash="dash", line_color="#52525b", line_width=1,
                               annotation_text="Fair Value",
                               annotation_font=dict(color="#52525b", size=10),
                               annotation_position="top")
            fig_heat.update_layout(
                xaxis=dict(range=[x_lo, x_hi]),
                coloraxis_colorbar=dict(
                    title="Oracle Score",
                    tickvals=[0, 25, 50, 75, 100],
                    ticktext=["0 Poor","25","50","75","100 Best"],
                    tickfont=dict(color="#71717a", size=9),
                    title_font=dict(color="#a1a1aa", size=10),
                )
            )
            fig_heat = dark_layout(fig_heat)
            st.plotly_chart(fig_heat, use_container_width=True)

        # ── 2.3 Volatility Distribution with stock labels ─────────────────
        if "daily_volatility" in fdf.columns and "Ticker" in fdf.columns:
            st.markdown("#### Portfolio Volatility Distribution")
            st.caption(
                "Each bar = number of stocks in that volatility bucket. "
                "**Hover a bar** to see which tickers fall in it. "
                "Green band = Low risk (<2%), Amber = Medium (2–5%), Red = High (>5%). "
                "A chart heavy on the left = a safer portfolio overall."
            )

            # Build bucketed data so we can show tickers per bar
            vd = fdf[["Ticker", "daily_volatility", "risk_level"]].copy().dropna()
            vd["daily_volatility"] = pd.to_numeric(vd["daily_volatility"], errors="coerce")
            vd = vd.dropna()

            # Dynamic bin width
            vmax = vd["daily_volatility"].quantile(0.98)
            nbins = max(15, min(40, int(len(vd) / 2)))
            bin_edges = np.linspace(0, max(vmax * 1.1, 10), nbins + 1)

            vd["bin_idx"] = pd.cut(vd["daily_volatility"], bins=bin_edges, labels=False)
            bucket_data = (
                vd.groupby("bin_idx", observed=True)
                  .agg(count=("Ticker", "count"),
                       tickers=("Ticker", lambda x: ", ".join(sorted(x))))
                  .reset_index()
            )
            bucket_data["bin_mid"]   = [(bin_edges[int(i)] + bin_edges[int(i)+1]) / 2
                                         for i in bucket_data["bin_idx"]]
            bucket_data["bin_label"] = [f"{bin_edges[int(i)]:.1f}–{bin_edges[int(i)+1]:.1f}%"
                                         for i in bucket_data["bin_idx"]]
            bucket_data["color"]     = bucket_data["bin_mid"].apply(
                lambda v: "#22c55e" if v < 2 else ("#f59e0b" if v < 5 else "#ef4444")
            )

            fig_vol = go.Figure()
            for _, br in bucket_data.iterrows():
                fig_vol.add_trace(go.Bar(
                    x=[br["bin_mid"]], y=[br["count"]],
                    width=[(bin_edges[1] - bin_edges[0]) * 0.85],
                    marker_color=br["color"], marker_opacity=0.8,
                    name=br["bin_label"],
                    hovertemplate=(
                        f"<b>Volatility: {br['bin_label']}</b><br>"
                        f"Count: {br['count']}<br>"
                        f"Stocks: {br['tickers']}<extra></extra>"
                    ),
                    showlegend=False,
                ))

            # Zone shading
            fig_vol.add_vrect(x0=0, x1=2,     fillcolor="rgba(34,197,94,0.05)",  line_width=0)
            fig_vol.add_vrect(x0=2, x1=5,     fillcolor="rgba(245,158,11,0.05)", line_width=0)
            fig_vol.add_vrect(x0=5, x1=max(vmax*1.2, 10),
                              fillcolor="rgba(239,68,68,0.05)", line_width=0)
            fig_vol.add_vline(x=2, line_dash="dot", line_color="#22c55e", line_width=1,
                              annotation_text="Low/Med boundary (2%)",
                              annotation_font=dict(color="#22c55e", size=9),
                              annotation_position="top right")
            fig_vol.add_vline(x=5, line_dash="dot", line_color="#f59e0b", line_width=1,
                              annotation_text="Med/High boundary (5%)",
                              annotation_font=dict(color="#f59e0b", size=9),
                              annotation_position="top right")
            fig_vol.update_layout(
                xaxis_title="Daily Volatility %  (ATR / Price × 100)",
                yaxis_title="Number of Stocks",
                xaxis=dict(range=[0, max(vmax * 1.15, 10)]),
                bargap=0.04,
                hovermode="x unified",
            )
            fig_vol = dark_layout(fig_vol)
            st.plotly_chart(fig_vol, use_container_width=True)

            # Summary metrics
            lc1, lc2, lc3 = st.columns(3)
            low_t  = vd[vd["daily_volatility"] < 2]["Ticker"].tolist()
            med_t  = vd[(vd["daily_volatility"] >= 2) & (vd["daily_volatility"] < 5)]["Ticker"].tolist()
            high_t = vd[vd["daily_volatility"] >= 5]["Ticker"].tolist()
            lc1.metric("🟢 Low Risk (<2%)",    len(low_t),
                       help="Moves less than 2% per day on average")
            lc2.metric("🟡 Medium Risk (2–5%)", len(med_t),
                       help="Moderate daily swings")
            lc3.metric("🔴 High Risk (>5%)",   len(high_t),
                       help="Highly volatile — size positions accordingly")

            # Show which tickers in each bucket
            bx1, bx2, bx3 = st.columns(3)
            with bx1:
                if low_t:
                    with st.expander(f"▸ Low risk tickers ({len(low_t)})"):
                        st.markdown(chips_html(low_t), unsafe_allow_html=True)
            with bx2:
                if med_t:
                    with st.expander(f"▸ Medium risk tickers ({len(med_t)})"):
                        st.markdown(chips_html(med_t), unsafe_allow_html=True)
            with bx3:
                if high_t:
                    with st.expander(f"▸ High risk tickers ({len(high_t)})"):
                        st.markdown(chips_html(high_t), unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════════════════
    # TAB 3 — DEEP DIVE
    # ═══════════════════════════════════════════════════════════════════════
    with tab3:
        st.markdown("### Deep Dive")
        if not fdf.empty:
            selected = st.selectbox(
                "Select asset",
                sorted(fdf["Ticker"].tolist()),
                format_func=lambda t: (
                    f"{t}  —  "
                    f"Score {int(_safe(fdf.loc[fdf['Ticker']==t,'Oracle_Score'].values[0]))}  ·  "
                    f"MoS {_safe(fdf.loc[fdf['Ticker']==t,'margin_of_safety'].values[0]):.1f}%"
                )
            )
            row = fdf[fdf["Ticker"] == selected].iloc[0]

            # ── Key metrics ───────────────────────────────────────────────
            m1, m2, m3, m4, m5, m6 = st.columns(6)
            price = _safe(row["Price"])
            iv    = _safe(row.get("intrinsic_value"))
            mos   = _safe(row.get("margin_of_safety"))
            rsi   = _safe(row.get("RSI"), 50)
            score = _safe(row.get("Oracle_Score"))
            risk  = row.get("risk_level", "–")

            m1.metric("Price",       f"${price:.2f}")
            m2.metric("Fair Value",  f"${iv:.2f}" if iv > 0 else "N/A",
                      help="DCF intrinsic value. N/A = no cash-flow history available.")
            m3.metric("MoS %",       f"{mos:.1f}%",
                      help="Margin of Safety. Positive = undervalued. 0% = no DCF data.")
            m4.metric("Oracle",      f"{score:.0f}")
            m5.metric("RSI",         f"{rsi:.1f}",
                      help="Relative Strength Index. <30 oversold · >70 overbought")
            m6.metric("Risk",        risk)

            dcf_method = row.get("dcf_method", "")
            if iv == 0:
                if "no_cf_data" in str(dcf_method):
                    st.warning("⚠️ **DCF unavailable**: yfinance found no cash-flow statement "
                               "for this ticker. ETFs, commodities and some foreign listings "
                               "don't report cash flows. Run a fresh scan to retry.")
                elif "all_negative_cf" in str(dcf_method):
                    st.warning("⚠️ **DCF unavailable**: All historical FCF values are negative "
                               "— typical for early-stage growth companies.")
                elif "no_shares_data" in str(dcf_method):
                    st.warning("⚠️ **DCF unavailable**: Could not retrieve shares-outstanding data.")

            st.markdown("---")

            # ── Football field ────────────────────────────────────────────
            st.markdown("#### Valuation Football Field")
            if iv > 0:
                fig_ff = go.Figure()
                bands = [
                    (iv * 0.5, iv * 0.8,  "rgba(59,130,246,0.12)",  "Deep Value Zone  (–50% to –20%)"),
                    (iv * 0.8, iv * 1.2,  "rgba(34,197,94,0.15)",   "Fair Value Zone  (±20%)"),
                    (iv * 1.2, iv * 1.5,  "rgba(245,158,11,0.10)",  "Premium Zone  (+20% to +50%)"),
                    (iv * 1.5, iv * 2.0,  "rgba(239,68,68,0.08)",   "Overvalued Zone  (>+50%)"),
                ]
                for y0, y1, col, name in bands:
                    fig_ff.add_shape(type="rect", x0=0, x1=1, y0=y0, y1=y1,
                                     fillcolor=col, line_width=0)
                    fig_ff.add_annotation(x=0.97, y=(y0 + y1) / 2, text=name,
                                          showarrow=False, xanchor="right",
                                          font=dict(size=9, color="#71717a"))
                fig_ff.add_hline(y=price, line_dash="dash", line_color="#ef4444", line_width=2,
                                 annotation_text=f"Current  ${price:.2f}",
                                 annotation_font=dict(color="#ef4444"),
                                 annotation_position="right")
                fig_ff.add_hline(y=iv, line_color="#3b82f6", line_width=1.5,
                                 annotation_text=f"Intrinsic  ${iv:.2f}",
                                 annotation_font=dict(color="#3b82f6"),
                                 annotation_position="right")
                gr = _safe(row.get("growth_rate"))
                conf = row.get("dcf_confidence", "–")
                fig_ff.update_layout(
                    height=360, showlegend=False,
                    xaxis=dict(showticklabels=False, showgrid=False, zeroline=False),
                    yaxis_title="Price ($)",
                    title=dict(text=f"{selected} — DCF confidence: {conf} · FCF growth rate: {gr:.1f}%",
                               font=dict(size=11, color="#71717a"))
                )
                fig_ff = dark_layout(fig_ff)
                st.plotly_chart(fig_ff, use_container_width=True)
            else:
                st.info("Football field chart requires a successful DCF calculation.")

            st.markdown("---")

            # ── AI Verdict ────────────────────────────────────────────────
            st.markdown("#### AI Investment Verdict")
            verdict_txt = row.get("AI_Verdict", "")
            if verdict_txt and verdict_txt != "Neutral outlook — monitoring":
                st.info(verdict_txt)
            else:
                st.caption("No strong signals detected for this asset.")

            st.markdown("---")

            # ── News ──────────────────────────────────────────────────────
            st.markdown("#### Latest News Intelligence")
            st.caption("Up to 5 headlines with AI sentiment labels, scores, and one-line reasoning.")
            arts = parse_articles(row.get("Articles_JSON", "[]"))
            if arts and arts[0].get("headline", "") not in ("No recent news available", "No recent news found"):
                st.markdown(art_html(arts), unsafe_allow_html=True)
            else:
                st.warning(
                    "No article data stored for this ticker. "
                    "This means the news fetch failed during the scan (network timeout or "
                    "Google News returned no results). Run **🗑 Clear Cache** → "
                    "**⟳ Run Global Scan** to retry."
                )

            st.markdown("---")

            # ── Risk profile ──────────────────────────────────────────────
            st.markdown("#### Risk Profile")
            r1, r2, r3, r4 = st.columns(4)
            r1.metric("Allocation Rec.",  f"{_safe(row.get('position_size')):.1f}%",
                      help="% of $100k portfolio, based on 2×ATR stop-loss rule")
            r2.metric("Daily Volatility", f"{_safe(row.get('daily_volatility')):.2f}%",
                      help="ATR as % of price — expected daily price range")
            r3.metric("Short Interest",   f"{_safe(row.get('short_interest')):.1f}%",
                      help="% of float sold short. >15% = high bearish pressure")
            r4.metric("Short Sentiment",  row.get("insider_sentiment", "–"))

            if row.get("Geo_Risk"):
                st.warning("🌍 Geopolitical risk detected in recent news.")

            st.markdown(
                f"[Open on Yahoo Finance ↗](https://finance.yahoo.com/quote/{selected})"
            )

    # ═══════════════════════════════════════════════════════════════════════
    # TAB 4 — MARKET INTEL
    # ═══════════════════════════════════════════════════════════════════════
    with tab4:
        st.markdown("### Market Intelligence")

        # Sector breakdown
        if "Sector" in fdf.columns:
            st.markdown("#### Sector Breakdown")
            agg_map = {k: "mean" for k in ["Oracle_Score","margin_of_safety","RSI","daily_volatility"]
                       if k in fdf.columns}
            agg_map["Ticker"] = "count"
            s_agg = (fdf.groupby("Sector").agg(agg_map).round(1)
                       .rename(columns={"Ticker": "Count",
                                        "Oracle_Score": "Avg Oracle",
                                        "margin_of_safety": "Avg MoS %",
                                        "RSI": "Avg RSI",
                                        "daily_volatility": "Avg Vol %"}))
            st.dataframe(s_agg, use_container_width=True)

        # Region box plot
        if "Region" in fdf.columns and "Oracle_Score" in fdf.columns:
            st.markdown("#### Oracle Score by Region")
            st.caption("Box plot: median line, IQR box, whiskers = 1.5× IQR. "
                       "Wider spread = more divergence within that region.")
            fig_reg = px.box(
                fdf, x="Region", y="Oracle_Score", color="Region",
                color_discrete_sequence=["#3b82f6","#22c55e","#f59e0b","#a855f7","#ef4444"],
                points="all",   # show individual dots
                hover_name="Ticker",
                labels={"Oracle_Score": "Oracle Score"},
            )
            fig_reg.update_layout(showlegend=False)
            fig_reg = dark_layout(fig_reg)
            st.plotly_chart(fig_reg, use_container_width=True)

        # Value vs Momentum
        if {"RSI","margin_of_safety","Oracle_Score","Sector"}.issubset(fdf.columns):
            st.markdown("#### Value vs Momentum — Full Universe")
            st.caption(
                "**Ideal zone**: right of the dashed vertical (positive MoS = undervalued) "
                "AND below RSI 70 (not overbought). Bubble size = Oracle Score."
            )
            dcf_ok = (fdf["intrinsic_value"] > 0).sum() if "intrinsic_value" in fdf.columns else 0
            if dcf_ok == 0:
                st.warning("All margin-of-safety values are 0 (no DCF data). "
                           "Clear cache and rescan to fix.")
            mos2 = fdf["margin_of_safety"].dropna()
            xl2  = max(mos2.quantile(0.02) * 1.1, -200) if len(mos2) else -10
            xh2  = min(mos2.quantile(0.98) * 1.1,  400) if len(mos2) else 10
            if abs(xh2 - xl2) < 5:
                xl2, xh2 = -10, 10

            vm = fdf.copy()
            vm["bubble"] = vm["Oracle_Score"].clip(lower=5)
            fig_vm = px.scatter(
                vm, x="margin_of_safety", y="RSI",
                size="bubble", color="Sector",
                hover_name="Ticker",
                hover_data={"Price": True, "Oracle_Score": True, "bubble": False},
                labels={"margin_of_safety": "Valuation Upside %", "RSI": "Momentum (RSI)"},
            )
            fig_vm.add_hline(y=70, line_dash="dot", line_color="#ef4444",
                             annotation_text="Overbought",
                             annotation_font=dict(color="#ef4444", size=10))
            fig_vm.add_hline(y=30, line_dash="dot", line_color="#22c55e",
                             annotation_text="Oversold",
                             annotation_font=dict(color="#22c55e", size=10))
            fig_vm.add_vline(x=0, line_dash="dash", line_color="#52525b")
            fig_vm.update_layout(xaxis=dict(range=[xl2, xh2]))
            fig_vm = dark_layout(fig_vm)
            st.plotly_chart(fig_vm, use_container_width=True)

        # Sentiment
        if "Sentiment" in fdf.columns:
            st.markdown("#### Market Sentiment Overview")
            avg_s = fdf["Sentiment"].mean()
            sc1, sc2, sc3 = st.columns(3)
            sc1.metric("Avg Sentiment", f"{avg_s:.3f}",
                       delta="Bullish" if avg_s > 0 else "Bearish")
            if "Geo_Risk" in fdf.columns:
                sc2.metric("Geo-Risk Count", int(fdf["Geo_Risk"].sum()))
            if "Oracle_Score" in fdf.columns:
                hc_pct = len(fdf[fdf["Oracle_Score"] > 75]) / max(len(fdf), 1) * 100
                sc3.metric("High Conviction %", f"{hc_pct:.1f}%")

            # Sentiment distribution
            fig_sent = px.histogram(
                fdf, x="Sentiment", nbins=20,
                color_discrete_sequence=["#3b82f6"],
                labels={"Sentiment": "Avg Sentiment Score (–1 bearish → +1 bullish)"},
            )
            fig_sent.add_vline(x=0, line_dash="dash", line_color="#52525b")
            fig_sent = dark_layout(fig_sent, "Sentiment Distribution")
            st.plotly_chart(fig_sent, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# EMPTY STATE
# ─────────────────────────────────────────────────────────────────────────────
else:
    st.markdown("""
    <div style="max-width:480px;margin:72px auto;text-align:center">
      <div style="font-size:44px;margin-bottom:14px;color:#3b82f6">◈</div>
      <div style="font-size:20px;font-weight:700;color:#f4f4f5;margin-bottom:8px">Welcome to Apex Markets</div>
      <div style="color:#52525b;font-size:13px;line-height:1.7">
        Global investment intelligence across 160+ assets.<br>
        Press <strong style="color:#3b82f6">⟳ Run Global Scan</strong> in the sidebar to begin.
      </div>
    </div>
    """, unsafe_allow_html=True)

    fc1, fc2, fc3, fc4 = st.columns(4)
    for col, icon, title, desc in [
        (fc1, "◎", "DCF Engine",      "Auto-discovers yfinance API shape at runtime"),
        (fc2, "◈", "5 News Articles", "Per stock with sentiment score & reasoning"),
        (fc3, "⬡", "Risk Sizing",     "ATR-based allocation with interactive histogram"),
        (fc4, "◻", "160+ Globals",    "US, Europe, Asia, EM across all sectors"),
    ]:
        col.markdown(
            f'<div style="background:#111113;border:1px solid #1f1f23;border-radius:10px;'
            f'padding:18px;text-align:center">'
            f'<div style="font-size:26px;color:#3b82f6;margin-bottom:6px">{icon}</div>'
            f'<div style="font-weight:600;color:#e4e4e7;font-size:12px">{title}</div>'
            f'<div style="color:#52525b;font-size:11px;margin-top:3px">{desc}</div>'
            f'</div>',
            unsafe_allow_html=True
        )
