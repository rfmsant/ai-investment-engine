"""
app.py - Apex Markets v6.0
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import json, os, ast
from logic import InvestmentEngine

st.set_page_config(page_title="Apex Markets", layout="wide",
                   page_icon="◈", initial_sidebar_state="expanded")

# ===========================================================================
# CSS
# ===========================================================================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');
html,body,[class*="css"]{font-family:'DM Sans',sans-serif;}
.stApp{background:#09090b;color:#e4e4e7;}
section[data-testid="stSidebar"]{background:#0f0f12!important;border-right:1px solid #1f1f23!important;}
section[data-testid="stSidebar"] label,section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] span{color:#71717a!important;}
section[data-testid="stSidebar"] h1,section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3{color:#a1a1aa!important;}
.stButton>button[kind="primary"]{background:#2563eb!important;color:#fff!important;
  border:none;border-radius:8px;font-weight:600;width:100%;padding:10px;font-size:13px;transition:background .2s;}
.stButton>button[kind="primary"]:hover{background:#1d4ed8!important;}
.stButton>button:not([kind="primary"]){background:#18181b!important;color:#71717a!important;
  border:1px solid #27272a!important;border-radius:8px;width:100%;padding:9px;font-size:12px;transition:all .2s;}
.stButton>button:not([kind="primary"]):hover{border-color:#3b82f6!important;color:#3b82f6!important;}
.stTabs [data-baseweb="tab-list"]{background:transparent;border-bottom:1px solid #27272a;gap:0;}
.stTabs [data-baseweb="tab"]{background:transparent;color:#52525b;font-size:12px;font-weight:500;
  letter-spacing:.05em;padding:12px 22px;border-bottom:2px solid transparent;transition:all .2s;}
.stTabs [aria-selected="true"]{color:#3b82f6!important;border-bottom-color:#3b82f6!important;background:transparent!important;}
[data-testid="metric-container"]{background:#111113;border:1px solid #1f1f23;
  border-radius:10px;padding:14px 18px;transition:border-color .2s;}
[data-testid="metric-container"]:hover{border-color:#3b82f6;}
[data-testid="metric-container"] label{color:#52525b!important;font-size:10px!important;
  text-transform:uppercase;letter-spacing:.08em;}
[data-testid="metric-container"] [data-testid="stMetricValue"]{color:#f4f4f5!important;
  font-size:24px!important;font-weight:700!important;}
[data-testid="stDataFrame"]{border-radius:10px;overflow:hidden;border:1px solid #1f1f23;}
.streamlit-expanderHeader{background:#111113!important;border:1px solid #1f1f23!important;
  border-radius:8px!important;color:#e4e4e7!important;font-size:13px!important;font-weight:500!important;}
.streamlit-expanderContent{background:#0d0d10!important;border:1px solid #1f1f23!important;
  border-top:none!important;border-radius:0 0 8px 8px!important;}
.art-card{background:#111113;border:1px solid #1f1f23;border-radius:8px;padding:12px 14px;
  margin-bottom:7px;display:flex;gap:12px;align-items:flex-start;}
.art-bar{width:3px;min-height:100%;border-radius:2px;flex-shrink:0;}
.art-bull{background:#22c55e;}.art-bear{background:#ef4444;}.art-neut{background:#52525b;}
.art-body{flex:1;min-width:0;}
.art-title{font-size:13px;font-weight:500;color:#e4e4e7;line-height:1.4;margin-bottom:5px;}
.art-meta{font-size:11px;color:#71717a;display:flex;gap:8px;flex-wrap:wrap;}
.art-score{font-family:'DM Mono',monospace;}
.badge{display:inline-block;font-size:9px;font-weight:700;letter-spacing:.06em;
  padding:2px 7px;border-radius:100px;text-transform:uppercase;}
.badge-bull{background:#14532d;color:#4ade80;}
.badge-bear{background:#450a0a;color:#f87171;}
.badge-neut{background:#27272a;color:#a1a1aa;}
.chip-wrap{display:flex;flex-wrap:wrap;gap:5px;padding:4px 0;max-height:200px;overflow-y:auto;}
.chip{background:#18181b;border:1px solid #27272a;border-radius:5px;padding:2px 9px;
  font-size:11px;font-family:'DM Mono',monospace;color:#a1a1aa;}
.apex-hdr{padding:2px 0 18px;border-bottom:1px solid #1f1f23;margin-bottom:22px;}
.apex-title{font-size:20px;font-weight:700;color:#f4f4f5;letter-spacing:-.02em;}
.apex-sub{font-size:11px;color:#3f3f46;letter-spacing:.06em;margin-left:10px;}
hr{border-color:#1f1f23!important;}
h2,h3{color:#e4e4e7!important;}
</style>
""", unsafe_allow_html=True)


# ===========================================================================
# HELPERS
# ===========================================================================

def dark_layout(fig, title=""):
    fig.update_layout(
        title=dict(text=title, font=dict(size=13, color="#a1a1aa"), x=0.0),
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
    if not raw or not isinstance(raw, str):
        return []
    raw = raw.strip()
    if raw in ("[]", "", "nan"):
        return []
    for fn in (json.loads,
               lambda s: json.loads(s.strip('"').replace('""', '"')),
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
        return '<p style="color:#52525b;font-size:12px">No news data.</p>'
    out = []
    for a in articles:
        label    = a.get("label", "Neutral")
        score    = a.get("score", 0.0)
        reason   = a.get("reason", "")[:90]
        date     = a.get("pub_date", "")[:12]
        link     = a.get("link", "")
        headline = a.get("headline", "")
        bar_cls   = {"Bullish": "art-bull", "Bearish": "art-bear"}.get(label, "art-neut")
        badge_cls = {"Bullish": "badge-bull","Bearish": "badge-bear"}.get(label, "badge-neut")
        score_col = "#22c55e" if score > 0.1 else ("#ef4444" if score < -0.1 else "#71717a")
        title_html = (f'<a href="{link}" target="_blank" style="color:#e4e4e7;text-decoration:none">'
                      f'{headline}</a>' if link else headline)
        out.append(
            f'<div class="art-card"><div class="art-bar {bar_cls}"></div>'
            f'<div class="art-body"><div class="art-title">{title_html}</div>'
            f'<div class="art-meta"><span class="badge {badge_cls}">{label}</span>'
            f'<span class="art-score" style="color:{score_col}">{score:+.2f}</span>'
            f'{f"<span>{date}</span>" if date else ""}'
            f'{f"<span style=color:#52525b>{reason}</span>" if reason else ""}'
            f'</div></div></div>'
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


# ===========================================================================
# ENGINE & SESSION STATE
# ===========================================================================

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


# ===========================================================================
# SIDEBAR
# ===========================================================================
with st.sidebar:
    st.markdown("### Apex Markets")
    st.markdown("---")

    if st.button("Run Global Scan", type="primary"):
        with st.spinner("Running full market scan..."):
            pb = st.progress(0, "Starting...")
            st.session_state["data"] = engine.fetch_market_data(pb)
            pb.empty()
        st.success(f"Scan complete — {len(st.session_state['data'])} assets analysed")
        st.rerun()

    if st.button("Clear Cache"):
        if os.path.exists("market_cache.csv"):
            os.remove("market_cache.csv")
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
    st.caption("Apex Markets v6.0")
    if not df_raw.empty and "Last_Updated" in df_raw.columns:
        st.caption(f"Last scan: {df_raw['Last_Updated'].iloc[0]}")


# ===========================================================================
# FILTER
# ===========================================================================
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


# ===========================================================================
# HEADER
# ===========================================================================
st.markdown(
    '<div class="apex-hdr"><span class="apex-title">Apex Markets</span>'
    '<span class="apex-sub">GLOBAL INVESTMENT INTELLIGENCE</span></div>',
    unsafe_allow_html=True
)

# ===========================================================================
# MAIN CONTENT
# ===========================================================================
if not fdf.empty:
    total   = len(fdf)
    hc_df   = fdf[fdf["Oracle_Score"] > 75]    if "Oracle_Score"    in fdf.columns else fdf.iloc[:0]
    uv_df   = fdf[fdf["margin_of_safety"] > 20] if "margin_of_safety" in fdf.columns else fdf.iloc[:0]
    geo_df  = fdf[fdf["Geo_Risk"] == True]       if "Geo_Risk"         in fdf.columns else fdf.iloc[:0]
    dcf_df  = fdf[fdf["intrinsic_value"] > 0]    if "intrinsic_value"  in fdf.columns else fdf.iloc[:0]

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Scanned",   total)
    c2.metric("High Conviction", len(hc_df),  help="Oracle Score > 75")
    c3.metric("Undervalued",     len(uv_df),  help="Margin of Safety > 20%")
    c4.metric("Geo-Risk Alerts", len(geo_df), help="Geopolitical keywords in news")
    c5.metric("DCF Available",   len(dcf_df), help="Stocks with intrinsic value calculated")

    if len(dcf_df) == 0:
        st.error("All DCF values are 0. Click **Clear Cache** then **Run Global Scan**.")

    e1, e2, e3 = st.columns(3)
    with e1:
        if len(hc_df):
            with st.expander(f"{len(hc_df)} High Conviction tickers"):
                st.markdown(chips_html(hc_df["Ticker"].tolist()), unsafe_allow_html=True)
    with e2:
        if len(uv_df):
            with st.expander(f"{len(uv_df)} Undervalued tickers"):
                st.markdown(chips_html(uv_df["Ticker"].tolist()), unsafe_allow_html=True)
    with e3:
        if len(dcf_df):
            with st.expander(f"{len(dcf_df)} tickers with DCF"):
                st.markdown(chips_html(dcf_df["Ticker"].tolist()), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Terminal", "Risk Lab", "Deep Dive", "Market Intel", "Diagnostics"
    ])

    # =========================================================
    # TAB 1 — TERMINAL
    # =========================================================
    with tab1:
        st.markdown("### Investment Terminal")
        sdf = fdf.sort_values("Oracle_Score", ascending=False) if "Oracle_Score" in fdf.columns else fdf
        show_cols = [c for c in ["Ticker","Region","Sector","Price","intrinsic_value",
                                  "margin_of_safety","RSI","Oracle_Score","risk_level",
                                  "dcf_confidence","dcf_method"] if c in sdf.columns]
        cfg = {
            "Price":            st.column_config.NumberColumn("Price",      format="$%.2f"),
            "intrinsic_value":  st.column_config.NumberColumn("Fair Value", format="$%.2f"),
            "margin_of_safety": st.column_config.NumberColumn("MoS %"),
            "Oracle_Score":     st.column_config.ProgressColumn("Oracle", min_value=0, max_value=100, format="%d"),
            "dcf_method":       st.column_config.TextColumn("DCF Method"),
        }
        st.dataframe(sdf[show_cols], column_config=cfg,
                     use_container_width=True, hide_index=True, height=420)

        st.markdown("---")
        st.markdown("### Top 5 Picks")
        for _, row in sdf.head(5).iterrows():
            with st.expander(
                f"**{row['Ticker']}** — Score {row.get('Oracle_Score','?')} · "
                f"MoS {_safe(row.get('margin_of_safety')):.1f}% · {row.get('risk_level','?')} risk"
            ):
                m1,m2,m3,m4 = st.columns(4)
                iv = _safe(row.get("intrinsic_value"))
                m1.metric("Price",      f"${_safe(row['Price']):.2f}")
                m2.metric("Fair Value", f"${iv:.2f}" if iv > 0 else "N/A")
                m3.metric("RSI",        f"{_safe(row.get('RSI'),50):.1f}")
                m4.metric("MoS %",      f"{_safe(row.get('margin_of_safety')):.1f}%")
                st.info(row.get("AI_Verdict", "Analysis pending"))
                arts = parse_articles(row.get("Articles_JSON","[]"))
                if arts:
                    st.markdown("**Latest News**")
                    st.markdown(art_html(arts[:3]), unsafe_allow_html=True)

    # =========================================================
    # TAB 2 — RISK LAB
    # =========================================================
    with tab2:
        st.markdown("### Risk Laboratory")

        # Position Sizing Matrix
        if {"daily_volatility","position_size","Oracle_Score","risk_level"}.issubset(fdf.columns):
            st.markdown("#### Position Sizing Matrix")
            st.caption("X = daily volatility. Y = recommended allocation %. Bubble size = Oracle Score. Colour = risk level.")
            pxm = fdf.copy()
            pxm["bubble"] = pxm["Oracle_Score"].clip(lower=5)
            fig_pos = px.scatter(
                pxm, x="daily_volatility", y="position_size",
                size="bubble", color="risk_level",
                hover_name="Ticker",
                hover_data={"Oracle_Score":True,"margin_of_safety":True,"Sector":True,"bubble":False},
                color_discrete_map={"Low":"#22c55e","Medium":"#f59e0b","High":"#ef4444"},
                labels={"daily_volatility":"Daily Volatility %","position_size":"Allocation %"},
            )
            fig_pos.add_vline(x=fdf["daily_volatility"].median(), line_dash="dot", line_color="#3f3f46")
            fig_pos.add_hline(y=fdf["position_size"].median(),    line_dash="dot", line_color="#3f3f46")
            st.plotly_chart(dark_layout(fig_pos), use_container_width=True)

        # Opportunity Heatmap
        if {"RSI","margin_of_safety","Oracle_Score"}.issubset(fdf.columns):
            st.markdown("#### Opportunity Heatmap")
            st.caption("X = DCF margin of safety. Y = RSI. Ideal = right of 0 + below RSI 70. Colour = Oracle Score.")
            dcf_count = (fdf["intrinsic_value"] > 0).sum() if "intrinsic_value" in fdf.columns else 0
            if dcf_count == 0:
                st.error("No DCF data — all bubbles cluster at X=0. Clear cache and rescan.")
            mos_vals = fdf["margin_of_safety"].dropna()
            x_lo = max(mos_vals.quantile(0.02)*1.1, -300) if len(mos_vals) else -10
            x_hi = min(mos_vals.quantile(0.98)*1.1,  500) if len(mos_vals) else 10
            if abs(x_hi - x_lo) < 5:
                x_lo, x_hi = -10, 10
            heat = fdf.copy()
            heat["bubble"] = heat["Oracle_Score"].clip(lower=5)
            fig_heat = px.scatter(
                heat, x="margin_of_safety", y="RSI",
                size="bubble", color="Oracle_Score",
                hover_name="Ticker",
                hover_data={"Sector":True,"Price":True,"intrinsic_value":True,"bubble":False},
                color_continuous_scale=[(0,"#ef4444"),(0.5,"#f59e0b"),(1,"#22c55e")],
                range_color=[0,100],
                labels={"margin_of_safety":"Valuation Upside %","RSI":"Momentum (RSI)"},
            )
            fig_heat.add_hline(y=70, line_dash="dot", line_color="#ef4444",
                               annotation_text="Overbought 70", annotation_font=dict(color="#ef4444",size=10))
            fig_heat.add_hline(y=30, line_dash="dot", line_color="#22c55e",
                               annotation_text="Oversold 30", annotation_font=dict(color="#22c55e",size=10))
            fig_heat.add_vline(x=0, line_dash="dash", line_color="#52525b")
            fig_heat.update_layout(xaxis=dict(range=[x_lo, x_hi]))
            st.plotly_chart(dark_layout(fig_heat), use_container_width=True)

        # Volatility Distribution with ticker hover
        if "daily_volatility" in fdf.columns:
            st.markdown("#### Portfolio Volatility Distribution")
            st.caption("Hover a bar to see which tickers fall in that volatility bucket.")
            vd = fdf[["Ticker","daily_volatility"]].dropna().copy()
            vd["daily_volatility"] = pd.to_numeric(vd["daily_volatility"], errors="coerce").dropna()
            vmax   = vd["daily_volatility"].quantile(0.98)
            nbins  = max(15, min(40, int(len(vd)/2)))
            edges  = np.linspace(0, max(vmax*1.1, 10), nbins+1)
            vd["bin"] = pd.cut(vd["daily_volatility"], bins=edges, labels=False)
            bdata = (vd.groupby("bin", observed=True)
                       .agg(count=("Ticker","count"),
                            tickers=("Ticker", lambda x: ", ".join(sorted(x))))
                       .reset_index())
            bdata["mid"]   = [(edges[int(i)]+edges[int(i)+1])/2 for i in bdata["bin"]]
            bdata["label"] = [f"{edges[int(i)]:.1f}-{edges[int(i)+1]:.1f}%" for i in bdata["bin"]]
            bdata["color"] = bdata["mid"].apply(
                lambda v: "#22c55e" if v < 2 else ("#f59e0b" if v < 5 else "#ef4444"))
            fig_vol = go.Figure()
            for _, br in bdata.iterrows():
                fig_vol.add_trace(go.Bar(
                    x=[br["mid"]], y=[br["count"]],
                    width=[(edges[1]-edges[0])*0.85],
                    marker_color=br["color"], marker_opacity=0.8,
                    showlegend=False,
                    hovertemplate=(f"<b>{br['label']}</b><br>Count: {br['count']}<br>"
                                   f"Tickers: {br['tickers']}<extra></extra>"),
                ))
            fig_vol.add_vline(x=2, line_dash="dot", line_color="#22c55e",
                              annotation_text="2%", annotation_font=dict(color="#22c55e",size=9))
            fig_vol.add_vline(x=5, line_dash="dot", line_color="#f59e0b",
                              annotation_text="5%", annotation_font=dict(color="#f59e0b",size=9))
            fig_vol.update_layout(xaxis_title="Daily Volatility %", yaxis_title="Stocks",
                                  xaxis=dict(range=[0,max(vmax*1.15,10)]), bargap=0.04)
            st.plotly_chart(dark_layout(fig_vol), use_container_width=True)

            low_t  = vd[vd["daily_volatility"] < 2]["Ticker"].tolist()
            med_t  = vd[(vd["daily_volatility"] >= 2) & (vd["daily_volatility"] < 5)]["Ticker"].tolist()
            high_t = vd[vd["daily_volatility"] >= 5]["Ticker"].tolist()
            vc1,vc2,vc3 = st.columns(3)
            vc1.metric("Low Risk (<2%)",   len(low_t))
            vc2.metric("Medium (2-5%)",    len(med_t))
            vc3.metric("High Risk (>5%)",  len(high_t))
            bx1,bx2,bx3 = st.columns(3)
            with bx1:
                if low_t:
                    with st.expander(f"Low risk ({len(low_t)})"):
                        st.markdown(chips_html(low_t), unsafe_allow_html=True)
            with bx2:
                if med_t:
                    with st.expander(f"Medium ({len(med_t)})"):
                        st.markdown(chips_html(med_t), unsafe_allow_html=True)
            with bx3:
                if high_t:
                    with st.expander(f"High risk ({len(high_t)})"):
                        st.markdown(chips_html(high_t), unsafe_allow_html=True)

    # =========================================================
    # TAB 3 — DEEP DIVE
    # =========================================================
    with tab3:
        st.markdown("### Deep Dive")
        selected = st.selectbox(
            "Select asset", sorted(fdf["Ticker"].tolist()),
            format_func=lambda t: (
                f"{t}  —  Score {int(_safe(fdf.loc[fdf['Ticker']==t,'Oracle_Score'].values[0]))}  "
                f"MoS {_safe(fdf.loc[fdf['Ticker']==t,'margin_of_safety'].values[0]):.1f}%"
            )
        )
        row = fdf[fdf["Ticker"] == selected].iloc[0]
        m1,m2,m3,m4,m5,m6 = st.columns(6)
        price = _safe(row["Price"])
        iv    = _safe(row.get("intrinsic_value"))
        mos   = _safe(row.get("margin_of_safety"))
        m1.metric("Price",      f"${price:.2f}")
        m2.metric("Fair Value", f"${iv:.2f}" if iv > 0 else "N/A")
        m3.metric("MoS %",      f"{mos:.1f}%")
        m4.metric("Oracle",     f"{_safe(row.get('Oracle_Score')):.0f}")
        m5.metric("RSI",        f"{_safe(row.get('RSI'),50):.1f}")
        m6.metric("Risk",       row.get("risk_level","?"))

        meth = str(row.get("dcf_method",""))
        if iv == 0:
            if "skipped" in meth:
                st.info("DCF not applicable for ETFs/commodities.")
            elif "no_fcf" in meth:
                st.warning("No free cash flow data found. Common for banks, early-stage, or foreign listings.")
            else:
                st.warning(f"DCF unavailable: {meth}")

        st.markdown("---")
        if iv > 0:
            st.markdown("#### Valuation Football Field")
            fig_ff = go.Figure()
            for y0, y1, col, name in [
                (iv*0.5, iv*0.8,  "rgba(59,130,246,0.12)",  "Deep Value (>-20% to -50%)"),
                (iv*0.8, iv*1.2,  "rgba(34,197,94,0.15)",   "Fair Value (+/-20%)"),
                (iv*1.2, iv*1.5,  "rgba(245,158,11,0.10)",  "Premium (+20% to +50%)"),
                (iv*1.5, iv*2.0,  "rgba(239,68,68,0.08)",   "Overvalued (>+50%)"),
            ]:
                fig_ff.add_shape(type="rect", x0=0, x1=1, y0=y0, y1=y1,
                                 fillcolor=col, line_width=0)
                fig_ff.add_annotation(x=0.97, y=(y0+y1)/2, text=name, showarrow=False,
                                      xanchor="right", font=dict(size=9,color="#71717a"))
            fig_ff.add_hline(y=price, line_dash="dash", line_color="#ef4444", line_width=2,
                             annotation_text=f"Price ${price:.2f}",
                             annotation_font=dict(color="#ef4444"))
            fig_ff.add_hline(y=iv, line_color="#3b82f6", line_width=1.5,
                             annotation_text=f"Intrinsic ${iv:.2f}",
                             annotation_font=dict(color="#3b82f6"))
            fig_ff.update_layout(height=320, showlegend=False,
                                 xaxis=dict(showticklabels=False,showgrid=False,zeroline=False),
                                 yaxis_title="Price ($)")
            st.plotly_chart(dark_layout(fig_ff), use_container_width=True)

        st.markdown("---")
        st.markdown("#### AI Verdict")
        verdict = row.get("AI_Verdict","")
        if verdict and verdict != "Neutral outlook":
            st.info(verdict)
        else:
            st.caption("No strong signals detected.")

        st.markdown("---")
        st.markdown("#### Latest News")
        arts = parse_articles(row.get("Articles_JSON","[]"))
        if arts and arts[0]["headline"] != "No recent news available":
            st.markdown(art_html(arts), unsafe_allow_html=True)
        else:
            st.warning("No articles stored. Run a fresh scan to populate.")

        st.markdown("---")
        st.markdown("#### Risk Profile")
        r1,r2,r3 = st.columns(3)
        r1.metric("Allocation",      f"{_safe(row.get('position_size')):.1f}%")
        r2.metric("Daily Volatility",f"{_safe(row.get('daily_volatility')):.2f}%")
        r3.metric("Short Interest",  f"{_safe(row.get('short_interest')):.1f}%")
        if row.get("Geo_Risk"):
            st.warning("Geopolitical risk detected in recent headlines.")
        st.markdown(f"[Yahoo Finance](https://finance.yahoo.com/quote/{selected})")

    # =========================================================
    # TAB 4 — MARKET INTEL
    # =========================================================
    with tab4:
        st.markdown("### Market Intelligence")

        if "Sector" in fdf.columns:
            st.markdown("#### Sector Breakdown")
            agg = {k:"mean" for k in ["Oracle_Score","margin_of_safety","RSI","daily_volatility"]
                   if k in fdf.columns}
            agg["Ticker"] = "count"
            s_agg = (fdf.groupby("Sector").agg(agg).round(1)
                       .rename(columns={"Ticker":"Count","Oracle_Score":"Avg Oracle",
                                        "margin_of_safety":"Avg MoS %","RSI":"Avg RSI",
                                        "daily_volatility":"Avg Vol %"}))
            st.dataframe(s_agg, use_container_width=True)

        if {"RSI","margin_of_safety","Oracle_Score","Sector"}.issubset(fdf.columns):
            st.markdown("#### Value vs Momentum")
            mos2  = fdf["margin_of_safety"].dropna()
            xl2   = max(mos2.quantile(0.02)*1.1,-200) if len(mos2) else -10
            xh2   = min(mos2.quantile(0.98)*1.1, 400) if len(mos2) else 10
            if abs(xh2-xl2) < 5:
                xl2, xh2 = -10, 10
            vm = fdf.copy()
            vm["bubble"] = vm["Oracle_Score"].clip(lower=5)
            fig_vm = px.scatter(
                vm, x="margin_of_safety", y="RSI", size="bubble", color="Sector",
                hover_name="Ticker",
                hover_data={"Price":True,"Oracle_Score":True,"bubble":False},
                labels={"margin_of_safety":"Valuation Upside %","RSI":"Momentum (RSI)"},
            )
            fig_vm.add_hline(y=70, line_dash="dot", line_color="#ef4444",
                             annotation_text="Overbought", annotation_font=dict(color="#ef4444",size=10))
            fig_vm.add_hline(y=30, line_dash="dot", line_color="#22c55e",
                             annotation_text="Oversold",   annotation_font=dict(color="#22c55e",size=10))
            fig_vm.add_vline(x=0, line_dash="dash", line_color="#52525b")
            fig_vm.update_layout(xaxis=dict(range=[xl2,xh2]))
            st.plotly_chart(dark_layout(fig_vm), use_container_width=True)

        if "Sentiment" in fdf.columns:
            st.markdown("#### Sentiment Overview")
            avg_s = fdf["Sentiment"].mean()
            sc1,sc2,sc3 = st.columns(3)
            sc1.metric("Avg Sentiment", f"{avg_s:.3f}")
            if "Geo_Risk" in fdf.columns:
                sc2.metric("Geo-Risk Count", int(fdf["Geo_Risk"].sum()))
            if "Oracle_Score" in fdf.columns:
                hc_pct = len(fdf[fdf["Oracle_Score"]>75])/max(len(fdf),1)*100
                sc3.metric("High Conviction %", f"{hc_pct:.1f}%")

    # =========================================================
    # TAB 5 — DIAGNOSTICS  (scoping fixed: info defined before use)
    # =========================================================
    with tab5:
        st.markdown("### Diagnostics")

        import yfinance as _yf
        import traceback as _tb

        # Environment
        st.markdown("#### Environment")
        try:    yf_ver = _yf.__version__
        except: yf_ver = "unknown"
        try:
            import pandas as _pd; pd_ver = _pd.__version__
        except: pd_ver = "unknown"
        try:
            import pandas_ta as _ta; ta_ver = _ta.__version__
        except: ta_ver = "NOT INSTALLED"

        ec1,ec2,ec3 = st.columns(3)
        ec1.metric("yfinance", yf_ver)
        ec2.metric("pandas",   pd_ver)
        ec3.metric("pandas-ta",ta_ver)

        st.markdown("---")
        st.markdown("#### Live DCF Probe")
        st.caption("Fetches live data for one ticker and shows exactly what the DCF engine sees.")

        probe_col1, probe_col2 = st.columns([2,1])
        with probe_col1:
            probe_ticker = st.text_input("Ticker", value="AAPL")
        with probe_col2:
            st.markdown("<br>", unsafe_allow_html=True)
            run_probe = st.button("Run Probe", type="primary")

        if run_probe and probe_ticker:
            t = probe_ticker.strip().upper()
            logs    = []
            summary = {}

            with st.spinner(f"Probing {t}..."):
                try:
                    stock = _yf.Ticker(t)

                    # info
                    probe_info = {}
                    try:
                        probe_info = stock.info or {}
                        logs.append(f"OK info: {len(probe_info)} keys")
                        for k in ["currentPrice","sharesOutstanding","freeCashflow",
                                  "operatingCashflow","totalDebt","totalCash",
                                  "revenueGrowth","earningsGrowth","trailingPE"]:
                            logs.append(f"   {k} = {probe_info.get(k)}")
                        summary["price"] = probe_info.get("currentPrice")
                    except Exception as e:
                        logs.append(f"FAIL info: {e}")

                    # price history
                    try:
                        hist = _yf.download(t, period="3mo", auto_adjust=True, progress=False)
                        if hist is not None and not hist.empty:
                            logs.append(f"OK price history: {len(hist)} rows")
                            summary["hist"] = True
                        else:
                            logs.append("FAIL price history: empty")
                    except Exception as e:
                        logs.append(f"FAIL price history: {e}")

                    # cashflow
                    logs.append("")
                    logs.append("-- Cashflow DataFrame --")
                    try:
                        cf = stock.cashflow
                        if cf is not None and not cf.empty:
                            logs.append(f"OK cashflow: {cf.shape[0]} rows x {cf.shape[1]} cols")
                            if "Free Cash Flow" in cf.index:
                                fcf_row = pd.to_numeric(cf.loc["Free Cash Flow"], errors="coerce").dropna()
                                logs.append(f"OK 'Free Cash Flow' row: {fcf_row.tolist()}")
                                summary["fcf_row"] = fcf_row.tolist()
                            else:
                                logs.append("MISS 'Free Cash Flow' not in index")
                                logs.append(f"   Available rows: {list(cf.index)[:10]}")
                        else:
                            logs.append("FAIL cashflow: empty or None")
                    except Exception as e:
                        logs.append(f"FAIL cashflow: {e}")

                    # DCF
                    logs.append("")
                    logs.append("-- DCF Result --")
                    try:
                        from logic import calc_dcf as _calc_dcf
                        dcf_result = _calc_dcf(stock, probe_info, sector="Technology")
                        iv = dcf_result["intrinsic_value"]
                        logs.append(f"{'OK' if iv > 0 else 'FAIL'} intrinsic_value = ${iv:.2f}")
                        logs.append(f"   margin_of_safety = {dcf_result['margin_of_safety']:.1f}%")
                        logs.append(f"   growth_rate      = {dcf_result['growth_rate']:.1f}%")
                        logs.append(f"   dcf_method       = {dcf_result['dcf_method']}")
                        logs.append(f"   dcf_confidence   = {dcf_result['dcf_confidence']}")
                        summary["dcf"] = dcf_result
                    except Exception as e:
                        logs.append(f"FAIL DCF: {e}")
                        logs.append(_tb.format_exc())

                    # News
                    logs.append("")
                    logs.append("-- News --")
                    try:
                        from logic import fetch_news as _fn
                        arts = _fn(t, max_items=3)
                        if arts and arts[0]["headline"] != "No recent news available":
                            logs.append(f"OK {len(arts)} articles")
                            for a in arts:
                                logs.append(f"   [{a['label']:7s} {a['score']:+.2f}] {a['headline'][:65]}")
                                logs.append(f"   reason: {a['reason']}")
                            summary["news"] = True
                        else:
                            logs.append("MISS no articles returned")
                    except Exception as e:
                        logs.append(f"FAIL news: {e}")

                except Exception as e:
                    logs.append(f"FATAL: {e}")
                    logs.append(_tb.format_exc())

            st.code("\n".join(logs), language="text")

            # Summary cards
            s1,s2,s3,s4 = st.columns(4)
            s1.metric("Price Data",    "OK" if summary.get("price") else "MISSING")
            s2.metric("Price History", "OK" if summary.get("hist")  else "MISSING")
            iv_val = summary.get("dcf",{}).get("intrinsic_value",0)
            s3.metric("DCF Value",     f"${iv_val:.2f}" if iv_val > 0 else "FAIL")
            s4.metric("News",          "OK" if summary.get("news")  else "MISSING")

        st.markdown("---")
        st.markdown("#### Cache Inspector")
        if os.path.exists("market_cache.csv"):
            try:
                cdf    = pd.read_csv("market_cache.csv")
                dcf_ok = (cdf["intrinsic_value"] > 0).sum() if "intrinsic_value" in cdf.columns else 0
                news_ok = cdf["Articles_JSON"].apply(
                    lambda x: isinstance(x,str) and len(x)>50
                ).sum() if "Articles_JSON" in cdf.columns else 0
                ci1,ci2,ci3,ci4 = st.columns(4)
                ci1.metric("Cached stocks", len(cdf))
                ci2.metric("With DCF",      dcf_ok)
                ci3.metric("With news",     news_ok)
                ci4.metric("Age", cdf["Last_Updated"].iloc[0] if "Last_Updated" in cdf.columns else "?")
                if dcf_ok == 0:
                    st.error("Stale cache with 0 DCF values. Clear Cache and rescan.")
                with st.expander("Preview (first 10 rows)"):
                    cols = [c for c in ["Ticker","Price","intrinsic_value","margin_of_safety",
                                         "dcf_method","risk_level"] if c in cdf.columns]
                    st.dataframe(cdf[cols].head(10), use_container_width=True, hide_index=True)
            except Exception as e:
                st.error(f"Could not read cache: {e}")
        else:
            st.info("No cache file yet — run a scan first.")

# ===========================================================================
# EMPTY STATE
# ===========================================================================
else:
    st.markdown("""
    <div style="max-width:480px;margin:72px auto;text-align:center">
      <div style="font-size:44px;margin-bottom:14px;color:#3b82f6">◈</div>
      <div style="font-size:20px;font-weight:700;color:#f4f4f5;margin-bottom:8px">Welcome to Apex Markets</div>
      <div style="color:#52525b;font-size:13px;line-height:1.7">
        Click <strong style="color:#3b82f6">Run Global Scan</strong> in the sidebar to begin.
      </div>
    </div>
    """, unsafe_allow_html=True)
