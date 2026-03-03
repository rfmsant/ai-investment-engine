import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import json
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
# DESIGN SYSTEM  ── dark, editorial, high-contrast
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');

  /* ── Global reset ── */
  html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }

  .stApp { background: #09090b; color: #e4e4e7; }

  /* ── Sidebar ── */
  section[data-testid="stSidebar"] {
    background: #111113 !important;
    border-right: 1px solid #27272a;
  }
  section[data-testid="stSidebar"] * { color: #a1a1aa !important; }
  section[data-testid="stSidebar"] .stButton>button {
    background: #3b82f6 !important;
    color: #fff !important;
    border: none;
    border-radius: 8px;
    font-weight: 600;
    width: 100%;
    padding: 10px 0;
    font-size: 13px;
    letter-spacing: 0.03em;
    transition: background .2s;
  }
  section[data-testid="stSidebar"] .stButton>button:hover { background:#2563eb!important; }

  /* ── Tabs ── */
  .stTabs [data-baseweb="tab-list"] {
    background: transparent;
    border-bottom: 1px solid #27272a;
    gap: 0;
  }
  .stTabs [data-baseweb="tab"] {
    background: transparent;
    color: #71717a;
    font-size: 13px;
    font-weight: 500;
    letter-spacing: 0.04em;
    padding: 12px 24px;
    border-bottom: 2px solid transparent;
    transition: all .2s;
  }
  .stTabs [aria-selected="true"] {
    color: #3b82f6 !important;
    border-bottom-color: #3b82f6 !important;
    background: transparent !important;
  }

  /* ── Metric cards ── */
  [data-testid="metric-container"] {
    background: #111113;
    border: 1px solid #27272a;
    border-radius: 12px;
    padding: 16px 20px;
    transition: border-color .2s;
  }
  [data-testid="metric-container"]:hover { border-color: #3b82f6; }
  [data-testid="metric-container"] label { color: #71717a !important; font-size: 11px !important; letter-spacing: 0.08em; text-transform: uppercase; }
  [data-testid="metric-container"] [data-testid="stMetricValue"] { color: #f4f4f5 !important; font-size: 26px !important; font-weight: 700 !important; }

  /* ── DataFrames ── */
  [data-testid="stDataFrame"] { border-radius: 12px; overflow: hidden; border: 1px solid #27272a; }

  /* ── Expanders ── */
  .streamlit-expanderHeader { background: #111113 !important; border: 1px solid #27272a !important; border-radius: 10px !important; color: #e4e4e7 !important; font-weight: 500 !important; }
  .streamlit-expanderContent { background: #0d0d10 !important; border: 1px solid #27272a !important; border-top: none !important; border-radius: 0 0 10px 10px !important; padding: 16px !important; }

  /* ── Dividers ── */
  hr { border-color: #27272a !important; }

  /* ── Labels / subheaders ── */
  h1 { font-size: 28px !important; font-weight: 700 !important; color: #f4f4f5 !important; letter-spacing: -0.02em; }
  h2 { font-size: 18px !important; font-weight: 600 !important; color: #e4e4e7 !important; }
  h3 { font-size: 14px !important; font-weight: 600 !important; color: #a1a1aa !important; text-transform: uppercase; letter-spacing: 0.06em; }

  /* ── Scrollable filter tags ── */
  .stMultiSelect > div { background: #111113 !important; border: 1px solid #27272a !important; border-radius: 8px !important; }

  /* ── Clickable metric pill ── */
  .apex-pill {
    display: inline-flex; align-items: center; gap: 8px;
    background: #111113; border: 1px solid #27272a;
    border-radius: 100px; padding: 6px 14px;
    cursor: pointer; transition: all .15s;
    font-size: 12px; color: #a1a1aa;
  }
  .apex-pill:hover { border-color: #3b82f6; color: #3b82f6; }
  .apex-pill .val { font-weight: 700; font-size: 16px; color: #f4f4f5; }

  /* ── Article cards ── */
  .article-card {
    background: #111113; border: 1px solid #27272a;
    border-radius: 10px; padding: 12px 16px; margin-bottom: 8px;
  }
  .article-card.bull { border-left: 3px solid #22c55e; }
  .article-card.bear { border-left: 3px solid #ef4444; }
  .article-card.neut { border-left: 3px solid #71717a; }
  .article-title { font-size: 13px; font-weight: 500; color: #e4e4e7; line-height: 1.4; }
  .article-meta  { font-size: 11px; color: #52525b; margin-top: 4px; }
  .badge {
    display: inline-block; font-size: 10px; font-weight: 600;
    letter-spacing: 0.05em; padding: 2px 8px; border-radius: 100px;
  }
  .badge-bull { background: #14532d; color: #4ade80; }
  .badge-bear { background: #450a0a; color: #f87171; }
  .badge-neut { background: #27272a; color: #a1a1aa; }

  /* ── Header bar ── */
  .apex-header {
    display: flex; align-items: baseline; gap: 12px;
    padding: 4px 0 20px;
    border-bottom: 1px solid #27272a; margin-bottom: 24px;
  }
  .apex-logo { font-size: 22px; font-weight: 700; color: #f4f4f5; letter-spacing: -0.03em; }
  .apex-sub  { font-size: 12px; color: #52525b; letter-spacing: 0.05em; }

  /* scrollable ticker list inside expander */
  .ticker-grid { display: flex; flex-wrap: wrap; gap: 6px; max-height: 220px; overflow-y: auto; padding: 4px 0; }
  .ticker-chip {
    background: #18181b; border: 1px solid #3f3f46;
    border-radius: 6px; padding: 3px 10px;
    font-size: 11px; font-family: 'DM Mono', monospace; color: #a1a1aa;
  }

  /* ── Plotly ── */
  .js-plotly-plot .plotly .modebar { background: transparent !important; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def score_color(score: float) -> str:
    if score >= 70: return "#22c55e"
    if score >= 45: return "#f59e0b"
    return "#ef4444"

def rsi_color(rsi: float) -> str:
    if rsi > 70: return "#ef4444"
    if rsi < 30: return "#22c55e"
    return "#a1a1aa"

def mos_color(mos: float) -> str:
    if mos > 20: return "#22c55e"
    if mos > 0:  return "#f59e0b"
    return "#ef4444"

def article_css_class(label: str) -> str:
    return {"Bullish": "bull", "Bearish": "bear"}.get(label, "neut")

def article_badge(label: str) -> str:
    cls = {"Bullish": "badge-bull", "Bearish": "badge-bear"}.get(label, "badge-neut")
    return f'<span class="badge {cls}">{label.upper()}</span>'

def parse_articles(json_str) -> list:
    """Robustly parse articles JSON that may have been mangled by CSV round-trip."""
    if not json_str or not isinstance(json_str, str) or json_str.strip() in ('[]', '', 'nan'):
        return []
    try:
        result = json.loads(json_str)
        if isinstance(result, list):
            return result
    except Exception:
        pass
    try:
        stripped = json_str.strip('"').replace('""', '"')
        result = json.loads(stripped)
        if isinstance(result, list):
            return result
    except Exception:
        pass
    try:
        import ast
        result = ast.literal_eval(json_str)
        if isinstance(result, list):
            return result
    except Exception:
        pass
    return []

def plotly_dark_layout(fig, title=""):
    fig.update_layout(
        title=dict(text=title, font=dict(size=14, color="#e4e4e7"), x=0),
        paper_bgcolor="#111113",
        plot_bgcolor="#0d0d10",
        font=dict(family="DM Sans", color="#71717a", size=11),
        margin=dict(l=16, r=16, t=40, b=16),
        legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="#27272a", borderwidth=1),
        xaxis=dict(gridcolor="#1f1f23", zerolinecolor="#27272a", linecolor="#27272a"),
        yaxis=dict(gridcolor="#1f1f23", zerolinecolor="#27272a", linecolor="#27272a"),
    )
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# ENGINE & SESSION
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    return InvestmentEngine()

engine = get_engine()

if 'data' not in st.session_state:
    df_cache, from_cache = engine.load_data()
    st.session_state.data = df_cache
    st.session_state.from_cache = from_cache

# modal-like drill-down state
if 'drill_ticker' not in st.session_state:
    st.session_state.drill_ticker = None
if 'show_metric_detail' not in st.session_state:
    st.session_state.show_metric_detail = None


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ◈ APEX MARKETS")
    st.markdown("---")

    if st.button("⟳  Run Global Scan", type="primary"):
        with st.spinner("Scanning global markets…"):
            progress_bar = st.progress(0, "Initialising…")
            st.session_state.data = engine.fetch_market_data(progress_bar)
            progress_bar.empty()
            st.success("Scan complete")
            st.rerun()

    import os as _os
    if st.button("🗑️  Clear Cache & Re-scan"):
        if _os.path.exists("market_cache.csv"):
            _os.remove("market_cache.csv")
        st.session_state.data = pd.DataFrame()
        st.rerun()

    df_raw = st.session_state.data

    if not df_raw.empty:
        st.markdown("#### Filters")

        if 'Sector' in df_raw.columns:
            all_sectors = sorted(df_raw['Sector'].dropna().unique())
            sel_sectors = st.multiselect("Sector", all_sectors, default=all_sectors)
        else:
            sel_sectors = []

        if 'Region' in df_raw.columns:
            all_regions = sorted(df_raw['Region'].dropna().unique())
            sel_regions = st.multiselect("Region", all_regions, default=all_regions)
        else:
            sel_regions = []

        if 'risk_level' in df_raw.columns:
            risk_opts = sorted(df_raw['risk_level'].dropna().unique())
            sel_risks = st.multiselect("Risk Level", risk_opts, default=risk_opts)
        else:
            sel_risks = []

        min_score = st.slider("Min Oracle Score", 0, 100, 0)

    st.markdown("---")
    st.caption("Apex Markets v3.0 · Global Edition")
    if not df_raw.empty and 'Last_Updated' in df_raw.columns:
        st.caption(f"Last scan: {df_raw['Last_Updated'].iloc[0]}")


# ─────────────────────────────────────────────────────────────────────────────
# FILTER
# ─────────────────────────────────────────────────────────────────────────────
df = st.session_state.data

if not df.empty:
    fdf = df.copy()
    if sel_sectors and 'Sector' in fdf.columns:
        fdf = fdf[fdf['Sector'].isin(sel_sectors)]
    if sel_regions and 'Region' in fdf.columns:
        fdf = fdf[fdf['Region'].isin(sel_regions)]
    if sel_risks and 'risk_level' in fdf.columns:
        fdf = fdf[fdf['risk_level'].isin(sel_risks)]
    if 'Oracle_Score' in fdf.columns:
        fdf = fdf[fdf['Oracle_Score'] >= min_score]
else:
    fdf = df


# ─────────────────────────────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(
    '<div class="apex-header">'
    '  <span class="apex-logo">◈ Apex Markets</span>'
    '  <span class="apex-sub">GLOBAL INVESTMENT INTELLIGENCE · 500+ ASSETS</span>'
    '</div>',
    unsafe_allow_html=True
)


# ─────────────────────────────────────────────────────────────────────────────
# KPI BAR  — clickable metrics that reveal ticker lists
# ─────────────────────────────────────────────────────────────────────────────
if not fdf.empty:
    total_opp      = len(fdf)
    high_conviction_df = fdf[fdf['Oracle_Score'] > 75] if 'Oracle_Score' in fdf.columns else pd.DataFrame()
    undervalued_df     = fdf[fdf['margin_of_safety'] > 20] if 'margin_of_safety' in fdf.columns else pd.DataFrame()
    geo_risk_df        = fdf[fdf['Geo_Risk'] == True] if 'Geo_Risk' in fdf.columns else pd.DataFrame()
    high_risk_df       = fdf[fdf['risk_level'] == 'High'] if 'risk_level' in fdf.columns else pd.DataFrame()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Opportunities", total_opp)
    c2.metric("High Conviction", len(high_conviction_df),
              help="Oracle Score > 75. Click expander below to see tickers.")
    c3.metric("Undervalued", len(undervalued_df),
              help="Margin of Safety > 20%. Click expander below to see tickers.")
    c4.metric("Geo-Risk Alerts", len(geo_risk_df))
    c5.metric("High Volatility", len(high_risk_df))

    # Expandable ticker lists below KPI bar
    def _ticker_chips(subset_df: pd.DataFrame) -> str:
        chips = "".join(
            f'<span class="ticker-chip">{t}</span>'
            for t in sorted(subset_df['Ticker'].tolist())
        )
        return f'<div class="ticker-grid">{chips}</div>'

    kpi_col1, kpi_col2, kpi_col3 = st.columns([1, 1, 1])
    with kpi_col1:
        if len(high_conviction_df) > 0:
            with st.expander(f"▸ {len(high_conviction_df)} High Conviction tickers"):
                st.markdown(_ticker_chips(high_conviction_df), unsafe_allow_html=True)
    with kpi_col2:
        if len(undervalued_df) > 0:
            with st.expander(f"▸ {len(undervalued_df)} Undervalued tickers"):
                st.markdown(_ticker_chips(undervalued_df), unsafe_allow_html=True)
    with kpi_col3:
        if len(geo_risk_df) > 0:
            with st.expander(f"▸ {len(geo_risk_df)} Geo-Risk tickers"):
                st.markdown(_ticker_chips(geo_risk_df), unsafe_allow_html=True)

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

    # ════════════════════════════════════════════════════════════════════════
    # TAB 1 — TERMINAL
    # ════════════════════════════════════════════════════════════════════════
    with tab1:
        st.markdown("### Investment Terminal")

        sorted_df = fdf.sort_values("Oracle_Score", ascending=False) if 'Oracle_Score' in fdf.columns else fdf

        display_cols = [c for c in [
            'Ticker', 'Region', 'Sector', 'Price', 'intrinsic_value',
            'margin_of_safety', 'RSI', 'Oracle_Score', 'risk_level', 'dcf_confidence'
        ] if c in sorted_df.columns]

        col_cfg = {}
        if 'Price' in display_cols:
            col_cfg['Price'] = st.column_config.NumberColumn("Price", format="$%.2f")
        if 'intrinsic_value' in display_cols:
            col_cfg['intrinsic_value'] = st.column_config.NumberColumn("Fair Value", format="$%.2f")
        if 'margin_of_safety' in display_cols:
            col_cfg['margin_of_safety'] = st.column_config.NumberColumn("Upside %",
                help="% gap between current price and DCF intrinsic value. Positive = undervalued.")
        if 'Oracle_Score' in display_cols:
            col_cfg['Oracle_Score'] = st.column_config.ProgressColumn(
                "Oracle Score", min_value=0, max_value=100, format="%d",
                help="Composite score: Valuation 40% + Technical 30% + Quality 20% + Sentiment 10%")
        if 'RSI' in display_cols:
            col_cfg['RSI'] = st.column_config.NumberColumn("RSI",
                help="Relative Strength Index. <30 = oversold (potential buy). >70 = overbought (caution).")
        if 'risk_level' in display_cols:
            col_cfg['risk_level'] = st.column_config.TextColumn("Risk",
                help="Based on ATR/Price ratio. Low < 2%, Medium 2-5%, High > 5%")
        if 'dcf_confidence' in display_cols:
            col_cfg['dcf_confidence'] = st.column_config.TextColumn("DCF Quality",
                help="High = 4+ years of cash-flow data. Medium = 2-3 years.")

        st.dataframe(
            sorted_df[display_cols],
            column_config=col_cfg,
            use_container_width=True,
            hide_index=True,
            height=420,
        )

        st.markdown("---")
        st.markdown("### 🏆 Top 5 Picks")

        top5 = sorted_df.head(5)
        for _, row in top5.iterrows():
            score_c = score_color(row.get('Oracle_Score', 0))
            mos_c   = mos_color(row.get('margin_of_safety', 0))
            label   = f"**{row['Ticker']}** — Oracle {row.get('Oracle_Score', '–')} · Upside {row.get('margin_of_safety', 0):.1f}%"

            with st.expander(label):
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Price",      f"${row.get('Price', 0):.2f}")
                m2.metric("Fair Value", f"${row.get('intrinsic_value', 0):.2f}" if row.get('intrinsic_value', 0) > 0 else "N/A")
                m3.metric("RSI",        f"{row.get('RSI', 0):.1f}")
                m4.metric("Risk",       row.get('risk_level', '–'))

                st.markdown("**AI Verdict**")
                st.info(row.get('AI_Verdict', 'Analysis pending…'))

                # Mini article feed
                articles = parse_articles(row.get('Articles_JSON', '[]'))
                if articles:
                    st.markdown("**Latest News**")
                    for a in articles[:3]:
                        css = article_css_class(a.get('label', 'Neutral'))
                        badge = article_badge(a.get('label', 'Neutral'))
                        reasoning_short = a.get('reasoning', '')[:80]
                        st.markdown(
                            f'<div class="article-card {css}">'
                            f'  <div class="article-title">{a["headline"]}</div>'
                            f'  <div class="article-meta">{badge} &nbsp; {reasoning_short}</div>'
                            f'</div>',
                            unsafe_allow_html=True
                        )


    # ════════════════════════════════════════════════════════════════════════
    # TAB 2 — RISK LAB
    # ════════════════════════════════════════════════════════════════════════
    with tab2:
        st.markdown("### Risk Laboratory")
        st.caption("How to read this section · **Position Matrix**: each bubble is a stock — bigger bubble = higher Oracle Score. Ideal candidates live in the bottom-left (low volatility, small position). **Heatmap**: green = good opportunity, red = overvalued/risky. **Volatility Distribution**: how spread out daily moves are across your watchlist.")

        # ── 2.1  Position Sizing Matrix ─────────────────────────────────────
        if {'daily_volatility', 'position_size', 'Oracle_Score', 'risk_level'}.issubset(fdf.columns):
            st.markdown("#### Position Sizing Matrix")
            fig_pos = px.scatter(
                fdf,
                x='daily_volatility',
                y='position_size',
                size='Oracle_Score',
                color='risk_level',
                hover_name='Ticker',
                hover_data={'Oracle_Score': True, 'margin_of_safety': True},
                color_discrete_map={'Low': '#22c55e', 'Medium': '#f59e0b', 'High': '#ef4444'},
                labels={
                    'daily_volatility': 'Daily Volatility % (lower = safer)',
                    'position_size':    'Recommended Position Size %',
                },
            )
            fig_pos = plotly_dark_layout(fig_pos)
            # Quadrant annotations
            xmax = fdf['daily_volatility'].max() * 1.05
            ymax = fdf['position_size'].max() * 1.05
            xmid = fdf['daily_volatility'].median()
            ymid = fdf['position_size'].median()
            for ann_text, ax, ay, acolor in [
                ("✅ Low risk, small size", xmax * 0.05, ymax * 0.1, "#22c55e"),
                ("⚠️ Low risk, large size", xmax * 0.05, ymax * 0.92, "#f59e0b"),
                ("🔴 High risk zone", xmax * 0.75, ymax * 0.92, "#ef4444"),
                ("⚡ Volatile, small size", xmax * 0.75, ymax * 0.1, "#f59e0b"),
            ]:
                fig_pos.add_annotation(x=ax, y=ay, text=ann_text, showarrow=False,
                                       font=dict(size=10, color=acolor), bgcolor="rgba(0,0,0,0.5)",
                                       borderpad=4)
            fig_pos.add_vline(x=xmid, line_dash="dot", line_color="#3f3f46")
            fig_pos.add_hline(y=ymid, line_dash="dot", line_color="#3f3f46")
            st.plotly_chart(fig_pos, use_container_width=True)

        # ── 2.2  Investment Heatmap ──────────────────────────────────────────
        st.markdown("#### Opportunity Heatmap")
        st.caption("Axis: X = Valuation opportunity (higher = more undervalued), Y = Technical momentum (RSI). Bubble size = Oracle Score. **Green zone**: undervalued & not overbought — ideal entries. **Red zone**: overvalued & overbought — caution.")

        if {'RSI', 'margin_of_safety', 'Oracle_Score', 'Sector'}.issubset(fdf.columns):
            # Check if DCF data is available
            dcf_available = (fdf['margin_of_safety'] != 0).sum()
            if dcf_available < 3:
                st.warning(f"⚠️ Only {dcf_available} stocks have DCF valuation data — this is why bubbles cluster on X=0. "
                           "Run a fresh scan to populate (requires yfinance to fetch cash flow statements, "
                           "which can take 5–10 min for a full universe).")

            # Compute axis range dynamically
            mos_values = fdf['margin_of_safety'].dropna()
            x_min = max(mos_values.quantile(0.02) - 10, -200)
            x_max = min(mos_values.quantile(0.98) + 10,  400)
            fig_heat = px.scatter(
                fdf,
                x='margin_of_safety',
                y='RSI',
                size=fdf['Oracle_Score'].clip(lower=5),
                color='Oracle_Score',
                hover_name='Ticker',
                hover_data={'Sector': True, 'Price': True, 'risk_level': True,
                            'intrinsic_value': True, 'dcf_confidence': True},
                color_continuous_scale=[(0, "#ef4444"), (0.5, "#f59e0b"), (1, "#22c55e")],
                range_color=[0, 100],
                labels={
                    'margin_of_safety': 'Valuation Upside % →',
                    'RSI':              'Momentum (RSI)',
                },
            )
            fig_heat = plotly_dark_layout(fig_heat)
            fig_heat.update_layout(xaxis=dict(range=[x_min, x_max]))
            # Zone shading
            fig_heat.add_shape(type="rect", x0=20, x1=300, y0=0, y1=70,
                               fillcolor="rgba(34,197,94,0.06)", line_width=0)
            fig_heat.add_shape(type="rect", x0=-300, x1=0, y0=70, y1=100,
                               fillcolor="rgba(239,68,68,0.06)", line_width=0)
            fig_heat.add_hline(y=70, line_dash="dot", line_color="#ef4444",
                               annotation_text="Overbought (RSI 70)", annotation_position="right",
                               annotation_font_color="#ef4444")
            fig_heat.add_hline(y=30, line_dash="dot", line_color="#22c55e",
                               annotation_text="Oversold (RSI 30)", annotation_position="right",
                               annotation_font_color="#22c55e")
            fig_heat.add_vline(x=0, line_dash="dash", line_color="#52525b",
                               annotation_text="Fair Value", annotation_font_color="#52525b")
            fig_heat.add_vline(x=20, line_dash="dot", line_color="#22c55e",
                               annotation_text="20% Upside", annotation_font_color="#22c55e")
            fig_heat.update_layout(coloraxis_colorbar=dict(
                title="Oracle Score",
                tickvals=[0, 25, 50, 75, 100],
                ticktext=["0 (Poor)", "25", "50", "75", "100 (Best)"],
                tickfont=dict(color="#71717a"),
                title_font=dict(color="#a1a1aa"),
            ))
            st.plotly_chart(fig_heat, use_container_width=True)

        # ── 2.3  Volatility Distribution ────────────────────────────────────
        if 'daily_volatility' in fdf.columns:
            st.markdown("#### Portfolio Volatility Distribution")
            fig_vol = go.Figure()
            fig_vol.add_trace(go.Histogram(
                x=fdf['daily_volatility'],
                nbinsx=30,
                marker_color='#3b82f6',
                opacity=0.75,
                name="Assets"
            ))
            # Colour zone shading (no annotation text to avoid overlap)
            fig_vol.add_vrect(x0=0,  x1=2,   fillcolor="rgba(34,197,94,0.08)",  line_width=0, layer="below")
            fig_vol.add_vrect(x0=2,  x1=5,   fillcolor="rgba(245,158,11,0.08)", line_width=0, layer="below")
            fig_vol.add_vrect(x0=5,  x1=200, fillcolor="rgba(239,68,68,0.06)",  line_width=0, layer="below")
            # Threshold lines with labels
            fig_vol.add_vline(x=2, line_dash="dot", line_color="#22c55e", line_width=1.5,
                              annotation_text="Low/Medium (2%)",
                              annotation_position="top right",
                              annotation_font=dict(color="#22c55e", size=10))
            fig_vol.add_vline(x=5, line_dash="dot", line_color="#f59e0b", line_width=1.5,
                              annotation_text="Medium/High (5%)",
                              annotation_position="top right",
                              annotation_font=dict(color="#f59e0b", size=10))
            fig_vol.update_layout(
                xaxis_title="Daily Volatility %  (ATR / Price × 100)",
                yaxis_title="Number of Assets",
                bargap=0.05,
                xaxis=dict(range=[0, max(fdf['daily_volatility'].quantile(0.99) * 1.1, 10)]),
            )
            fig_vol = plotly_dark_layout(fig_vol)
            st.plotly_chart(fig_vol, use_container_width=True)

            st.caption("**How to read:** Green zone = low volatility (<2% daily move), amber = medium (2–5%), red = high (>5%). A distribution skewed left = safer watchlist overall.")

            # Quick legend
            lc1, lc2, lc3 = st.columns(3)
            low_n  = len(fdf[fdf['daily_volatility'] < 2])  if 'daily_volatility' in fdf.columns else 0
            med_n  = len(fdf[(fdf['daily_volatility'] >= 2) & (fdf['daily_volatility'] < 5)]) if 'daily_volatility' in fdf.columns else 0
            high_n = len(fdf[fdf['daily_volatility'] >= 5]) if 'daily_volatility' in fdf.columns else 0
            lc1.metric("🟢 Low Risk (<2%)",   low_n,  help="Moves less than 2% per day on average")
            lc2.metric("🟡 Medium Risk (2-5%)", med_n, help="Moderate daily swings, manageable with sizing")
            lc3.metric("🔴 High Risk (>5%)",  high_n, help="Highly volatile — reduce position size accordingly")


    # ════════════════════════════════════════════════════════════════════════
    # TAB 3 — DEEP DIVE
    # ════════════════════════════════════════════════════════════════════════
    with tab3:
        st.markdown("### Deep Dive")

        ticker_list = sorted(fdf['Ticker'].tolist()) if not fdf.empty else []
        if ticker_list:
            selected_ticker = st.selectbox("Select asset", ticker_list)

            row = fdf[fdf['Ticker'] == selected_ticker].iloc[0]

            # ── Key metrics row ─────────────────────────────────────────────
            m1, m2, m3, m4, m5, m6 = st.columns(6)
            m1.metric("Price",       f"${row['Price']:.2f}")
            m2.metric("Fair Value",  f"${row['intrinsic_value']:.2f}" if row.get('intrinsic_value', 0) > 0 else "N/A",
                      help="DCF intrinsic value per share")
            m3.metric("Upside %",    f"{row.get('margin_of_safety', 0):.1f}%",
                      help="Positive = undervalued vs intrinsic value")
            m4.metric("Oracle Score", f"{row.get('Oracle_Score', 0):.0f}")
            m5.metric("RSI",          f"{row.get('RSI', 0):.1f}",
                      help="<30 oversold, >70 overbought")
            m6.metric("Risk",         row.get('risk_level', '–'))

            st.markdown("---")

            # ── Valuation Football Field ─────────────────────────────────────
            st.markdown("#### Valuation Football Field")
            st.caption("Shows where the current price sits relative to DCF-derived valuation bands. **Green band** = fair value ±20%. Dashed red line = today's price. Blue line = intrinsic value.")

            iv = row.get('intrinsic_value', 0)
            cp = row.get('Price', 0)

            if iv > 0:
                fig_ff = go.Figure()
                bands = [
                    (iv * 0.5,  iv * 0.8,  "rgba(59,130,246,0.10)", "Deep Value Zone"),
                    (iv * 0.8,  iv * 1.2,  "rgba(34,197,94,0.15)",  "Fair Value Zone"),
                    (iv * 1.2,  iv * 1.5,  "rgba(245,158,11,0.10)", "Premium Zone"),
                    (iv * 1.5,  iv * 2.0,  "rgba(239,68,68,0.08)",  "Overvalued Zone"),
                ]
                for y0, y1, color, name in bands:
                    fig_ff.add_shape(type="rect", x0=0, x1=1,
                                     y0=y0, y1=y1,
                                     fillcolor=color, line_width=0)
                    fig_ff.add_annotation(x=0.98, y=(y0 + y1) / 2, text=name,
                                          showarrow=False, xanchor="right",
                                          font=dict(size=10, color="#71717a"))

                fig_ff.add_hline(y=cp, line_dash="dash", line_color="#ef4444", line_width=2,
                                 annotation_text=f"Current  ${cp:.2f}",
                                 annotation_font_color="#ef4444", annotation_position="right")
                fig_ff.add_hline(y=iv, line_color="#3b82f6", line_width=1.5,
                                 annotation_text=f"Intrinsic  ${iv:.2f}",
                                 annotation_font_color="#3b82f6", annotation_position="right")

                fig_ff.update_layout(
                    height=380,
                    showlegend=False,
                    xaxis=dict(showticklabels=False, showgrid=False, zeroline=False),
                    yaxis_title="Price ($)",
                )
                fig_ff = plotly_dark_layout(fig_ff)
                st.plotly_chart(fig_ff, use_container_width=True)
            else:
                st.info("DCF valuation not available — the company may not have sufficient cash-flow history (requires ≥ 2 years of financial statements).")

            # ── Multi-article news feed ────────────────────────────────────
            st.markdown("#### Latest News Intelligence")
            st.caption("Up to 5 recent headlines with AI sentiment labels and one-line summaries.")

            articles = parse_articles(row.get('Articles_JSON', '[]'))
            if articles:
                for a in articles:
                    css   = article_css_class(a.get('label', 'Neutral'))
                    badge = article_badge(a.get('label', 'Neutral'))
                    score_txt = f"{a.get('score', 0):+.2f}"
                    pub   = a.get('pub_date', '')[:16]
                    st.markdown(
                        f'<div class="article-card {css}">'
                        f'  <div class="article-title">{a["headline"]}</div>'
                        f'  <div class="article-meta">'
                        f'    {badge} &nbsp; <span style="color:#52525b">{pub}</span>'
                        f'    &nbsp;·&nbsp; <span style="color:#a1a1aa">{a.get("reasoning","")}</span>'
                        f'    &nbsp;·&nbsp; <span style="font-family:DM Mono;color:#3b82f6">score {score_txt}</span>'
                        f'  </div>'
                        f'</div>',
                        unsafe_allow_html=True
                    )
            else:
                st.caption("No article data — run a fresh scan to populate.")

            # ── Risk profile ────────────────────────────────────────────────
            st.markdown("---")
            st.markdown("#### Risk Profile")
            r1, r2, r3, r4 = st.columns(4)
            r1.metric("Position Size Rec.", f"{row.get('position_size', 0):.1f}%",
                      help="Max % of $100k portfolio to allocate based on 2× ATR stop-loss")
            r2.metric("Daily Volatility",   f"{row.get('daily_volatility', 0):.2f}%",
                      help="ATR as % of price — a proxy for daily expected move")
            r3.metric("Short Interest",     f"{row.get('short_interest', 0):.1f}%",
                      help="% of float sold short. >15% = bearish pressure")
            r4.metric("Insider Sentiment",  row.get('insider_sentiment', '–'))

            if row.get('Geo_Risk', False):
                st.warning("🌍 Geopolitical risk detected in recent news headlines.")

            st.markdown(f"[Open on Yahoo Finance ↗](https://finance.yahoo.com/quote/{selected_ticker})", unsafe_allow_html=False)


    # ════════════════════════════════════════════════════════════════════════
    # TAB 4 — MARKET INTEL
    # ════════════════════════════════════════════════════════════════════════
    with tab4:
        st.markdown("### Market Intelligence")

        # ── Sector summary table ────────────────────────────────────────────
        if 'Sector' in fdf.columns:
            st.markdown("#### Sector Breakdown")
            agg_cols = {k: 'mean' for k in ['Oracle_Score', 'margin_of_safety', 'RSI', 'daily_volatility'] if k in fdf.columns}
            agg_cols['Ticker'] = 'count'
            sector_agg = fdf.groupby('Sector').agg(agg_cols).round(1).rename(columns={'Ticker': 'Count'})
            st.dataframe(sector_agg, use_container_width=True)

        # ── Region breakdown ────────────────────────────────────────────────
        if 'Region' in fdf.columns and 'Oracle_Score' in fdf.columns:
            st.markdown("#### Region vs Oracle Score")
            fig_region = px.box(
                fdf, x='Region', y='Oracle_Score', color='Region',
                color_discrete_sequence=['#3b82f6', '#22c55e', '#f59e0b', '#a855f7', '#ef4444'],
                labels={'Oracle_Score': 'Oracle Score', 'Region': 'Region'},
            )
            fig_region = plotly_dark_layout(fig_region)
            fig_region.update_layout(showlegend=False)
            st.plotly_chart(fig_region, use_container_width=True)

        # ── Value vs Momentum scatter (all stocks) ────────────────────────
        if {'RSI', 'margin_of_safety', 'Oracle_Score', 'Sector'}.issubset(fdf.columns):
            st.markdown("#### Value vs Momentum — Full Universe")
            st.caption("**Ideal zone**: right side (undervalued) + below RSI 70 (not overbought). Bubble size = Oracle Score.")
            fig_vm = px.scatter(
                fdf,
                x='margin_of_safety',
                y='RSI',
                size=fdf['Oracle_Score'].clip(lower=5),
                color='Sector',
                hover_name='Ticker',
                labels={'margin_of_safety': 'Valuation Upside %', 'RSI': 'Momentum (RSI)'},
            )
            fig_vm.add_hline(y=70, line_dash="dot", line_color="#ef4444",
                             annotation_text="Overbought", annotation_font_color="#ef4444")
            fig_vm.add_hline(y=30, line_dash="dot", line_color="#22c55e",
                             annotation_text="Oversold", annotation_font_color="#22c55e")
            fig_vm.add_vline(x=0,  line_dash="dash", line_color="#52525b")
            fig_vm = plotly_dark_layout(fig_vm)
            st.plotly_chart(fig_vm, use_container_width=True)

        # ── Sentiment overview ────────────────────────────────────────────
        if 'Sentiment' in fdf.columns:
            st.markdown("#### Sentiment Overview")
            avg_sent = fdf['Sentiment'].mean()
            sc1, sc2, sc3 = st.columns(3)
            sc1.metric("Average Sentiment", f"{avg_sent:.3f}",
                       delta="Bullish leaning" if avg_sent > 0 else "Bearish leaning")
            if 'Geo_Risk' in fdf.columns:
                sc2.metric("Geo-Risk Count", int(fdf['Geo_Risk'].sum()),
                           help="Assets with geopolitical keywords in recent headlines")
            if 'Oracle_Score' in fdf.columns:
                hc_pct = len(fdf[fdf['Oracle_Score'] > 75]) / max(len(fdf), 1) * 100
                sc3.metric("High Conviction %", f"{hc_pct:.1f}%")


# ─────────────────────────────────────────────────────────────────────────────
# EMPTY STATE
# ─────────────────────────────────────────────────────────────────────────────
else:
    st.markdown("""
    <div style="max-width:520px;margin:80px auto;text-align:center;">
      <div style="font-size:48px;margin-bottom:16px;">◈</div>
      <h2 style="color:#f4f4f5;font-size:22px;font-weight:700;margin-bottom:8px;">Welcome to Apex Markets</h2>
      <p style="color:#71717a;font-size:14px;line-height:1.6;">
        Global investment intelligence across 500+ assets.<br>
        Press <strong style="color:#3b82f6">⟳ Run Global Scan</strong> in the sidebar to begin.
      </p>
    </div>
    """, unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns(4)
    for col, icon, title, desc in [
        (c1, "◎", "DCF Engine",       "Intrinsic value from real cash-flow data"),
        (c2, "◈", "LLM News",         "5 articles per stock with sentiment scoring"),
        (c3, "⬡", "Risk Manager",     "ATR-based position sizing & volatility maps"),
        (c4, "◻", "500+ Stocks",      "US, Europe, Asia, EM — all in one scan"),
    ]:
        col.markdown(
            f'<div style="background:#111113;border:1px solid #27272a;border-radius:12px;'
            f'padding:20px;text-align:center;">'
            f'<div style="font-size:28px;margin-bottom:8px;color:#3b82f6">{icon}</div>'
            f'<div style="font-weight:600;color:#e4e4e7;font-size:13px">{title}</div>'
            f'<div style="color:#52525b;font-size:11px;margin-top:4px">{desc}</div>'
            f'</div>',
            unsafe_allow_html=True
        )
