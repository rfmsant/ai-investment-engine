import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import os
from logic import InvestmentEngine, generate_ai_report

# 1. PAGE CONFIG
st.set_page_config(page_title="Apex Markets Pro", layout="wide", page_icon="◈")

# 2. CUSTOM CSS (Advanced Styling)
st.markdown("""
<style>
    .stApp { background-color: #09090b; color: #e4e4e7; }
    /* Cards */
    [data-testid="metric-container"] { background-color: #18181b; border: 1px solid #27272a; border-radius: 8px; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1); }
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] { background-color: #000000; border-radius: 4px; padding: 10px 20px; color: #a1a1aa; }
    .stTabs [aria-selected="true"] { background-color: #27272a; color: #fff; border: 1px solid #3f3f46; }
    /* Headers */
    h1, h2, h3 { font-family: 'DM Sans', sans-serif; letter-spacing: -0.5px; }
    /* Buttons */
    .stButton>button { border-radius: 6px; font-weight: 600; transition: all 0.2s; }
</style>
""", unsafe_allow_html=True)

# 3. SECURE API KEY
OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY")

# 4. ENGINE
@st.cache_resource
def get_engine():
    return InvestmentEngine()

engine = get_engine()

if "data" not in st.session_state:
    st.session_state["data"] = pd.DataFrame()

if st.session_state["data"].empty:
    df_c, ok = engine.load_data()
    st.session_state["data"] = df_c

# 5. SIDEBAR
with st.sidebar:
    st.title("◈ Apex Pro")
    st.caption("Institutional Investment Engine")
    
    if OPENAI_API_KEY:
        st.success("🧠 AI Cortex: Online")
    else:
        st.error("🧠 AI Cortex: Offline")

    st.divider()

    if st.button("📡 Run Global Scan", type="primary"):
        with st.spinner("Triangulating market data..."):
            pb = st.progress(0, "Initializing...")
            st.session_state["data"] = engine.fetch_market_data(pb)
            pb.empty()
        st.success("Market Data Updated")
        st.rerun()

    if st.button("🗑️ Flush Cache"):
        if os.path.exists("market_cache.csv"):
            os.remove("market_cache.csv")
        st.session_state["data"] = pd.DataFrame()
        st.rerun()

# 6. MAIN DASHBOARD
df = st.session_state["data"]

if df.empty:
    st.info("⚠️ System Idle. Click 'Run Global Scan' in the sidebar.")
    st.stop()

# --- TOP METRICS ---
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Universe", len(df))
c2.metric("High Conviction (>75)", len(df[df['Oracle_Score'] > 75]))
c3.metric("Deep Value (MoS > 20%)", len(df[df['margin_of_safety'] > 20]))
c4.metric("Market Sentiment", f"{df['Sentiment'].mean():.2f}")
geo_risk_count = df['Articles_JSON'].apply(lambda x: 'war' in x.lower() or 'tension' in x.lower() if x else False).sum()
c5.metric("Geo-Risk Flags", geo_risk_count)

# --- TABS ---
tab1, tab2, tab3, tab4 = st.tabs(["📊 Terminal", "🧪 Risk Lab", "🧠 Deep Dive (AI)", "🗺️ Market Intel"])

# === TAB 1: TERMINAL ===
with tab1:
    st.subheader("Asset Screener")
    sectors = st.multiselect("Filter Sector", df['Sector'].unique())
    view_df = df[df['Sector'].isin(sectors)] if sectors else df
    
    st.dataframe(
        view_df[['Ticker', 'Sector', 'Price', 'intrinsic_value', 'margin_of_safety', 'Oracle_Score', 'AI_Verdict', 'RSI']],
        column_config={
            "Price": st.column_config.NumberColumn(format="$%.2f"),
            "intrinsic_value": st.column_config.NumberColumn("Fair Value", format="$%.2f"),
            "margin_of_safety": st.column_config.NumberColumn("Upside", format="%.1f%%"),
            "Oracle_Score": st.column_config.ProgressColumn("Oracle", min_value=0, max_value=100),
            "RSI": st.column_config.NumberColumn("RSI", help="<30 Oversold, >70 Overbought")
        },
        use_container_width=True, height=600, hide_index=True
    )

# === TAB 2: RISK LAB (Restored Heatmaps) ===
with tab2:
    st.subheader("Risk Laboratory")
    
    col_a, col_b = st.columns(2)
    
    with col_a:
        st.markdown("**Valuation vs. Momentum Matrix**")
        # X: Margin of Safety, Y: RSI, Size: Score, Color: Sector
        fig_heat = px.scatter(
            df, x="margin_of_safety", y="RSI",
            size="Oracle_Score", color="Sector",
            hover_name="Ticker",
            title="Opportunity Map (Right-Bottom is Ideal)",
            labels={"margin_of_safety": "Undervaluation %", "RSI": "Momentum"},
            template="plotly_dark"
        )
        fig_heat.add_hline(y=30, line_dash="dot", line_color="green", annotation_text="Oversold")
        fig_heat.add_vline(x=20, line_dash="dot", line_color="green", annotation_text="Deep Value")
        st.plotly_chart(fig_heat, use_container_width=True)
        
    with col_b:
        st.markdown("**Volatility & Allocation**")
        # Bar chart of risk levels
        if 'risk_level' in df.columns:
            risk_counts = df['risk_level'].value_counts()
            fig_risk = px.bar(
                risk_counts, 
                title="Portfolio Risk Distribution",
                template="plotly_dark",
                color=risk_counts.index,
                color_discrete_map={"High": "#ef4444", "Medium": "#f59e0b", "Low": "#22c55e"}
            )
            st.plotly_chart(fig_risk, use_container_width=True)

# === TAB 3: DEEP DIVE (AI) ===
with tab3:
    st.subheader("Motley Fool Style Analysis")
    selected_ticker = st.selectbox("Select Asset for Deep Dive", df['Ticker'].unique())
    
    if selected_ticker:
        row = df[df['Ticker'] == selected_ticker].iloc[0]
        
        # Financial Strip
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Price", f"${row['Price']}")
        m2.metric("Fair Value", f"${row['intrinsic_value']}")
        m3.metric("Safety Margin", f"{row['margin_of_safety']}%")
        m4.metric("Growth Est", f"{row['growth_rate']}%")
        
        st.divider()
        
        col_report, col_news = st.columns([2, 1])
        
        with col_report:
            if not OPENAI_API_KEY:
                st.warning("⚠️ Add OPENAI_API_KEY to secrets to unlock the Analyst Report.")
            else:
                if st.button(f"✨ Generate Strategic Report for {selected_ticker}", type="primary"):
                    with st.spinner("🤖 Consulting the Investment Committee..."):
                        news_list = json.loads(row['Articles_JSON']) if isinstance(row['Articles_JSON'], str) else []
                        report = generate_ai_report(selected_ticker, row.to_dict(), news_list, OPENAI_API_KEY)
                        st.session_state[f"rep_{selected_ticker}"] = report
                
                if f"rep_{selected_ticker}" in st.session_state:
                    st.markdown(st.session_state[f"rep_{selected_ticker}"])

        with col_news:
            st.markdown("#### 📰 Wire Feed")
            try:
                news_items = json.loads(row['Articles_JSON'])
                if not news_items: st.caption("No wire data found.")
                for n in news_items[:6]:
                    sentiment_icon = "🟢" if n['score'] > 0.1 else "🔴" if n['score'] < -0.1 else "⚪"
                    st.markdown(f"{sentiment_icon} [{n['headline']}]({n['link']})")
                    st.caption(f"Reasoning: {n['reason']}")
                    st.markdown("---")
            except:
                st.caption("Wire data unavailable.")

# === TAB 4: MARKET INTEL ===
with tab4:
    st.subheader("Market Intelligence")
    if 'Sector' in df.columns:
        # Aggregation
        sector_perf = df.groupby("Sector")[['Oracle_Score', 'margin_of_safety', 'Sentiment']].mean().reset_index()
        st.dataframe(sector_perf.style.background_gradient(cmap="RdYlGn"), use_container_width=True)
        
        fig_sec = px.treemap(
            df, path=['Sector', 'Ticker'], values='Price',
            color='margin_of_safety',
            color_continuous_scale='RdYlGn',
            color_continuous_midpoint=0,
            title="Market Valuation Map (Green = Undervalued)",
            template="plotly_dark"
        )
        st.plotly_chart(fig_sec, use_container_width=True)
