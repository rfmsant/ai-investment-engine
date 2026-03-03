import streamlit as st
import pandas as pd
import plotly.express as px
import json
import os
from logic import InvestmentEngine, generate_ai_report

st.set_page_config(page_title="Apex Markets Pro", layout="wide", page_icon="◈")

# Custom CSS
st.markdown("""
<style>
    .stApp { background-color: #09090b; color: #e4e4e7; }
    [data-testid="metric-container"] { background-color: #18181b; border: 1px solid #27272a; padding: 10px; border-radius: 6px; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] { background-color: #000000; border-radius: 4px; color: #a1a1aa; font-size: 14px; }
    .stTabs [aria-selected="true"] { background-color: #2563eb; color: #fff; }
</style>
""", unsafe_allow_html=True)

OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY")

@st.cache_resource
def get_engine(): return InvestmentEngine()

engine = get_engine()

if "data" not in st.session_state: st.session_state["data"] = pd.DataFrame()
if st.session_state["data"].empty: 
    df_c, ok = engine.load_data()
    st.session_state["data"] = df_c

# SIDEBAR
with st.sidebar:
    st.title("◈ Apex Pro")
    st.caption("Institutional Intelligence")
    
    if st.button("📡 Global Scan (~3 mins)", type="primary"):
        with st.spinner("Scanning 100+ Institutional Assets..."):
            pb = st.progress(0, "Initializing...")
            st.session_state["data"] = engine.fetch_market_data(pb)
            pb.empty()
        st.rerun()

    # LEGEND
    with st.expander("📘 Metrics Legend"):
        st.markdown("""
        **Oracle Score:** 0-100 Rating (High is Better).
        **Fair Value:** Theoretical value based on cash flow.
        **Upside %:** Potential profit to Fair Value.
        **Risk Level:** Based on daily price volatility.
        **RSI:** <30 (Buy Zone), >70 (Sell Zone).
        """)

df = st.session_state["data"]
if df.empty:
    st.info("System Idle. Click 'Global Scan' to begin.")
    st.stop()

# METRICS
c1, c2, c3, c4 = st.columns(4)
c1.metric("Universe Size", len(df), help="Total assets scanned")
c2.metric("Hot Picks", len(df[df['Oracle_Score'] > 70]), help="Score > 70")
c3.metric("Deep Value", len(df[df['margin_of_safety'] > 20]), help="Upside > 20%")
c4.metric("High Risk", len(df[df['risk_level'] == 'High']), help="High Volatility")

tab1, tab2, tab3 = st.tabs(["🔥 Hot vs Not", "🧠 Deep Dive", "🧪 Risk Map"])

# --- TAB 1: HOT VS NOT ---
with tab1:
    col_hot, col_cold = st.columns(2)
    with col_hot:
        st.subheader("🔥 Top Opportunities")
        hot_df = df[(df['Oracle_Score'] > 60) | (df['margin_of_safety'] > 15)].sort_values("Oracle_Score", ascending=False).head(15)
        st.dataframe(hot_df[['Ticker', 'Sector', 'Price', 'margin_of_safety', 'Oracle_Score']], hide_index=True, use_container_width=True)

    with col_cold:
        st.subheader("❄️ High Risk / Avoid")
        cold_df = df[(df['Oracle_Score'] < 40) | (df['margin_of_safety'] < -20)].sort_values("Oracle_Score", ascending=True).head(15)
        st.dataframe(cold_df[['Ticker', 'Sector', 'Price', 'margin_of_safety', 'risk_level']], hide_index=True, use_container_width=True)

# --- TAB 2: DEEP DIVE ---
with tab2:
    selected = st.selectbox("Select Asset", df['Ticker'].unique())
    if selected:
        row = df[df['Ticker'] == selected].iloc[0]
        
        c_stats, c_news = st.columns([1, 1])
        with c_stats:
            st.metric("Fair Value (DCF)", f"${row['intrinsic_value']}")
            st.metric("Upside Potential", f"{row['margin_of_safety']}%")
            
            if OPENAI_API_KEY:
                if st.button(f"Generate AI Report for {selected}", type="primary"):
                    with st.spinner("Consulting Analyst..."):
                        n_list = json.loads(row['Articles_JSON']) if isinstance(row['Articles_JSON'], str) else []
                        rep = generate_ai_report(selected, row.to_dict(), n_list, OPENAI_API_KEY)
                        st.session_state[f"rep_{selected}"] = rep
                if f"rep_{selected}" in st.session_state:
                    st.markdown(st.session_state[f"rep_{selected}"])
            else:
                st.warning("Connect API Key for Reports")

        with c_news:
            st.subheader("Wire Feed")
            try:
                news_items = json.loads(row['Articles_JSON'])
                for n in news_items[:5]:
                    st.markdown(f"**{n['reason']}**") # SHOWS THE CATEGORY (e.g. Earnings)
                    st.markdown(f"[{n['headline']}]({n['link']})")
                    st.divider()
            except: st.write("No news.")

# --- TAB 3: RISK MAP (TREEMAP) ---
with tab3:
    st.subheader("Market Risk Visualizer")
    st.caption("Click a box to zoom in. Size = Oracle Score. Color = Risk Level.")
    
    # TREEMAP: Group by Sector -> Risk Level -> Ticker
    fig = px.treemap(
        df, 
        path=[px.Constant("Market"), 'risk_level', 'Sector', 'Ticker'], 
        values='Oracle_Score',
        color='risk_level',
        color_discrete_map={"Low":"#22c55e", "Medium":"#f59e0b", "High":"#ef4444", "(?)":"#333"},
        template="plotly_dark",
        height=600
    )
    st.plotly_chart(fig, use_container_width=True)
