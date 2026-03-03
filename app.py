import streamlit as st
import pandas as pd
import json
import os
from logic import InvestmentEngine, generate_ai_report

st.set_page_config(page_title="Apex Pro", layout="wide", page_icon="◈")

# APPLE DESIGN SYSTEM
st.markdown("""
<style>
    .stApp { background-color: #000000; color: #f5f5f7; font-family: -apple-system, system-ui, sans-serif; }
    [data-testid="metric-container"] { background-color: #1c1c1e; border: 1px solid #2c2c2e; border-radius: 14px; padding: 20px; }
    .stDataFrame { background-color: #1c1c1e; border-radius: 12px; }
    .stButton>button { background-color: #0A84FF !important; color: white !important; border-radius: 10px; border: none; padding: 10px 24px; font-weight: 600; }
    section[data-testid="stSidebar"] { background-color: #1c1c1e; border-right: 1px solid #2c2c2e; }
    h1, h2, h3 { font-weight: 600; letter-spacing: -0.02em; }
    .stTabs [data-baseweb="tab-list"] { gap: 24px; border-bottom: 1px solid #2c2c2e; }
    .stTabs [aria-selected="true"] { color: #0A84FF; border-bottom: 2px solid #0A84FF; }
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

with st.sidebar:
    st.markdown("## ◈ Apex Pro")
    if st.button("📡 Global Scan (500+ Assets)", type="primary", use_container_width=True):
        pb = st.progress(0, "Warming Engines...")
        st.session_state["data"] = engine.fetch_market_data(pb)
        pb.empty()
        st.rerun()
    
    if st.button("🧹 Clear Cache", use_container_width=True):
        if os.path.exists("market_cache.csv"): os.remove("market_cache.csv")
        st.session_state["data"] = pd.DataFrame()
        st.rerun()

df = st.session_state["data"]
if df.empty:
    st.info("Market engine offline. Click Global Scan.")
    st.stop()

# --- TOP HUD ---
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Universe", len(df))
    with st.expander("Sectors"): st.table(df['Sector'].value_counts().head(5))
with c2:
    hot = df[df['Oracle_Score'] > 75]
    st.metric("Hot Picks", len(hot))
    if not hot.empty:
        with st.expander("Show Tickers"): st.write(", ".join(hot['Ticker'].tolist()))
with c3:
    dv = df[df['margin_of_safety'] > 20]
    st.metric("Deep Value", len(dv))
    if not dv.empty:
        with st.expander("Show Tickers"): st.write(", ".join(dv['Ticker'].tolist()))
with c4:
    hr = df[df['risk_level'] == 'High']
    st.metric("High Risk", len(hr))
    if not hr.empty:
        with st.expander("Show Tickers"): st.write(", ".join(hr['Ticker'].tolist()))

# --- TABS ---
tab1, tab2 = st.tabs(["📊 Market Monitor", "🧠 Deep Analysis"])

with tab1:
    c_l, c_r = st.columns(2)
    with c_l:
        st.markdown("### 🟢 Top Conviction")
        st.dataframe(df.sort_values("Oracle_Score", ascending=False).head(20)[['Ticker', 'Price', 'margin_of_safety', 'Oracle_Score']], hide_index=True, use_container_width=True)
    with c_r:
        st.markdown("### 🔴 Risk Alerts")
        st.dataframe(df.sort_values("margin_of_safety", ascending=True).head(20)[['Ticker', 'Price', 'margin_of_safety', 'risk_level']], hide_index=True, use_container_width=True)

with tab2:
    sel = st.selectbox("Select Asset", sorted(df['Ticker'].unique()))
    if sel:
        row = df[df['Ticker'] == sel].iloc[0]
        st.markdown(f"## {sel} Analysis")
        
        cl1, cl2 = st.columns([2, 1])
        with cl1:
            if st.button(f"Generate Report for {sel}", type="primary"):
                with st.spinner("AI analyzing wire data and financials..."):
                    n = json.loads(row['Articles_JSON'])
                    rep = generate_ai_report(sel, row.to_dict(), n, OPENAI_API_KEY)
                    st.session_state[f"rep_{sel}"] = rep
            if f"rep_{sel}" in st.session_state: st.markdown(st.session_state[f"rep_{sel}"])
        
        with cl2:
            st.markdown("#### 📰 Wire Feed")
            news = json.loads(row['Articles_JSON'])
            for item in news:
                st.markdown(f"**{item['headline']}**")
                st.caption(item['reason'])
                st.divider()
