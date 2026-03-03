import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import json, os, ast
from logic import InvestmentEngine, generate_ai_report

# 1. PAGE CONFIG
st.set_page_config(page_title="Apex Markets", layout="wide", page_icon="◈")

# 2. CUSTOM CSS
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
.stApp { background: #09090b; color: #e4e4e7; }
[data-testid="metric-container"] { background: #111113; border: 1px solid #1f1f23; border-radius: 10px; padding: 15px; }
.stButton>button { border-radius: 8px !important; }
</style>
""", unsafe_allow_html=True)

# 3. API KEY SECURITY (GitHub Safe)
# This looks for the key in Streamlit Cloud Secrets or local environment
OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY")

# 4. ENGINE & DATA LOADING
@st.cache_resource
def get_engine():
    return InvestmentEngine()

engine = get_engine()

if "data" not in st.session_state:
    st.session_state["data"] = pd.DataFrame()

# Load cached data if available
if st.session_state["data"].empty:
    df_c, ok = engine.load_data()
    st.session_state["data"] = df_c

# 5. SIDEBAR
with st.sidebar:
    st.title("◈ Apex Markets")
    st.caption("Investment Intelligence Engine")
    st.divider()
    
    if st.button("🚀 Run Global Scan", type="primary"):
        with st.spinner("Analyzing Market Universe..."):
            pb = st.progress(0, "Starting...")
            st.session_state["data"] = engine.fetch_market_data(pb)
            pb.empty()
        st.success("Scan Complete!")
        st.rerun()

    if st.button("🧹 Clear All Data"):
        if os.path.exists("market_cache.csv"):
            os.remove("market_cache.csv")
        st.session_state["data"] = pd.DataFrame()
        st.rerun()
    
    st.divider()
    if OPENAI_API_KEY:
        st.success("✅ AI Engine: Connected")
    else:
        st.warning("⚠️ AI Engine: Offline (No Key)")

# 6. MAIN DASHBOARD
df = st.session_state["data"]

if df.empty:
    st.markdown("""
    <div style='text-align: center; padding: 50px;'>
        <h1>◈ Welcome to Apex</h1>
        <p style='color: #71717a;'>Click <b>Run Global Scan</b> in the sidebar to begin analysis.</p>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# Summary Bar
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Scanned", len(df))
c2.metric("Undervalued Assets", len(df[df['margin_of_safety'] > 20]))
c3.metric("High Oracle Score", len(df[df['Oracle_Score'] > 75]))
c4.metric("Avg Sentiment", f"{df['Sentiment'].mean():.2f}")

# Tabs
tab1, tab2, tab3 = st.tabs(["📊 Terminal", "🧠 AI Deep Dive", "🔬 Data View"])

# --- TAB 1: TERMINAL ---
with tab1:
    st.subheader("Market Screening Terminal")
    st.dataframe(
        df[['Ticker', 'Price', 'intrinsic_value', 'margin_of_safety', 'Oracle_Score', 'AI_Verdict']],
        column_config={
            "Price": st.column_config.NumberColumn(format="$%.2f"),
            "intrinsic_value": st.column_config.NumberColumn("Fair Value", format="$%.2f"),
            "margin_of_safety": st.column_config.NumberColumn("Upside %", format="%.1f%%"),
            "Oracle_Score": st.column_config.ProgressColumn("Score", min_value=0, max_value=100)
        },
        use_container_width=True, height=500, hide_index=True
    )

# --- TAB 2: AI DEEP DIVE ---
with tab2:
    st.subheader("Motley Fool Style Deep Analysis")
    selected_ticker = st.selectbox("Select Asset", df['Ticker'].unique())
    
    if selected_ticker:
        row = df[df['Ticker'] == selected_ticker].iloc[0]
        
        # Snapshots
        sc1, sc2, sc3, sc4 = st.columns(4)
        sc1.metric("Current Price", f"${row['Price']}")
        sc2.metric("DCF Value", f"${row['intrinsic_value']}")
        sc3.metric("Growth Est", f"{row['growth_rate']}%")
        sc4.metric("Market Sentiment", row['Sentiment'])
        
        st.divider()
        
        col_report, col_news = st.columns([2, 1])
        
        with col_report:
            if not OPENAI_API_KEY:
                st.error("AI Report unavailable. Please add 'OPENAI_API_KEY' to your Streamlit Secrets.")
            else:
                if st.button(f"Generate Motley Fool Report for {selected_ticker}", type="primary"):
                    with st.spinner("🤖 Analyzing financials and news..."):
                        news_list = json.loads(row['Articles_JSON'])
                        report = generate_ai_report(selected_ticker, row.to_dict(), news_list, OPENAI_API_KEY)
                        st.session_state[f"rep_{selected_ticker}"] = report
                
                if f"rep_{selected_ticker}" in st.session_state:
                    st.markdown(st.session_state[f"rep_{selected_ticker}"])
                else:
                    st.info("Click the button above to start the AI analysis.")

        with col_news:
            st.markdown("#### Latest News Signals")
            news_items = json.loads(row['Articles_JSON'])
            for n in news_items[:5]:
                st.markdown(f"**{n['label']}**: [{n['headline']}]({n['link']})")
                st.caption(f"Score: {n['score']} | {n['reason']}")
                st.divider()

# --- TAB 3: DATA VIEW ---
with tab3:
    st.subheader("Raw Financial Diagnostics")
    st.dataframe(df)
