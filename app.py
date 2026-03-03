import streamlit as st
import pandas as pd
import json
import os
import time
from logic import InvestmentEngine, generate_ai_report

st.set_page_config(page_title="Apex Pro", layout="wide", page_icon="◈")

# Design
st.markdown("""
<style>
    .stApp { background-color: #000; color: #f5f5f7; }
    [data-testid="metric-container"] { background-color: #1c1c1e; border: 1px solid #2c2c2e; border-radius: 12px; }
    .stButton>button { background-color: #0A84FF !important; color: white !important; border-radius: 8px; width: 100%; }
</style>
""", unsafe_allow_html=True)

OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY")
engine = InvestmentEngine()

# State
df = engine.load_data()

with st.sidebar:
    st.markdown("## ◈ Apex Pro")
    
    # Progress Calculation
    total_t = 500 # Target
    current_t = len(df)
    progress = min(current_t / total_t, 1.0)
    st.progress(progress, text=f"{current_t} / {total_t} Stocks")

    if st.button("🚀 Start/Resume Batch Scan"):
        with st.spinner("Processing next 10 stocks..."):
            df, more = engine.run_batch(batch_size=10)
            st.rerun() # Refresh to update UI and keep browser active

    if st.button("🧹 Reset System"):
        if os.path.exists("market_cache.csv"): os.remove("market_cache.csv")
        st.rerun()

if df.empty:
    st.title("System Offline")
    st.info("Click 'Start/Resume Batch Scan' to populate the database.")
    st.stop()

# HUD
c1, c2, c3, c4 = st.columns(4)
c1.metric("Universe", len(df))
c2.metric("Hot Picks", len(df[df['Oracle_Score'] > 75]))
c3.metric("Deep Value", len(df[df['margin_of_safety'] > 20]))
c4.metric("Avg Score", f"{df['Oracle_Score'].mean():.1f}")

tab1, tab2 = st.tabs(["📊 Market", "🧠 Analyst"])

with tab1:
    st.dataframe(df.sort_values("Oracle_Score", ascending=False), use_container_width=True, hide_index=True)

with tab2:
    sel = st.selectbox("Select Asset", sorted(df['Ticker'].unique()))
    if sel:
        row = df[df['Ticker'] == sel].iloc[0]
        col_l, col_r = st.columns([2, 1])
        with col_l:
            if st.button(f"Generate Memo"):
                with st.spinner("AI Analysis..."):
                    n = json.loads(row['Articles_JSON'])
                    st.markdown(generate_ai_report(sel, row.to_dict(), n, OPENAI_API_KEY))
        with col_r:
            st.markdown("#### News Wire")
            for i in json.loads(row['Articles_JSON']):
                st.write(f"**{i['headline']}**")
                st.caption(i['reason'])
                st.divider()
