import streamlit as st
import pandas as pd
import json
import os
from logic import generate_ai_report

st.set_page_config(page_title="Apex Pro", layout="wide")

# Cupertino Styling
st.markdown("<style>.stApp { background-color: #000; color: #f5f5f7; }</style>", unsafe_allow_html=True)

if not os.path.exists("market_cache.csv"):
    st.error("Database not found. Please wait for the first GitHub Action scan to complete.")
    st.stop()

df = pd.read_csv("market_cache.csv")
OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY")

st.title("◈ Apex Pro")
st.metric("Total Institutional Coverage", len(df))

tab1, tab2 = st.tabs(["📊 Market Data", "🧠 AI Analyst"])

with tab1:
    st.dataframe(df.sort_values("Oracle_Score", ascending=False), use_container_width=True, hide_index=True)

with tab2:
    sel = st.selectbox("Select Asset", df['Ticker'].unique())
    if sel:
        row = df[df['Ticker'] == sel].iloc[0]
        if st.button("Generate Memo"):
            st.markdown(generate_ai_report(sel, row.to_dict(), json.loads(row['Articles_JSON']), OPENAI_API_KEY))
