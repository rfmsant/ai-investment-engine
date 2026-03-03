import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import json, os, ast
from logic import InvestmentEngine, generate_ai_report

st.set_page_config(page_title="Apex Markets", layout="wide", page_icon="◈")

# ===========================================================================
# CSS & STYLING
# ===========================================================================
st.markdown("""
<style>
.stApp{background:#09090b;color:#e4e4e7;}
[data-testid="metric-container"]{background:#18181b;border:1px solid #27272a;border-radius:8px;padding:10px;}
.stButton>button[kind="primary"]{background:#2563eb;color:white;border:none;}
div[data-testid="stExpander"] details summary p{font-weight: 600; color: #a1a1aa;}
</style>
""", unsafe_allow_html=True)

# ===========================================================================
# SESSION & DATA LOADING
# ===========================================================================
@st.cache_resource
def get_engine():
    return InvestmentEngine()

engine = get_engine()

if "data" not in st.session_state:
    st.session_state["data"] = pd.DataFrame()

if st.session_state["data"].empty:
    df_c, ok = engine.load_data()
    st.session_state["data"] = df_c

# ===========================================================================
# SIDEBAR - SECURE API KEY
# ===========================================================================
with st.sidebar:
    st.title("Apex Markets")
    
    # Check for the key in Streamlit Secrets (for Cloud) or Environment Variables (Local)
    api_key = st.secrets.get("OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY")
    
    if not api_key:
        st.warning("⚠️ OpenAI Key not found in Environment/Secrets.")
        # Optional: Keep the input box ONLY as a fallback for local testing
        api_key = st.text_input("Enter API Key for this session", type="password")
    else:
        st.success("✅ AI Engine Active (Key Loaded)")
# ===========================================================================
# MAIN DASHBOARD
# ===========================================================================
df = st.session_state["data"]

if df.empty:
    st.warning("⚠️ No data loaded. Click 'Run Global Scan' in the sidebar.")
    st.stop()

# Header Metrics
st.markdown("## 🦅 Market Overview")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Assets Scanned", len(df))
col2.metric("Undervalued (>20% MoS)", len(df[df['margin_of_safety'] > 20]))
col3.metric("Strong Buy Signals", len(df[df['Oracle_Score'] > 75]))
col4.metric("Avg Market Sentiment", f"{df['Sentiment'].mean():.2f}")

# Tabs
tab1, tab2, tab3 = st.tabs(["📊 Terminal", "🧠 Deep Dive (AI)", "🔬 Diagnostics"])

# --- TAB 1: TERMINAL ---
with tab1:
    st.subheader("Screening Terminal")
    
    # Simple Filters
    sector_filter = st.multiselect("Filter Sector", df['Sector'].unique())
    if sector_filter:
        view_df = df[df['Sector'].isin(sector_filter)]
    else:
        view_df = df
        
    st.dataframe(
        view_df[['Ticker', 'Price', 'intrinsic_value', 'margin_of_safety', 'Oracle_Score', 'AI_Verdict', 'RSI']],
        column_config={
            "Price": st.column_config.NumberColumn(format="$%.2f"),
            "intrinsic_value": st.column_config.NumberColumn("Fair Value", format="$%.2f"),
            "margin_of_safety": st.column_config.NumberColumn("Upside %", format="%.1f%%"),
            "Oracle_Score": st.column_config.ProgressColumn("Score", min_value=0, max_value=100)
        },
        use_container_width=True,
        height=500
    )

# --- TAB 2: DEEP DIVE (AI ENHANCED) ---
with tab2:
    st.subheader("🤖 AI Investment Analyst")
    
    selected_ticker = st.selectbox("Select Asset for Deep Analysis", df['Ticker'].unique())
    
    if selected_ticker:
        row = df[df['Ticker'] == selected_ticker].iloc[0]
        
        # Financial Snapshot
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Ticker", row['Ticker'])
        c2.metric("Fair Value", f"${row['intrinsic_value']}", delta=f"{row['margin_of_safety']}%")
        c3.metric("Oracle Score", f"{row['Oracle_Score']:.0f}/100")
        c4.metric("Risk Level", row['risk_level'])
        
        st.divider()
        
        # Parse stored news
        try:
            news_data = json.loads(row['Articles_JSON'])
        except:
            news_data = []

        col_ai, col_news = st.columns([2, 1])
        
        with col_ai:
            st.markdown("#### 📝 Motley Fool Style Report")
            
            if "ai_report" not in st.session_state:
                st.session_state["ai_report"] = {}

            # Button to trigger AI
            if st.button(f"Generate AI Analysis for {selected_ticker}", type="primary"):
                if not api_key:
                    st.error("❌ Please enter your OpenAI API Key in the sidebar first.")
                else:
                    with st.spinner("🤖 Consulting the Oracle... (Generating Report)"):
                        # Prepare context
                        ctx = row.to_dict()
                        report = generate_ai_report(selected_ticker, ctx, news_data, api_key)
                        st.session_state["ai_report"][selected_ticker] = report
            
            # Display Report
            if selected_ticker in st.session_state["ai_report"]:
                st.markdown(st.session_state["ai_report"][selected_ticker])
            else:
                st.info("Click the button above to generate a deep-dive analysis.")

        with col_news:
            st.markdown("#### 📰 Recent Headlines")
            for n in news_data[:5]:
                color = "green" if n['label'] == "Bullish" else "red" if n['label'] == "Bearish" else "grey"
                st.markdown(f"**[{n['label']}]** [{n['headline']}]({n['link']})")
                st.caption(f"Reasoning: {n['reason']}")
                st.markdown("---")

# --- TAB 3: DIAGNOSTICS ---
with tab3:
    st.write("Debug Data for Selected Ticker")
    if selected_ticker:
        row = df[df['Ticker'] == selected_ticker].iloc[0]
        st.json(row.to_dict())
