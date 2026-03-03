import streamlit as st
import pandas as pd
import json
import os
from logic import InvestmentEngine, generate_ai_report

# 1. PAGE CONFIGURATION
st.set_page_config(page_title="Apex Markets", layout="wide", page_icon="◈")

# 2. CUSTOM STYLING (Dark Mode)
st.markdown("""
<style>
    .stApp { background-color: #09090b; color: #e4e4e7; }
    [data-testid="metric-container"] { background-color: #18181b; border: 1px solid #27272a; border-radius: 8px; }
    div[data-testid="stExpander"] details summary p { font-weight: 600; }
    .stButton>button { width: 100%; border-radius: 6px; font-weight: 600; }
</style>
""", unsafe_allow_html=True)

# 3. SECURE API KEY RETRIEVAL
# This pulls from .streamlit/secrets.toml locally OR Streamlit Cloud Secrets
api_key = st.secrets.get("OPENAI_API_KEY", None)

# 4. INITIALIZE ENGINE
@st.cache_resource
def get_engine():
    return InvestmentEngine()

engine = get_engine()

# Load Data State
if "data" not in st.session_state:
    st.session_state["data"] = pd.DataFrame()

if st.session_state["data"].empty:
    df_c, ok = engine.load_data()
    st.session_state["data"] = df_c

# 5. SIDEBAR
with st.sidebar:
    st.title("◈ Apex Markets")
    st.caption("AI-Powered Investment Engine")
    
    st.divider()
    
    # status indicator
    if api_key:
        st.success("✅ AI Engine Online")
    else:
        st.error("❌ AI Engine Offline (Missing Key)")
        st.caption("Add OPENAI_API_KEY to secrets.toml")

    st.divider()

    if st.button("🚀 Run Global Scan", type="primary"):
        with st.spinner("Scanning market universe..."):
            pb = st.progress(0, "Initializing...")
            st.session_state["data"] = engine.fetch_market_data(pb)
            pb.empty()
        st.success("Scan Complete")
        st.rerun()

    if st.button("🧹 Clear Cache"):
        if os.path.exists("market_cache.csv"):
            os.remove("market_cache.csv")
        st.session_state["data"] = pd.DataFrame()
        st.rerun()

# 6. MAIN DASHBOARD
df = st.session_state["data"]

if df.empty:
    st.info("⚠️ No data loaded. Click 'Run Global Scan' in the sidebar to begin.")
    st.stop()

# Top Metrics
c1, c2, c3, c4 = st.columns(4)
c1.metric("Assets Scanned", len(df))
c2.metric("Undervalued (>20%)", len(df[df['margin_of_safety'] > 20]))
c3.metric("High Conviction", len(df[df['Oracle_Score'] > 75]))
c4.metric("Avg Sentiment", f"{df['Sentiment'].mean():.2f}")

# Tabs
tab1, tab2, tab3 = st.tabs(["📊 Terminal", "🧠 AI Deep Dive", "🔬 Diagnostics"])

# --- TAB 1: TERMINAL ---
with tab1:
    st.subheader("Market Screening Terminal")
    
    # Filter
    sectors = st.multiselect("Filter Sector", df['Sector'].unique())
    view_df = df[df['Sector'].isin(sectors)] if sectors else df
    
    st.dataframe(
        view_df[['Ticker', 'Price', 'intrinsic_value', 'margin_of_safety', 'Oracle_Score', 'AI_Verdict']],
        column_config={
            "Price": st.column_config.NumberColumn(format="$%.2f"),
            "intrinsic_value": st.column_config.NumberColumn("Fair Value", format="$%.2f"),
            "margin_of_safety": st.column_config.NumberColumn("Upside %", format="%.1f%%"),
            "Oracle_Score": st.column_config.ProgressColumn("Score", min_value=0, max_value=100)
        },
        use_container_width=True,
        height=500,
        hide_index=True
    )

# --- TAB 2: AI DEEP DIVE ---
with tab2:
    st.subheader("Motley Fool Style Analysis")
    
    selected_ticker = st.selectbox("Select Asset", df['Ticker'].unique())
    
    if selected_ticker:
        row = df[df['Ticker'] == selected_ticker].iloc[0]
        
        # Financial Snapshot
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Current Price", f"${row['Price']}")
        m2.metric("Fair Value (DCF)", f"${row['intrinsic_value']}")
        m3.metric("Upside Potential", f"{row['margin_of_safety']}%")
        m4.metric("Growth Est", f"{row['growth_rate']}%")
        
        st.divider()
        
        col_ai, col_news = st.columns([2, 1])
        
        with col_ai:
            if not api_key:
                st.warning("⚠️ Please add your OpenAI API Key to secrets to unlock this feature.")
            else:
                if st.button(f"✨ Generate Deep Analysis for {selected_ticker}", type="primary"):
                    with st.spinner("🤖 Consulting the Oracle..."):
                        # Parse news from JSON string back to list
                        try:
                            news_list = json.loads(row['Articles_JSON'])
                        except:
                            news_list = []
                            
                        # Call Logic Layer
                        report = generate_ai_report(selected_ticker, row.to_dict(), news_list, api_key)
                        st.session_state[f"report_{selected_ticker}"] = report
                
                # Display Report if it exists in session state
                if f"report_{selected_ticker}" in st.session_state:
                    st.markdown(st.session_state[f"report_{selected_ticker}"])

        with col_news:
            st.markdown("#### 📰 Recent News")
            try:
                news_items = json.loads(row['Articles_JSON'])
                if not news_items:
                    st.write("No news found.")
                for n in news_items[:5]:
                    color = "🟢" if n['label'] == "Bullish" else "🔴" if n['label'] == "Bearish" else "⚪"
                    st.markdown(f"{color} [{n['headline']}]({n['link']})")
                    st.caption(f"Score: {n['score']} | {n['reason']}")
                    st.divider()
            except:
                st.write("News data unavailable.")

# --- TAB 3: DIAGNOSTICS ---
with tab3:
    st.write("Raw Data View")
    st.dataframe(df)
