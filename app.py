import streamlit as st
import pandas as pd
import json
import os
from logic import InvestmentEngine, generate_ai_report

st.set_page_config(page_title="Apex Pro", layout="wide", page_icon="◈")

# ===========================================================================
# APPLE-STYLE DESIGN SYSTEM
# ===========================================================================
st.markdown("""
<style>
    /* System Reset */
    .stApp { background-color: #000000; color: #f5f5f7; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }
    
    /* Metrics Cards */
    [data-testid="metric-container"] { 
        background-color: #1c1c1e; 
        border: 1px solid #2c2c2e; 
        border-radius: 12px; 
        padding: 16px;
    }
    
    /* Tables */
    [data-testid="stDataFrame"] { background-color: #1c1c1e; border-radius: 12px; }
    
    /* Buttons */
    .stButton>button { 
        background-color: #0A84FF !important; 
        color: white !important; 
        border-radius: 8px; 
        border: none;
        font-weight: 500;
    }
    
    /* Sidebar */
    section[data-testid="stSidebar"] { background-color: #1c1c1e; border-right: 1px solid #2c2c2e; }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] { gap: 20px; }
    .stTabs [data-baseweb="tab"] { color: #86868b; font-weight: 500; }
    .stTabs [aria-selected="true"] { color: #0A84FF; border-bottom-color: #0A84FF; }
    
    h1, h2, h3 { color: #f5f5f7; font-weight: 600; }
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

# ===========================================================================
# SIDEBAR
# ===========================================================================
with st.sidebar:
    st.markdown("## ◈ Apex Pro")
    
    if st.button("📡 Global Scan", type="primary"):
        with st.spinner("Analyzing 100+ Assets..."):
            pb = st.progress(0, "Initializing...")
            st.session_state["data"] = engine.fetch_market_data(pb)
            pb.empty()
        st.rerun()

    if st.button("🧹 Clear Cache"):
        if os.path.exists("market_cache.csv"):
            os.remove("market_cache.csv")
        st.session_state["data"] = pd.DataFrame()
        st.rerun()

    st.divider()
    with st.expander("Definitions"):
        st.caption("**Oracle Score:** 0-100 rating. >75 is Strong Buy.")
        st.caption("**Deep Value:** Stocks trading >20% below DCF.")
        st.caption("**High Risk:** Daily Volatility > 3.5%.")

# ===========================================================================
# MAIN DASHBOARD
# ===========================================================================
df = st.session_state["data"]
if df.empty:
    st.info("System Idle. Click 'Global Scan' in sidebar.")
    st.stop()

# --- DRILL-DOWN METRICS ---
# The expanders inside columns allow you to see WHICH stocks are in the count
c1, c2, c3, c4 = st.columns(4)

with c1:
    st.metric("Universe", len(df))
    with st.expander("View Sectors"):
        st.dataframe(df['Sector'].value_counts(), use_container_width=True)

with c2:
    hot_picks = df[df['Oracle_Score'] > 75]
    st.metric("Hot Picks", len(hot_picks))
    with st.expander("View List"):
        st.dataframe(hot_picks[['Ticker', 'Oracle_Score']], hide_index=True)

with c3:
    deep_value = df[df['margin_of_safety'] > 20]
    st.metric("Deep Value", len(deep_value))
    with st.expander("View List"):
        st.dataframe(deep_value[['Ticker', 'margin_of_safety']], hide_index=True)

with c4:
    high_risk = df[df['risk_level'] == 'High']
    st.metric("High Risk", len(high_risk))
    with st.expander("View List"):
        st.dataframe(high_risk[['Ticker', 'risk_level']], hide_index=True)

# --- CONTENT TABS ---
tab1, tab2 = st.tabs(["⚡ Market Monitor", "🧠 AI Analyst"])

# TAB 1: MARKET MONITOR (Clean List View)
with tab1:
    col_opps, col_avoid = st.columns(2)
    
    with col_opps:
        st.markdown("### 🟢 Top Opportunities")
        # Logic: Score > 60 OR Upside > 15
        opps = df[(df['Oracle_Score'] > 60) | (df['margin_of_safety'] > 15)].sort_values("Oracle_Score", ascending=False).head(15)
        st.dataframe(
            opps[['Ticker', 'Sector', 'Price', 'margin_of_safety', 'Oracle_Score']],
            column_config={
                "margin_of_safety": st.column_config.NumberColumn("Upside", format="%.1f%%"),
                "Oracle_Score": st.column_config.ProgressColumn("Score", min_value=0, max_value=100)
            },
            hide_index=True, use_container_width=True
        )

    with col_avoid:
        st.markdown("### 🔴 High Risk / Overvalued")
        # Logic: Score < 40 OR Downside < -20
        avoid = df[(df['Oracle_Score'] < 40) | (df['margin_of_safety'] < -20)].sort_values("Oracle_Score", ascending=True).head(15)
        st.dataframe(
            avoid[['Ticker', 'Sector', 'margin_of_safety', 'risk_level', 'Oracle_Score']],
            column_config={
                "margin_of_safety": st.column_config.NumberColumn("Overvalued", format="%.1f%%"),
                "Oracle_Score": st.column_config.ProgressColumn("Score", min_value=0, max_value=100)
            },
            hide_index=True, use_container_width=True
        )

# TAB 2: AI ANALYST
with tab2:
    st.markdown("### Deep Dive Analysis")
    
    col_sel, col_btn = st.columns([3, 1])
    with col_sel:
        selected = st.selectbox("Select Asset", df['Ticker'].unique(), label_visibility="collapsed")
    
    if selected:
        row = df[df['Ticker'] == selected].iloc[0]
        
        # Financial Strip
        f1, f2, f3, f4 = st.columns(4)
        f1.metric("Price", f"${row['Price']}")
        f2.metric("Fair Value", f"${row['intrinsic_value']}")
        f3.metric("Growth Est", f"{row['growth_rate']}%")
        f4.metric("Risk", row['risk_level'])

        st.divider()

        # Split: Report on Left, News on Right
        c_report, c_feed = st.columns([2, 1])
        
        with c_report:
            if OPENAI_API_KEY:
                if st.button(f"Generate Memo for {selected}", type="primary", use_container_width=True):
                    with st.spinner("Compiling Intelligence..."):
                        news_list = json.loads(row['Articles_JSON']) if isinstance(row['Articles_JSON'], str) else []
                        report = generate_ai_report(selected, row.to_dict(), news_list, OPENAI_API_KEY)
                        st.session_state[f"rep_{selected}"] = report
                
                if f"rep_{selected}" in st.session_state:
                    st.markdown(st.session_state[f"rep_{selected}"])
            else:
                st.warning("API Key Required for AI Reports")

        with c_feed:
            st.markdown("#### 📰 Wire Feed")
            try:
                news_items = json.loads(row['Articles_JSON'])
                if not news_items: st.caption("No recent data.")
                for n in news_items[:5]:
                    # Color code based on sentiment
                    color = "#22c55e" if n['label'] == "Bullish" else "#ef4444" if n['label'] == "Bearish" else "#a1a1aa"
                    
                    st.markdown(f"**[{n['headline']}]({n['link']})**")
                    st.caption(f"Sentiment: {n['label']}")
                    # THE SUMMARY IS HERE:
                    st.markdown(f"<div style='border-left: 2px solid {color}; padding-left: 10px; font-size: 13px; color: #d1d1d6;'>{n['reason']}</div>", unsafe_allow_html=True)
                    st.divider()
            except: 
                st.write("Feed Error")
