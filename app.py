import streamlit as st
import pandas as pd
import plotly.express as px
import json
import os
from logic import InvestmentEngine, generate_ai_report

st.set_page_config(page_title="Ruca's Investor Page", layout="wide", page_icon="◈")

# Styling
st.markdown("""
<style>
    .stApp { background-color: #09090b; color: #e4e4e7; }
    [data-testid="metric-container"] { background-color: #18181b; border: 1px solid #27272a; padding: 15px; border-radius: 8px; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] { background-color: #000000; border-radius: 4px; color: #a1a1aa; }
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
    st.title("◈ Ruca's Investments")
    if st.button("📡 Run Global Scan", type="primary"):
        with st.spinner("Scanning market data..."):
            pb = st.progress(0, "Initializing...")
            st.session_state["data"] = engine.fetch_market_data(pb)
            pb.empty()
        st.rerun()

    # LEGEND SECTION
    with st.expander("📚 Legend / Definitions"):
        st.markdown("""
        **Oracle Score:** 0-100 score combining Valuation, Momentum, and News. >75 is a Strong Buy.
        **Fair Value (DCF):** The "True" value of the stock based on cash flow.
        **Upside (MoS):** Margin of Safety. How much cheaper is price vs fair value?
        **RSI:** Momentum. <30 is Oversold (Cheap), >70 is Overbought (Expensive).
        """)

# MAIN DASHBOARD
df = st.session_state["data"]
if df.empty:
    st.info("System Idle. Click 'Run Global Scan' in the sidebar.")
    st.stop()

# METRICS
c1, c2, c3, c4 = st.columns(4)
c1.metric("Assets Analyzed", len(df), help="Total stocks in the current scan universe.")
c2.metric("Market Sentiment", f"{df['Sentiment'].mean():.2f}", help="-1.0 (Bearish) to +1.0 (Bullish)")
c3.metric("Undervalued Opportunities", len(df[df['margin_of_safety'] > 15]), help="Stocks with >15% Upside")
c4.metric("High Conviction Buys", len(df[df['Oracle_Score'] > 75]), help="Stocks with Oracle Score > 75")

tab1, tab2, tab3 = st.tabs(["🔥 Hot vs Not", "🧠 Deep Dive (AI)", "🧪 Risk Lab"])

# --- TAB 1: HOT vs NOT (Replaces Terminal) ---
with tab1:
    st.subheader("Actionable Intelligence")
    
    col_hot, col_cold = st.columns(2)
    
    with col_hot:
        st.markdown("### 🔥 Top Opportunities (Buy Candidates)")
        st.caption("High Oracle Score + Undervalued + Positive Sentiment")
        # Filter: Score > 60 OR Upside > 15%
        hot_df = df[(df['Oracle_Score'] > 60) | (df['margin_of_safety'] > 15)].sort_values("Oracle_Score", ascending=False).head(10)
        
        st.dataframe(
            hot_df[['Ticker', 'Price', 'intrinsic_value', 'margin_of_safety', 'Oracle_Score']],
            column_config={
                "Price": st.column_config.NumberColumn(format="$%.2f"),
                "intrinsic_value": st.column_config.NumberColumn("Fair Val", format="$%.2f", help="DCF Intrinsic Value"),
                "margin_of_safety": st.column_config.NumberColumn("Upside", format="%.1f%%", help="Discount to Fair Value"),
                "Oracle_Score": st.column_config.ProgressColumn("Score", min_value=0, max_value=100, help="Overall Rating"),
            },
            hide_index=True, use_container_width=True
        )

    with col_cold:
        st.markdown("### ❄️ Avoid / High Risk")
        st.caption("Overvalued + Negative Sentiment + High Risk")
        # Filter: Score < 40 OR Overvalued by -20%
        cold_df = df[(df['Oracle_Score'] < 40) | (df['margin_of_safety'] < -20)].sort_values("Oracle_Score", ascending=True).head(10)
        
        st.dataframe(
            cold_df[['Ticker', 'Price', 'margin_of_safety', 'risk_level', 'Oracle_Score']],
            column_config={
                "Price": st.column_config.NumberColumn(format="$%.2f"),
                "margin_of_safety": st.column_config.NumberColumn("Overvalued", format="%.1f%%", help="Negative means expensive"),
                "risk_level": st.column_config.TextColumn("Risk", help="Volatility Level"),
                "Oracle_Score": st.column_config.ProgressColumn("Score", min_value=0, max_value=100),
            },
            hide_index=True, use_container_width=True
        )

# --- TAB 2: DEEP DIVE ---
with tab2:
    selected = st.selectbox("Select Asset for Report", df['Ticker'].unique())
    if selected:
        row = df[df['Ticker'] == selected].iloc[0]
        
        # Financials
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Price", f"${row['Price']}")
        m2.metric("Fair Value", f"${row['intrinsic_value']}", help="Calculated via DCF")
        m3.metric("Growth Est", f"{row['growth_rate']}%", help="5yr Revenue/Earnings CAGR")
        m4.metric("Risk Level", row['risk_level'], help="Based on daily volatility")

        st.divider()
        
        c_rep, c_news = st.columns([2, 1])
        
        with c_rep:
            if not OPENAI_API_KEY:
                st.error("🔑 Add OPENAI_API_KEY to secrets to generate reports.")
            else:
                if st.button(f"Generate Analyst Report for {selected}", type="primary"):
                    with st.spinner("Analyzing..."):
                        news_list = json.loads(row['Articles_JSON']) if isinstance(row['Articles_JSON'], str) else []
                        report = generate_ai_report(selected, row.to_dict(), news_list, OPENAI_API_KEY)
                        st.session_state[f"rep_{selected}"] = report
                
                if f"rep_{selected}" in st.session_state:
                    st.markdown(st.session_state[f"rep_{selected}"])

        with c_news:
            st.subheader("Wire Feed")
            try:
                news_items = json.loads(row['Articles_JSON'])
                for n in news_items[:6]:
                    icon = "🟢" if n['label'] == "Bullish" else "🔴" if n['label'] == "Bearish" else "⚪"
                    st.markdown(f"**{icon} [{n['headline']}]({n['link']})**")
                    # SHOW SUMMARY HERE
                    st.caption(f"{n['reason']}") 
                    st.divider()
            except:
                st.write("No news data.")

# --- TAB 3: RISK LAB ---
with tab3:
    st.subheader("Market Visuals")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Valuation vs Momentum**")
        fig = px.scatter(df, x="margin_of_safety", y="RSI", size="Oracle_Score", color="Sector", hover_name="Ticker", template="plotly_dark")
        fig.add_hline(y=30, line_dash="dot", line_color="green")
        fig.add_vline(x=20, line_dash="dot", line_color="green")
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        st.markdown("**Risk Distribution**")
        fig2 = px.bar(df['risk_level'].value_counts(), template="plotly_dark", color_discrete_sequence=['#2563eb'])
        st.plotly_chart(fig2, use_container_width=True)
