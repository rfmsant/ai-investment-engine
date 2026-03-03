import streamlit as st
import pandas as pd
import plotly.express as px
from logic import InvestmentEngine

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="Rui's Investment Command Center", layout="wide", page_icon="🚀")

# CSS
st.markdown("""
<style>
    .metric-card {background-color: #0e1117; padding: 15px; border-radius: 10px; border: 1px solid #333;}
    .stInfo {background-color: #0e1117;}
</style>
""", unsafe_allow_html=True)

st.title("🚀 Rui's AI Investment Command Center")

engine = InvestmentEngine()

# --- DEFINITIONS ---
TOOLTIPS = {
    "RSI": "Relative Strength Index (0-100).\n• < 30: Oversold (Cheap) \n• > 70: Overbought (Expensive)",
    "Oracle": "Your Proprietary 'Must Invest' Score (0-100).",
}

# --- LOAD DATA ---
if 'data' not in st.session_state:
    df_cache, from_cache = engine.load_data()
    st.session_state.data = df_cache

if st.sidebar.button('🔄 Update Market Data (Live Scan)'):
    bar = st.progress(0, "Initializing AI Scan...")
    st.session_state.data = engine.fetch_market_data(bar)
    bar.empty()
    st.rerun()

df = st.session_state.data

# --- DASHBOARD ---
if not df.empty:
    
    # FILTER
    if 'Sector' in df.columns:
        all_sectors = sorted(df['Sector'].unique())
        selected_sectors = st.sidebar.multiselect("Filter by Sector", all_sectors, default=all_sectors)
        filtered_df = df[df['Sector'].isin(selected_sectors)]
    else:
        filtered_df = df

    # TABS
    tab1, tab2, tab3, tab4 = st.tabs(["🧠 The Oracle (Must Invest)", "🦅 The Matrix (Heatmap)", "🔬 Stock Deep Dive", "🏭 Industry Matrix"])

    # --- TAB 1: THE ORACLE ---
    with tab1:
        st.header("🏆 Top 'Rational Opportunist' Picks")
        
        if 'Oracle_Score' in filtered_df.columns:
            top_picks = filtered_df.sort_values("Oracle_Score", ascending=False).head(10)
            
            st.dataframe(
                top_picks[['Ticker', 'Sector', 'Oracle_Score', 'Price', 'RSI', 'AI_Verdict']],
                column_config={
                    "Oracle_Score": st.column_config.ProgressColumn("Conviction Score", help=TOOLTIPS["Oracle"], min_value=0, max_value=100, format="%d"),
                    "Price": st.column_config.NumberColumn("Price", format="$%.2f"),
                    "RSI": st.column_config.NumberColumn("RSI", help=TOOLTIPS["RSI"]),
                    "AI_Verdict": st.column_config.TextColumn("AI Analysis", width="large")
                },
                use_container_width=True,
                hide_index=True
            )
        else:
            st.warning("Oracle Score missing. Please click 'Update Market Data'.")

    # --- TAB 2: THE MATRIX (IMPROVED HEATMAP) ---
    with tab2:
        st.header("🦅 Market Strategic Matrix")
        
        # Prepare Data (Safety size for bubbles)
        plot_df = filtered_df.copy()
        plot_df['Chart_Size'] = plot_df['ROE'].apply(lambda x: max(x, 5))
        
        # Create Scatter Plot
        fig = px.scatter(
            plot_df,
            x="RSI",
            y="Sentiment",
            size="Chart_Size",
            color="Sector",
            hover_name="Ticker",
            text="Ticker",
            hover_data={"Chart_Size": False, "ROE": True, "Price": True, "Headline": True},
            labels={"RSI": "Price Strength (Cheap <--- 50 ---> Expensive)", "Sentiment": "News (Bad <--- 0 ---> Good)"},
            height=700,
            range_x=[10, 90],
            range_y=[-1, 1],
            title="Strategic Quadrants"
        )
        
        # --- DRAW THE 4 QUADRANTS ---
        
        # 1. GOLDEN OPPORTUNITY (Top Left): Low RSI (Cheap), High Sentiment (Good News)
        fig.add_shape(type="rect", x0=10, x1=50, y0=0, y1=1,
            fillcolor="Green", opacity=0.1, layer="below", line_width=0)
        fig.add_annotation(x=30, y=0.8, text="💎 GOLDEN OPPORTUNITY<br>(Cheap + Good News)", 
            showarrow=False, font=dict(size=14, color="green"))

        # 2. MOMENTUM RUNNERS (Top Right): High RSI (Expensive), High Sentiment (Good News)
        fig.add_shape(type="rect", x0=50, x1=90, y0=0, y1=1,
            fillcolor="Blue", opacity=0.05, layer="below", line_width=0)
        fig.add_annotation(x=70, y=0.8, text="🚀 MOMENTUM RUNNERS<br>(Expensive + Good News)", 
            showarrow=False, font=dict(size=14, color="blue"))

        # 3. VALUE TRAPS (Bottom Left): Low RSI (Cheap), Low Sentiment (Bad News)
        fig.add_shape(type="rect", x0=10, x1=50, y0=-1, y1=0,
            fillcolor="Gray", opacity=0.1, layer="below", line_width=0)
        fig.add_annotation(x=30, y=-0.5, text="🗑️ VALUE TRAPS<br>(Cheap + Bad News)", 
            showarrow=False, font=dict(size=14, color="gray"))

        # 4. SELL RISK (Bottom Right): High RSI (Expensive), Low Sentiment (Bad News)
        fig.add_shape(type="rect", x0=50, x1=90, y0=-1, y1=0,
            fillcolor="Red", opacity=0.1, layer="below", line_width=0)
        fig.add_annotation(x=70, y=-0.5, text="⚠️ SELL RISK<br>(Expensive + Bad News)", 
            showarrow=False, font=dict(size=14, color="red"))

        # Center Line
        fig.add_vline(x=50, line_width=1, line_dash="dash", line_color="black")
        fig.add_hline(y=0, line_width=1, line_dash="dash", line_color="black")

        st.plotly_chart(fig, use_container_width=True)
        
        st.info("**Strategy Guide:** Focus your attention on the **Top Left (Green)** quadrant. These are high-quality companies (Big Bubbles) that are temporarily beaten down but have positive news.")

    # --- TAB 3: DEEP DIVE ---
    with tab3:
        st.header("🔬 Stock Deep Dive")
        
        ticker_list = filtered_df['Ticker'].tolist()
        if ticker_list:
            selected_ticker = st.selectbox("Select a Stock to Analyze:", ticker_list)
            
            if selected_ticker:
                row = df[df['Ticker'] == selected_ticker].iloc[0]
                
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Current Price", f"${row['Price']}")
                c2.metric("RSI (Momentum)", f"{row['RSI']}", delta="Oversold" if row['RSI']<40 else "Neutral")
                c3.metric("AI Sentiment", f"{row['Sentiment']}", delta="Positive" if row['Sentiment']>0 else "Negative")
                c4.metric("Quality (ROE)", f"{row['ROE']}%")
                
                st.divider()
                st.subheader("🤖 AI Analyst Verdict")
                if row['Geo_Risk']:
                    st.error(f"⚠️ **GEOPOLITICAL ALERT**: News mentions War, Tariffs, or Supply Chains.")
                
                st.success(f"**Analysis:** {row['AI_Verdict']}")
                
                st.subheader("📰 Latest News")
                st.write(f"*{row['Headline']}*")
                
                st.markdown(f"👉 [Open Yahoo Finance Page for {selected_ticker}](https://finance.yahoo.com/quote/{selected_ticker})")

    # --- TAB 4: INDUSTRY MATRIX ---
    with tab4:
        st.header("🏭 Industry Performance")
        
        industry_group = filtered_df.groupby("Sector")[['RSI', 'Sentiment', 'ROE', 'PE']].mean(numeric_only=True).reset_index()
        
        if not industry_group.empty:
            cheapest_sector = industry_group.sort_values("RSI").iloc[0]['Sector']
            strongest_sector = industry_group.sort_values("RSI", ascending=False).iloc[0]['Sector']
            
            st.info(f"**🏭 Sector Summary:** Most Oversold = **{cheapest_sector}** | Strongest Momentum = **{strongest_sector}**")
            
            st.dataframe(
                industry_group.style.background_gradient(subset=['RSI'], cmap='RdYlGn_r'),
                use_container_width=True
            )

else:
    st.info("👋 Welcome! Click 'Update Market Data' in the sidebar to run your first scan.")