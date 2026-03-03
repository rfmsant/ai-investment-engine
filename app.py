import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from logic import InvestmentEngine

# Page Configuration
st.set_page_config(
    page_title="Institutional Investment Command Center", 
    layout="wide", 
    page_icon="🏛️"
)

# Professional CSS
st.markdown("""
<style>
    .metric-card {
        background-color: #0e1117;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #333;
        margin: 5px;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding-left: 20px;
        padding-right: 20px;
    }
    .institutional-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #ffffff;
        text-align: center;
        margin-bottom: 20px;
        background: linear-gradient(90deg, #1f77b4, #ff7f0e);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .risk-high { color: #ff4444; font-weight: bold; }
    .risk-medium { color: #ffaa00; font-weight: bold; }
    .risk-low { color: #44ff44; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# Title
st.markdown('<h1 class="institutional-header">🏛️ INSTITUTIONAL INVESTMENT COMMAND CENTER</h1>', unsafe_allow_html=True)
st.markdown("*Powered by Advanced DCF Modeling, LLM Analysis & Risk Management*")

# Initialize Engine
@st.cache_resource
def get_engine():
    return InvestmentEngine()

engine = get_engine()

# Data Loading
if 'data' not in st.session_state:
    df_cache, from_cache = engine.load_data()
    st.session_state.data = df_cache
    if from_cache:
        st.sidebar.success("✅ Data loaded from cache")

# Sidebar Controls
st.sidebar.header("🎛️ Control Panel")

if st.sidebar.button('🔄 Execute Full Market Scan', type="primary"):
    with st.spinner("Executing institutional-grade analysis..."):
        progress_bar = st.progress(0, "Initializing systems...")
        st.session_state.data = engine.fetch_market_data(progress_bar)
        progress_bar.empty()
        st.success("✅ Market scan complete")
        st.rerun()

df = st.session_state.data

# Filters
if not df.empty:
    if 'Sector' in df.columns:
        all_sectors = sorted(df['Sector'].unique())
        selected_sectors = st.sidebar.multiselect(
            "🏭 Sector Filter", 
            all_sectors, 
            default=all_sectors
        )
        filtered_df = df[df['Sector'].isin(selected_sectors)]
    else:
        filtered_df = df
    
    # Risk Filter
    if 'risk_level' in filtered_df.columns:
        available_risks = filtered_df['risk_level'].unique()
        risk_levels = st.sidebar.multiselect(
            "⚡ Risk Level Filter",
            available_risks,
            default=available_risks
        )
        filtered_df = filtered_df[filtered_df['risk_level'].isin(risk_levels)]
    
    # Oracle Score Filter
    min_score = st.sidebar.slider("🎯 Minimum Oracle Score", 0, 100, 50)
    filtered_df = filtered_df[filtered_df['Oracle_Score'] >= min_score]
else:
    filtered_df = df

# Main Dashboard
if not filtered_df.empty:
    # Key Metrics Row
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Opportunities", len(filtered_df))
    with col2:
        high_conviction = len(filtered_df[filtered_df['Oracle_Score'] > 75])
        st.metric("High Conviction", high_conviction)
    with col3:
        undervalued = len(filtered_df[filtered_df['margin_of_safety'] > 20])
        st.metric("Undervalued Assets", undervalued)
    with col4:
        avg_upside = filtered_df[filtered_df['margin_of_safety'] > 0]['margin_of_safety'].mean()
        st.metric("Avg Upside %", f"{avg_upside:.1f}%" if not pd.isna(avg_upside) else "N/A")
    with col5:
        low_risk = len(filtered_df[filtered_df['risk_level'] == 'Low']) if 'risk_level' in filtered_df.columns else 0
        st.metric("Low Risk Assets", low_risk)

    # Tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "🎯 The Terminal",
        "🛡️ Risk Laboratory", 
        "🔬 Deep Dive Analysis",
        "📊 Market Intelligence"
    ])

    # TAB 1: THE TERMINAL
    with tab1:
        st.header("🎯 Institutional Investment Terminal")
        
        # Sort by Oracle Score
        terminal_df = filtered_df.sort_values("Oracle_Score", ascending=False)
        
        # Display configuration
        display_cols = [
            'Ticker', 'Sector', 'Price', 'intrinsic_value', 'margin_of_safety',
            'RSI', 'Oracle_Score', 'risk_level', 'dcf_confidence'
        ]
        
        available_cols = [col for col in display_cols if col in terminal_df.columns]
        
        st.dataframe(
            terminal_df[available_cols].head(20),
            column_config={
                "Price": st.column_config.NumberColumn("Current Price", format="$%.2f"),
                "intrinsic_value": st.column_config.NumberColumn("Fair Value", format="$%.2f"),
                "margin_of_safety": st.column_config.NumberColumn(
                    "Upside %", 
                    help="Percentage upside to intrinsic value"
                ),
                "Oracle_Score": st.column_config.ProgressColumn(
                    "Oracle Score",
                    min_value=0,
                    max_value=100,
                    format="%d"
                ),
                "RSI": st.column_config.NumberColumn("RSI"),
                "risk_level": st.column_config.TextColumn("Risk"),
                "dcf_confidence": st.column_config.TextColumn("DCF Quality")
            },
            use_container_width=True,
            hide_index=True
        )
        
        # Top Picks Summary
        st.subheader("🏆 Top 5 Institutional Picks")
        top5 = terminal_df.head(5)
        
        for _, row in top5.iterrows():
            with st.expander(f"**{row['Ticker']}** - Oracle Score: {row['Oracle_Score']} | Upside: {row['margin_of_safety']:.1f}%"):
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Current Price", f"${row['Price']}")
                    st.metric("Fair Value", f"${row['intrinsic_value']}")
                    st.metric("Risk Level", row.get('risk_level', 'Unknown'))
                with col2:
                    st.write("**AI Analysis:**")
                    st.write(row.get('AI_Verdict', 'Analysis pending...'))

    # TAB 2: RISK LABORATORY
    with tab2:
        st.header("🛡️ Risk Management Laboratory")
        
        # Position Sizing Recommendations
        st.subheader("📊 Position Sizing Matrix")
        
        if 'position_size' in filtered_df.columns and 'daily_volatility' in filtered_df.columns:
            # Create risk-adjusted position sizing chart
            fig_positions = px.scatter(
                filtered_df,
                x='daily_volatility',
                y='position_size',
                size='Oracle_Score',
                color='risk_level',
                hover_name='Ticker',
                title="Risk-Adjusted Position Sizing",
                labels={
                    'daily_volatility': 'Daily Volatility %',
                    'position_size': 'Recommended Position Size %'
                },
                color_discrete_map={'Low': 'green', 'Medium': 'orange', 'High': 'red'}
            )
            st.plotly_chart(fig_positions, use_container_width=True)
        
        # Correlation Analysis
        st.subheader("🔗 Portfolio Correlation Risk")
        
        # Create correlation matrix using available numeric columns
        numeric_cols = ['Price', 'RSI', 'Oracle_Score', 'margin_of_safety']
        available_numeric = [col for col in numeric_cols if col in filtered_df.columns]
        
        if len(available_numeric) > 1 and len(filtered_df) > 5:
            try:
                corr_matrix = filtered_df[available_numeric].corr()
                
                fig_corr = px.imshow(
                    corr_matrix.values,
                    x=corr_matrix.columns,
                    y=corr_matrix.columns,
                    text_auto=True,
                    aspect="auto",
                    title="Asset Correlation Matrix",
                    color_continuous_scale="RdBu_r"
                )
                st.plotly_chart(fig_corr, use_container_width=True)
                
                # Risk Warning
                high_corr_count = ((corr_matrix.abs() > 0.7).sum().sum() - len(corr_matrix)) // 2
                if high_corr_count > 0:
                    st.warning(f"⚠️ **Systemic Risk Alert:** {high_corr_count} high correlation pairs detected")
            except Exception as e:
                st.info("Correlation analysis requires more data points")
        
        # Volatility Distribution
        st.subheader("📈 Volatility Distribution")
        
        if 'daily_volatility' in filtered_df.columns:
            fig_vol = px.histogram(
                filtered_df,
                x='daily_volatility',
                nbins=20,
                title="Portfolio Volatility Distribution",
                labels={'daily_volatility': 'Daily Volatility %'}
            )
            st.plotly_chart(fig_vol, use_container_width=True)

    # TAB 3: DEEP DIVE
    with tab3:
        st.header("🔬 Deep Dive Analysis")
        
        ticker_list = filtered_df['Ticker'].tolist()
        if ticker_list:
            selected_ticker = st.selectbox("Select Asset for Analysis:", ticker_list)
            
            if selected_ticker:
                row = filtered_df[filtered_df['Ticker'] == selected_ticker].iloc[0]
                
                # Key Metrics
                col1, col2, col3, col4, col5 = st.columns(5)
                col1.metric("Current Price", f"${row['Price']}")
                col2.metric("Fair Value", f"${row['intrinsic_value']}")
                col3.metric("Upside", f"{row['margin_of_safety']:.1f}%")
                col4.metric("Oracle Score", f"{row['Oracle_Score']}")
                col5.metric("Risk Level", row.get('risk_level', 'Unknown'))
                
                # Valuation Football Field Chart
                st.subheader("⚽ Valuation Football Field")
                
                current_price = row['Price']
                fair_value = row['intrinsic_value']
                
                if fair_value > 0:
                    fig_football = go.Figure()
                    
                    # Add ranges
                    fig_football.add_shape(
                        type="rect", x0=0, x1=1, y0=fair_value*0.8, y1=fair_value*1.2,
                        fillcolor="lightgreen", opacity=0.3, line_width=0
                    )
                    fig_football.add_shape(
                        type="rect", x0=0, x1=1, y0=fair_value*0.6, y1=fair_value*0.8,
                        fillcolor="lightblue", opacity=0.3, line_width=0
                    )
                    fig_football.add_shape(
                        type="rect", x0=0, x1=1, y0=fair_value*1.2, y1=fair_value*1.4,
                        fillcolor="lightyellow", opacity=0.3, line_width=0
                    )
                    
                    # Add current price line
                    fig_football.add_hline(
                        y=current_price, 
                        line_dash="dash", 
                        line_color="red", 
                        annotation_text=f"Current: ${current_price}"
                    )
                    
                    # Add fair value line
                    fig_football.add_hline(
                        y=fair_value, 
                        line_color="blue", 
                        annotation_text=f"Fair Value: ${fair_value}"
                    )
                    
                    fig_football.update_layout(
                        title=f"{selected_ticker} - Valuation Analysis",
                        xaxis_title="",
                        yaxis_title="Price ($)",
                        showlegend=False,
                        height=400
                    )
                    
                    fig_football.update_xaxis(showticklabels=False)
                    
                    st.plotly_chart(fig_football, use_container_width=True)
                else:
                    st.warning("DCF valuation not available for this asset")
                
                # LLM Analysis
                st.subheader("🤖 AI Investment Thesis")
                if 'LLM_Reasoning' in row and row['LLM_Reasoning']:
                    st.success(f"**AI Analysis:** {row['LLM_Reasoning']}")
                else:
                    st.info("AI analysis pending...")
                
                # Risk Assessment
                st.subheader("⚡ Risk Profile")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.metric("Position Size Rec.", f"{row.get('position_size', 0)}%")
                    st.metric("Daily Volatility", f"{row.get('daily_volatility', 0)}%")
                    
                with col2:
                    if 'short_interest' in row:
                        st.metric("Short Interest", f"{row['short_interest']}%")
                    if 'Geo_Risk' in row and row['Geo_Risk']:
                        st.error("🌍 Geopolitical Risk Detected")
                
                # News Analysis
                st.subheader("📰 Latest Intelligence")
                if 'Headline' in row:
                    st.write(f"*{row['Headline']}*")
                
                # External Link
                st.markdown(f"[📊 View on Yahoo Finance](https://finance.yahoo.com/quote/{selected_ticker})")

    # TAB 4: MARKET INTELLIGENCE
    with tab4:
        st.header("📊 Market Intelligence Dashboard")
        
        # Sector Performance
        st.subheader("🏭 Sector Analysis")
        
        if 'Sector' in filtered_df.columns:
            sector_analysis = filtered_df.groupby('Sector').agg({
                'Oracle_Score': 'mean',
                'margin_of_safety': 'mean',
                'RSI': 'mean',
                'Ticker': 'count'
            }).round(2)
            sector_analysis.columns = ['Avg Oracle Score', 'Avg Upside %', 'Avg RSI', 'Count']
            
            st.dataframe(sector_analysis, use_container_width=True)
            
            # Sector Performance Chart
            fig_sector = px.scatter(
                filtered_df,
                x='RSI',
                y='margin_of_safety',
                color='Sector',
                size='Oracle_Score',
                hover_name='Ticker',
                title="Sector Positioning: Value vs Momentum",
                labels={
                    'RSI': 'Technical Momentum (RSI)',
                    'margin_of_safety': 'Value Opportunity (Upside %)'
                }
            )
            
            # Add quadrant lines
            fig_sector.add_hline(y=0, line_dash="dash", line_color="gray")
            fig_sector.add_vline(x=50, line_dash="dash", line_color="gray")
            
            st.plotly_chart(fig_sector, use_container_width=True)
        
        # Market Sentiment Overview
        st.subheader("🌡️ Market Sentiment")
        
        if 'Sentiment' in filtered_df.columns:
            avg_sentiment = filtered_df['Sentiment'].mean()
            sentiment_color = "green" if avg_sentiment > 0 else "red"
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Market Sentiment", f"{avg_sentiment:.2f}", delta="Bullish" if avg_sentiment > 0 else "Bearish")
            
            if 'Geo_Risk' in filtered_df.columns:
                geo_risk_count = filtered_df['Geo_Risk'].sum()
                col2.metric("Geopolitical Alerts", geo_risk_count)
            
            high_conviction_pct = (len(filtered_df[filtered_df['Oracle_Score'] > 75]) / len(filtered_df)) * 100
            col3.metric("High Conviction %", f"{high_conviction_pct:.1f}%")

else:
    # Empty State
    st.info("👋 **Welcome to the Institutional Investment Command Center**")
    st.write("Click **'Execute Full Market Scan'** in the sidebar to begin analysis.")
    
    st.markdown("""
    ### 🏛️ System Capabilities:
    - **DCF Valuation Engine**: Automated intrinsic value calculations
    - **LLM News Analysis**: AI-powered sentiment and risk detection  
    - **Risk Management**: Volatility-based position sizing
    - **Alternative Data**: Short interest and insider sentiment
    - **Correlation Analysis**: Portfolio systemic risk monitoring
    """)

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("*Institutional Investment Engine v2.0*")
if not df.empty:
    last_update = df['Last_Updated'].iloc[0] if 'Last_Updated' in df.columns else "Unknown"
    st.sidebar.caption(f"Last Update: {last_update}")
