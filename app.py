"""app.py - Apex Markets v7.0"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import json, os, ast

st.set_page_config(page_title="Apex Markets", layout="wide", page_icon="◈")

st.markdown("""<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');
html,body,[class*="css"]{font-family:'DM Sans',sans-serif;}
.stApp{background:#09090b;color:#e4e4e7;}
section[data-testid="stSidebar"]{background:#0f0f12!important;border-right:1px solid #1f1f23!important;}
section[data-testid="stSidebar"] label,section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] span{color:#71717a!important;}
.stButton>button[kind="primary"]{background:#2563eb!important;color:#fff!important;border:none;
  border-radius:8px;font-weight:600;width:100%;padding:10px;font-size:13px;}
.stButton>button:not([kind="primary"]){background:#18181b!important;color:#71717a!important;
  border:1px solid #27272a!important;border-radius:8px;width:100%;padding:9px;font-size:12px;}
.stButton>button:not([kind="primary"]):hover{border-color:#3b82f6!important;color:#3b82f6!important;}
.stTabs [data-baseweb="tab-list"]{background:transparent;border-bottom:1px solid #27272a;gap:0;}
.stTabs [data-baseweb="tab"]{background:transparent;color:#52525b;font-size:12px;font-weight:500;
  padding:12px 22px;border-bottom:2px solid transparent;}
.stTabs [aria-selected="true"]{color:#3b82f6!important;border-bottom-color:#3b82f6!important;background:transparent!important;}
[data-testid="metric-container"]{background:#111113;border:1px solid #1f1f23;border-radius:10px;padding:14px 18px;}
[data-testid="metric-container"] label{color:#52525b!important;font-size:10px!important;text-transform:uppercase;}
[data-testid="metric-container"] [data-testid="stMetricValue"]{color:#f4f4f5!important;font-size:24px!important;font-weight:700!important;}
[data-testid="stDataFrame"]{border-radius:10px;overflow:hidden;border:1px solid #1f1f23;}
.streamlit-expanderHeader{background:#111113!important;border:1px solid #1f1f23!important;border-radius:8px!important;color:#e4e4e7!important;}
.streamlit-expanderContent{background:#0d0d10!important;border:1px solid #1f1f23!important;border-top:none!important;border-radius:0 0 8px 8px!important;}
.art-card{background:#111113;border:1px solid #1f1f23;border-radius:8px;padding:12px 14px;margin-bottom:7px;display:flex;gap:12px;}
.art-bar{width:3px;border-radius:2px;align-self:stretch;}
.art-bull{background:#22c55e;}.art-bear{background:#ef4444;}.art-neut{background:#52525b;}
.badge{display:inline-block;font-size:9px;font-weight:700;padding:2px 7px;border-radius:100px;text-transform:uppercase;}
.badge-bull{background:#14532d;color:#4ade80;}.badge-bear{background:#450a0a;color:#f87171;}.badge-neut{background:#27272a;color:#a1a1aa;}
.chip-wrap{display:flex;flex-wrap:wrap;gap:5px;padding:4px 0;}
.chip{background:#18181b;border:1px solid #27272a;border-radius:5px;padding:2px 9px;font-size:11px;color:#a1a1aa;}
.apex-hdr{padding:2px 0 18px;border-bottom:1px solid #1f1f23;margin-bottom:22px;}
.apex-title{font-size:20px;font-weight:700;color:#f4f4f5;}
.apex-sub{font-size:11px;color:#3f3f46;letter-spacing:.06em;margin-left:10px;}
hr{border-color:#1f1f23!important;}h2,h3{color:#e4e4e7!important;}
</style>""", unsafe_allow_html=True)

def dark_layout(fig, title=""):
    fig.update_layout(title=dict(text=title,font=dict(size=13,color="#a1a1aa"),x=0.0),
        paper_bgcolor="#111113",plot_bgcolor="#0d0d10",
        font=dict(family="DM Sans",color="#71717a",size=11),
        margin=dict(l=10,r=10,t=36,b=10),
        legend=dict(bgcolor="rgba(0,0,0,0)",bordercolor="#27272a",borderwidth=1),
        xaxis=dict(gridcolor="#1a1a1f",zerolinecolor="#27272a",linecolor="#27272a"),
        yaxis=dict(gridcolor="#1a1a1f",zerolinecolor="#27272a",linecolor="#27272a"))
    return fig

def _safe(val, default=0.0):
    try:
        f=float(val); return default if (f!=f or not np.isfinite(f)) else f
    except: return default

def parse_articles(raw):
    if not raw or not isinstance(raw,str): return []
    raw=raw.strip()
    if raw in ("[]","","nan"): return []
    for fn in (json.loads, lambda s:json.loads(s.strip('"').replace('""','"')), ast.literal_eval):
        try:
            r=fn(raw)
            if isinstance(r,list) and r: return r
        except: continue
    return []

def art_html(articles):
    if not articles: return '<p style="color:#52525b;font-size:12px">No news.</p>'
    out=[]
    for a in articles:
        lbl=a.get("label","Neutral"); score=a.get("score",0.0)
        reason=a.get("reason","")[:90]; date=a.get("pub_date","")[:12]
        link=a.get("link",""); headline=a.get("headline","")
        bc={"Bullish":"art-bull","Bearish":"art-bear"}.get(lbl,"art-neut")
        bdc={"Bullish":"badge-bull","Bearish":"badge-bear"}.get(lbl,"badge-neut")
        sc="#22c55e" if score>0.1 else("#ef4444" if score<-0.1 else "#71717a")
        ht=(f'<a href="{link}" target="_blank" style="color:#e4e4e7;text-decoration:none">{headline}</a>'
            if link else headline)
        out.append(f'<div class="art-card"><div class="art-bar {bc}"></div>'
                   f'<div style="flex:1"><div style="font-size:13px;font-weight:500;color:#e4e4e7;margin-bottom:4px">{ht}</div>'
                   f'<div style="font-size:11px;color:#71717a;display:flex;gap:8px;flex-wrap:wrap">'
                   f'<span class="badge {bdc}">{lbl}</span>'
                   f'<span style="color:{sc}">{score:+.2f}</span>'
                   f'{"<span>"+date+"</span>" if date else ""}'
                   f'{"<span style=color:#52525b>"+reason+"</span>" if reason else ""}'
                   f'</div></div></div>')
    return "".join(out)

def chips(tickers):
    return '<div class="chip-wrap">'+"".join(f'<span class="chip">{t}</span>' for t in sorted(tickers))+'</div>'

CACHE="market_cache.csv"
try:
    from logic import InvestmentEngine
    engine=InvestmentEngine(); HAS_ENGINE=True
except Exception: HAS_ENGINE=False

for k,d in [("data",pd.DataFrame()),("loaded",False)]:
    if k not in st.session_state: st.session_state[k]=d

if st.session_state["data"].empty and not st.session_state["loaded"]:
    if os.path.exists(CACHE):
        try:
            df_c=pd.read_csv(CACHE)
            if "Articles_JSON" in df_c.columns:
                df_c["Articles_JSON"]=df_c["Articles_JSON"].fillna("[]").astype(str)
            st.session_state["data"]=df_c
        except Exception as e: st.error(f"Cache error: {e}")
    st.session_state["loaded"]=True

with st.sidebar:
    st.markdown("### Apex Markets")
    st.markdown("---")
    if HAS_ENGINE:
        if st.button("Run Global Scan", type="primary"):
            with st.spinner("Scanning universe..."):
                pb=st.progress(0,"Starting...")
                st.session_state["data"]=engine.fetch_market_data(pb)
                pb.empty()
            st.success(f"Done — {len(st.session_state['data'])} assets")
            st.rerun()
        if st.button("Clear Cache"):
            if os.path.exists(CACHE): os.remove(CACHE)
            st.session_state["data"]=pd.DataFrame(); st.session_state["loaded"]=False
            st.rerun()
    df_raw=st.session_state["data"]
    if not df_raw.empty:
        st.markdown("---"); st.markdown("**Filters**")
        all_sec=sorted(df_raw["Sector"].dropna().unique()) if "Sector" in df_raw.columns else []
        all_risk=sorted(df_raw["risk_level"].dropna().unique()) if "risk_level" in df_raw.columns else []
        sel_sec=st.multiselect("Sector",all_sec,default=all_sec)
        sel_risk=st.multiselect("Risk",all_risk,default=all_risk)
        min_sc=st.slider("Min Oracle Score",0,100,0)
        sel_dcf=(st.multiselect("DCF Method",sorted(df_raw["dcf_method"].dropna().unique()),
                  default=sorted(df_raw["dcf_method"].dropna().unique()))
                 if "dcf_method" in df_raw.columns else [])
    else:
        sel_sec=sel_risk=sel_dcf=[]; min_sc=0
    st.markdown("---"); st.caption("Apex Markets v7.0")
    if not df_raw.empty and "Last_Updated" in df_raw.columns:
        st.caption(f"Last scan: {df_raw['Last_Updated'].iloc[0]}")

df=st.session_state["data"]
if not df.empty:
    fdf=df.copy()
    if sel_sec  and "Sector"     in fdf.columns: fdf=fdf[fdf["Sector"].isin(sel_sec)]
    if sel_risk and "risk_level" in fdf.columns: fdf=fdf[fdf["risk_level"].isin(sel_risk)]
    if sel_dcf  and "dcf_method" in fdf.columns: fdf=fdf[fdf["dcf_method"].isin(sel_dcf)]
    if "Oracle_Score" in fdf.columns: fdf=fdf[fdf["Oracle_Score"]>=min_sc]
    fdf=fdf.reset_index(drop=True)
else: fdf=df

st.markdown('<div class="apex-hdr"><span class="apex-title">Apex Markets</span>'
            '<span class="apex-sub">GLOBAL INVESTMENT INTELLIGENCE</span></div>',
            unsafe_allow_html=True)

if not fdf.empty:
    hc_df=fdf[fdf["Oracle_Score"]>75]    if "Oracle_Score"    in fdf.columns else fdf.iloc[:0]
    uv_df=fdf[fdf["margin_of_safety"]>20] if "margin_of_safety"in fdf.columns else fdf.iloc[:0]
    geo_df=fdf[fdf["Geo_Risk"]==True]     if "Geo_Risk"        in fdf.columns else fdf.iloc[:0]
    dcf_df=fdf[fdf["intrinsic_value"]>0]  if "intrinsic_value" in fdf.columns else fdf.iloc[:0]

    c1,c2,c3,c4,c5=st.columns(5)
    c1.metric("Total Scanned",len(fdf))
    c2.metric("High Conviction",len(hc_df),help="Oracle > 75")
    c3.metric("Undervalued",len(uv_df),help="MoS > 20%")
    c4.metric("Geo-Risk",len(geo_df))
    c5.metric("DCF Available",len(dcf_df))

    if len(dcf_df)==0 and "intrinsic_value" in fdf.columns:
        st.error("All DCF = 0. Clear Cache → Run Global Scan.")

    e1,e2,e3=st.columns(3)
    with e1:
        if len(hc_df):
            with st.expander(f"{len(hc_df)} High Conviction"):
                st.markdown(chips(hc_df["Ticker"].tolist()),unsafe_allow_html=True)
    with e2:
        if len(uv_df):
            with st.expander(f"{len(uv_df)} Undervalued"):
                st.markdown(chips(uv_df["Ticker"].tolist()),unsafe_allow_html=True)
    with e3:
        if len(dcf_df):
            with st.expander(f"{len(dcf_df)} with DCF"):
                st.markdown(chips(dcf_df["Ticker"].tolist()),unsafe_allow_html=True)

    st.markdown("<br>",unsafe_allow_html=True)
    tab1,tab2,tab3,tab4,tab5=st.tabs(["Terminal","Risk Lab","Deep Dive","Market Intel","AI Analyst"])

    with tab1:
        st.markdown("### Investment Terminal")
        sdf=fdf.sort_values("Oracle_Score",ascending=False) if "Oracle_Score" in fdf.columns else fdf
        show=[c for c in ["Ticker","Sector","Price","intrinsic_value","margin_of_safety","RSI",
                           "Oracle_Score","risk_level","dcf_confidence","dcf_method","growth_rate",
                           "PE","ROE","short_interest"] if c in sdf.columns]
        cfg={"Price":st.column_config.NumberColumn("Price",format="$%.2f"),
             "intrinsic_value":st.column_config.NumberColumn("Fair Value",format="$%.2f"),
             "margin_of_safety":st.column_config.NumberColumn("MoS %",format="%.1f%%"),
             "Oracle_Score":st.column_config.ProgressColumn("Oracle",min_value=0,max_value=100,format="%d"),
             "growth_rate":st.column_config.NumberColumn("Growth %",format="%.1f%%"),
             "PE":st.column_config.NumberColumn("P/E"),
             "ROE":st.column_config.NumberColumn("ROE %",format="%.1f%%"),
             "short_interest":st.column_config.NumberColumn("Short %",format="%.1f%%")}
        st.dataframe(sdf[show],column_config=cfg,use_container_width=True,hide_index=True,height=420)
        st.markdown("---"); st.markdown("### Top 10 Picks")
        for _,row in sdf.head(10).iterrows():
            iv=_safe(row.get("intrinsic_value")); mos=_safe(row.get("margin_of_safety"))
            with st.expander(f"**{row['Ticker']}** ({row.get('Sector','?')}) — Oracle {_safe(row.get('Oracle_Score')):.0f} · MoS {mos:.1f}% · {row.get('risk_level','?')} risk"):
                m1,m2,m3,m4,m5=st.columns(5)
                m1.metric("Price",f"${_safe(row['Price']):.2f}")
                m2.metric("Fair Value",f"${iv:.2f}" if iv>0 else "N/A")
                m3.metric("MoS %",f"{mos:.1f}%")
                m4.metric("RSI",f"{_safe(row.get('RSI'),50):.1f}")
                m5.metric("DCF via",row.get("dcf_method","?"))
                verdict=row.get("AI_Verdict","")
                if verdict: st.info(verdict)
                arts=parse_articles(row.get("Articles_JSON","[]"))
                if arts: st.markdown("**News**"); st.markdown(art_html(arts[:3]),unsafe_allow_html=True)

    with tab2:
        st.markdown("### Risk Laboratory")
        if {"daily_volatility","position_size","Oracle_Score","risk_level"}.issubset(fdf.columns):
            st.markdown("#### Position Sizing Matrix")
            pm_df=fdf.copy(); pm_df["bubble"]=pm_df["Oracle_Score"].clip(lower=5)
            fig_ps=px.scatter(pm_df,x="daily_volatility",y="position_size",size="bubble",
                color="risk_level",hover_name="Ticker",
                hover_data={"Oracle_Score":True,"margin_of_safety":True,"Sector":True,"bubble":False},
                color_discrete_map={"Low":"#22c55e","Medium":"#f59e0b","High":"#ef4444"},
                labels={"daily_volatility":"Daily Volatility %","position_size":"Allocation %"})
            fig_ps.add_vline(x=fdf["daily_volatility"].median(),line_dash="dot",line_color="#3f3f46")
            fig_ps.add_hline(y=fdf["position_size"].median(),line_dash="dot",line_color="#3f3f46")
            st.plotly_chart(dark_layout(fig_ps),use_container_width=True)

        if {"RSI","margin_of_safety","Oracle_Score"}.issubset(fdf.columns):
            st.markdown("#### Opportunity Heatmap")
            mos_v=fdf["margin_of_safety"].dropna()
            xlo=max(mos_v.quantile(0.02)*1.1,-300) if len(mos_v) else -10
            xhi=min(mos_v.quantile(0.98)*1.1,500) if len(mos_v) else 10
            if abs(xhi-xlo)<5: xlo,xhi=-10,10
            hm=fdf.copy(); hm["bubble"]=hm["Oracle_Score"].clip(lower=5)
            fig_h=px.scatter(hm,x="margin_of_safety",y="RSI",size="bubble",color="Oracle_Score",
                hover_name="Ticker",hover_data={"Sector":True,"Price":True,"intrinsic_value":True,"bubble":False},
                color_continuous_scale=[(0,"#ef4444"),(0.5,"#f59e0b"),(1,"#22c55e")],range_color=[0,100],
                labels={"margin_of_safety":"Valuation Upside %","RSI":"RSI"})
            fig_h.add_hline(y=70,line_dash="dot",line_color="#ef4444",annotation_text="Overbought",annotation_font=dict(color="#ef4444",size=9))
            fig_h.add_hline(y=30,line_dash="dot",line_color="#22c55e",annotation_text="Oversold",annotation_font=dict(color="#22c55e",size=9))
            fig_h.add_vline(x=0,line_dash="dash",line_color="#52525b")
            fig_h.update_layout(xaxis=dict(range=[xlo,xhi]))
            st.plotly_chart(dark_layout(fig_h),use_container_width=True)

        if "daily_volatility" in fdf.columns:
            st.markdown("#### Volatility Distribution")
            st.caption("Hover a bar to see which tickers are in that bucket.")
            vd=fdf[["Ticker","daily_volatility"]].dropna().copy()
            vmax=vd["daily_volatility"].quantile(0.98)
            nb=max(15,min(40,len(vd)//2))
            eds=np.linspace(0,max(vmax*1.1,10),nb+1)
            vd["bin"]=pd.cut(vd["daily_volatility"],bins=eds,labels=False)
            bd=(vd.groupby("bin",observed=True)
                  .agg(count=("Ticker","count"),tickers=("Ticker",lambda x:", ".join(sorted(x))))
                  .reset_index())
            bd["mid"]=[(eds[int(i)]+eds[int(i)+1])/2 for i in bd["bin"]]
            bd["lbl"]=[f"{eds[int(i)]:.1f}-{eds[int(i)+1]:.1f}%" for i in bd["bin"]]
            bd["col"]=bd["mid"].apply(lambda v:"#22c55e" if v<2 else("#f59e0b" if v<5 else "#ef4444"))
            fig_v=go.Figure()
            for _,br in bd.iterrows():
                fig_v.add_trace(go.Bar(x=[br["mid"]],y=[br["count"]],width=[(eds[1]-eds[0])*0.85],
                    marker_color=br["col"],marker_opacity=0.8,showlegend=False,
                    hovertemplate=f"<b>{br['lbl']}</b><br>Count: {br['count']}<br>Tickers: {br['tickers']}<extra></extra>"))
            fig_v.add_vline(x=2,line_dash="dot",line_color="#22c55e",annotation_text="2%",annotation_font=dict(color="#22c55e",size=9))
            fig_v.add_vline(x=5,line_dash="dot",line_color="#f59e0b",annotation_text="5%",annotation_font=dict(color="#f59e0b",size=9))
            fig_v.update_layout(xaxis_title="Daily Volatility %",yaxis_title="Stocks",xaxis=dict(range=[0,max(vmax*1.15,10)]),bargap=0.04)
            st.plotly_chart(dark_layout(fig_v),use_container_width=True)
            lo=vd[vd["daily_volatility"]<2]["Ticker"].tolist()
            me=vd[(vd["daily_volatility"]>=2)&(vd["daily_volatility"]<5)]["Ticker"].tolist()
            hi=vd[vd["daily_volatility"]>=5]["Ticker"].tolist()
            vc1,vc2,vc3=st.columns(3)
            vc1.metric("Low (<2%)",len(lo)); vc2.metric("Medium (2-5%)",len(me)); vc3.metric("High (>5%)",len(hi))
            for col,lst,lbl in [(vc1,lo,"Low"),(vc2,me,"Medium"),(vc3,hi,"High")]:
                if lst:
                    with col:
                        with st.expander(f"{lbl} ({len(lst)})"):
                            st.markdown(chips(lst),unsafe_allow_html=True)

    with tab3:
        st.markdown("### Deep Dive")
        options=sorted(fdf["Ticker"].tolist())
        sel=st.selectbox("Select asset",options,
            format_func=lambda t:(f"{t}  —  Oracle {int(_safe(fdf.loc[fdf['Ticker']==t,'Oracle_Score'].values[0]))}  MoS {_safe(fdf.loc[fdf['Ticker']==t,'margin_of_safety'].values[0]):.1f}%")
            if len(fdf.loc[fdf['Ticker']==t]) else t)
        row=fdf[fdf["Ticker"]==sel].iloc[0]
        price=_safe(row["Price"]); iv=_safe(row.get("intrinsic_value")); mos=_safe(row.get("margin_of_safety"))
        m1,m2,m3,m4,m5,m6=st.columns(6)
        m1.metric("Price",f"${price:.2f}")
        m2.metric("Fair Value",f"${iv:.2f}" if iv>0 else "N/A")
        m3.metric("MoS %",f"{mos:.1f}%")
        m4.metric("Oracle",f"{_safe(row.get('Oracle_Score')):.0f}")
        m5.metric("RSI",f"{_safe(row.get('RSI'),50):.1f}")
        m6.metric("Risk",str(row.get("risk_level","?")))
        meth=str(row.get("dcf_method",""))
        if iv==0:
            if "skipped" in meth: st.info("DCF not applicable.")
            elif "no_fcf" in meth: st.warning("No cash flow data found.")
            else: st.warning(f"DCF unavailable: {meth}")
        if iv>0:
            st.markdown("#### Valuation Football Field")
            fig_ff=go.Figure()
            for y0,y1,col,name in [(iv*0.5,iv*0.8,"rgba(59,130,246,0.12)","Deep Value"),
                                    (iv*0.8,iv*1.2,"rgba(34,197,94,0.15)","Fair Value"),
                                    (iv*1.2,iv*1.5,"rgba(245,158,11,0.10)","Premium"),
                                    (iv*1.5,iv*2.0,"rgba(239,68,68,0.08)","Overvalued")]:
                fig_ff.add_shape(type="rect",x0=0,x1=1,y0=y0,y1=y1,fillcolor=col,line_width=0)
                fig_ff.add_annotation(x=0.97,y=(y0+y1)/2,text=name,showarrow=False,xanchor="right",font=dict(size=9,color="#71717a"))
            fig_ff.add_hline(y=price,line_dash="dash",line_color="#ef4444",line_width=2,
                annotation_text=f"Price ${price:.2f}",annotation_font=dict(color="#ef4444"))
            fig_ff.add_hline(y=iv,line_color="#3b82f6",line_width=1.5,
                annotation_text=f"Intrinsic ${iv:.2f}",annotation_font=dict(color="#3b82f6"))
            fig_ff.update_layout(height=300,showlegend=False,
                xaxis=dict(showticklabels=False,showgrid=False,zeroline=False),yaxis_title="Price ($)")
            st.plotly_chart(dark_layout(fig_ff),use_container_width=True)
        st.markdown("---"); st.markdown("#### AI Verdict")
        verdict=row.get("AI_Verdict","")
        st.info(verdict if verdict and verdict!="Neutral outlook" else "No strong signals.")
        st.markdown("---"); st.markdown("#### Latest News")
        arts=parse_articles(row.get("Articles_JSON","[]"))
        if arts and arts[0].get("headline","") not in ("No recent news","No recent news available"):
            st.markdown(art_html(arts),unsafe_allow_html=True)
        else: st.warning("No articles stored.")
        st.markdown("---")
        r1,r2,r3,r4=st.columns(4)
        r1.metric("Allocation",f"{_safe(row.get('position_size')):.1f}%")
        r2.metric("Daily Vol",f"{_safe(row.get('daily_volatility')):.2f}%")
        r3.metric("Short Int",f"{_safe(row.get('short_interest')):.1f}%")
        r4.metric("DCF Confidence",str(row.get("dcf_confidence","?")))
        if row.get("Geo_Risk"): st.warning("Geopolitical risk in headlines.")
        st.markdown(f"[Yahoo Finance](https://finance.yahoo.com/quote/{sel})  ·  [TradingView](https://www.tradingview.com/chart/?symbol={sel})")

    with tab4:
        st.markdown("### Market Intelligence")
        if "Sector" in fdf.columns:
            st.markdown("#### Sector Breakdown")
            agg_d={k:"mean" for k in ["Oracle_Score","margin_of_safety","RSI","daily_volatility","PE","ROE"] if k in fdf.columns}
            agg_d["Ticker"]="count"
            sec_agg=(fdf.groupby("Sector").agg(agg_d).round(1)
                       .rename(columns={"Ticker":"Count","Oracle_Score":"Avg Oracle","margin_of_safety":"Avg MoS %","RSI":"Avg RSI","daily_volatility":"Avg Vol %"}))
            st.dataframe(sec_agg,use_container_width=True)
            if "Oracle_Score" in fdf.columns:
                sco=fdf.groupby("Sector")["Oracle_Score"].mean().sort_values(ascending=True)
                fig_sec=go.Figure(go.Bar(y=sco.index.tolist(),x=sco.values.tolist(),orientation="h",marker_color="#3b82f6",marker_opacity=0.8))
                fig_sec.update_layout(xaxis_title="Avg Oracle Score",height=max(250,len(sco)*30))
                st.plotly_chart(dark_layout(fig_sec,"Sector Oracle Scores"),use_container_width=True)
        if {"RSI","margin_of_safety","Oracle_Score","Sector"}.issubset(fdf.columns):
            st.markdown("#### Value vs Momentum")
            mos2=fdf["margin_of_safety"].dropna()
            xl2=max(mos2.quantile(0.02)*1.1,-200) if len(mos2) else -10
            xh2=min(mos2.quantile(0.98)*1.1,400) if len(mos2) else 10
            if abs(xh2-xl2)<5: xl2,xh2=-10,10
            vm=fdf.copy(); vm["bubble"]=vm["Oracle_Score"].clip(lower=5)
            fig_vm=px.scatter(vm,x="margin_of_safety",y="RSI",size="bubble",color="Sector",
                hover_name="Ticker",hover_data={"Price":True,"Oracle_Score":True,"bubble":False},
                labels={"margin_of_safety":"Valuation Upside %","RSI":"RSI"})
            fig_vm.add_hline(y=70,line_dash="dot",line_color="#ef4444",annotation_text="Overbought",annotation_font=dict(color="#ef4444",size=9))
            fig_vm.add_hline(y=30,line_dash="dot",line_color="#22c55e",annotation_text="Oversold",annotation_font=dict(color="#22c55e",size=9))
            fig_vm.add_vline(x=0,line_dash="dash",line_color="#52525b")
            fig_vm.update_layout(xaxis=dict(range=[xl2,xh2]))
            st.plotly_chart(dark_layout(fig_vm),use_container_width=True)
        if "Sentiment" in fdf.columns:
            st.markdown("#### Sentiment")
            avg_s=fdf["Sentiment"].mean()
            sc1,sc2,sc3=st.columns(3)
            sc1.metric("Avg Sentiment",f"{avg_s:.3f}",delta="Bullish" if avg_s>0 else "Bearish")
            if "Geo_Risk" in fdf.columns: sc2.metric("Geo-Risk Stocks",int(fdf["Geo_Risk"].sum()))
            if "Oracle_Score" in fdf.columns: sc3.metric("High Conviction %",f"{len(fdf[fdf['Oracle_Score']>75])/max(len(fdf),1)*100:.1f}%")
            fig_s=px.histogram(fdf,x="Sentiment",nbins=30,color_discrete_sequence=["#3b82f6"])
            fig_s.add_vline(x=0,line_dash="dash",line_color="#52525b")
            st.plotly_chart(dark_layout(fig_s,"Sentiment Distribution"),use_container_width=True)
        if "dcf_method" in fdf.columns:
            st.markdown("#### DCF Coverage")
            dm=fdf["dcf_method"].value_counts().reset_index(); dm.columns=["Method","Count"]
            fig_dm=px.bar(dm,x="Count",y="Method",orientation="h",color_discrete_sequence=["#3b82f6"])
            fig_dm.update_layout(height=max(200,len(dm)*28))
            st.plotly_chart(dark_layout(fig_dm,"DCF Methods"),use_container_width=True)

    with tab5:
        st.markdown("### AI Analyst")
        OPENAI_KEY=st.secrets.get("OPENAI_API_KEY","") if hasattr(st,"secrets") else ""
        sel2=st.selectbox("Asset for AI memo",sorted(fdf["Ticker"].tolist()),key="ai_sel")
        if sel2:
            row2=fdf[fdf["Ticker"]==sel2].iloc[0]
            ca,cb=st.columns([2,1])
            with ca:
                st.markdown("**Pre-computed verdict:**")
                st.info(row2.get("AI_Verdict","No verdict"))
            with cb:
                iv2=_safe(row2.get("intrinsic_value"))
                st.metric("Fair Value",f"${iv2:.2f}" if iv2>0 else "N/A")
                st.metric("MoS %",f"{_safe(row2.get('margin_of_safety')):.1f}%")
                st.metric("DCF via",str(row2.get("dcf_method","?")))
            if OPENAI_KEY:
                if st.button("Generate GPT-4o Memo",type="primary"):
                    from logic import generate_ai_report
                    arts2=parse_articles(row2.get("Articles_JSON","[]"))
                    with st.spinner("Writing memo..."):
                        memo=generate_ai_report(sel2,row2.to_dict(),arts2,OPENAI_KEY)
                    st.markdown("---"); st.markdown(memo)
            else:
                st.info("Add OPENAI_API_KEY to Streamlit secrets for GPT-4o memos.")
            st.markdown("---"); st.markdown("#### News")
            arts2=parse_articles(row2.get("Articles_JSON","[]"))
            if arts2: st.markdown(art_html(arts2),unsafe_allow_html=True)
else:
    st.markdown("""
    <div style="max-width:480px;margin:72px auto;text-align:center">
      <div style="font-size:44px;margin-bottom:14px;color:#3b82f6">◈</div>
      <div style="font-size:20px;font-weight:700;color:#f4f4f5;margin-bottom:8px">Welcome to Apex Markets</div>
      <div style="color:#52525b;font-size:13px;line-height:1.8">
        No market data yet.<br>
        <strong style="color:#3b82f6">Option A:</strong> Click Run Global Scan in the sidebar.<br>
        <strong style="color:#3b82f6">Option B:</strong> Trigger the GitHub Action (daily_scan.yml).
      </div>
    </div>""", unsafe_allow_html=True)
