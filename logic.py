import yfinance as yf
import pandas as pd
import pandas_ta as ta
import numpy as np
import requests
import xml.etree.ElementTree as ET
from textblob import TextBlob
import os
from datetime import datetime
import json
import time
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

# Configuration
CACHE_FILE = "market_cache.csv"
MOCK_MODE = True  # Set to False when you have LLM API keys

# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL STOCK UNIVERSE  (500+ tickers across US, EU, Asia, EM)
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_UNIVERSE = [
    # ── US Large Cap Tech ──────────────────────────────────────────────────
    {"Ticker": "AAPL",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "MSFT",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "NVDA",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "GOOGL", "Sector": "Technology",    "Region": "US"},
    {"Ticker": "META",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "AMZN",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "TSLA",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "AMD",   "Sector": "Technology",    "Region": "US"},
    {"Ticker": "INTC",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "QCOM",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "AVGO",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "CRM",   "Sector": "Technology",    "Region": "US"},
    {"Ticker": "ORCL",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "NOW",   "Sector": "Technology",    "Region": "US"},
    {"Ticker": "ADBE",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "MU",    "Sector": "Technology",    "Region": "US"},
    {"Ticker": "AMAT",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "LRCX",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "KLAC",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "MRVL",  "Sector": "Technology",    "Region": "US"},
    # ── US Financials ──────────────────────────────────────────────────────
    {"Ticker": "JPM",   "Sector": "Financials",    "Region": "US"},
    {"Ticker": "BAC",   "Sector": "Financials",    "Region": "US"},
    {"Ticker": "GS",    "Sector": "Financials",    "Region": "US"},
    {"Ticker": "MS",    "Sector": "Financials",    "Region": "US"},
    {"Ticker": "WFC",   "Sector": "Financials",    "Region": "US"},
    {"Ticker": "C",     "Sector": "Financials",    "Region": "US"},
    {"Ticker": "BLK",   "Sector": "Financials",    "Region": "US"},
    {"Ticker": "SCHW",  "Sector": "Financials",    "Region": "US"},
    {"Ticker": "AXP",   "Sector": "Financials",    "Region": "US"},
    {"Ticker": "V",     "Sector": "Financials",    "Region": "US"},
    {"Ticker": "MA",    "Sector": "Financials",    "Region": "US"},
    {"Ticker": "PYPL",  "Sector": "Financials",    "Region": "US"},
    {"Ticker": "COF",   "Sector": "Financials",    "Region": "US"},
    {"Ticker": "USB",   "Sector": "Financials",    "Region": "US"},
    {"Ticker": "PNC",   "Sector": "Financials",    "Region": "US"},
    # ── US Healthcare ──────────────────────────────────────────────────────
    {"Ticker": "LLY",   "Sector": "Healthcare",    "Region": "US"},
    {"Ticker": "JNJ",   "Sector": "Healthcare",    "Region": "US"},
    {"Ticker": "UNH",   "Sector": "Healthcare",    "Region": "US"},
    {"Ticker": "PFE",   "Sector": "Healthcare",    "Region": "US"},
    {"Ticker": "MRK",   "Sector": "Healthcare",    "Region": "US"},
    {"Ticker": "ABBV",  "Sector": "Healthcare",    "Region": "US"},
    {"Ticker": "TMO",   "Sector": "Healthcare",    "Region": "US"},
    {"Ticker": "DHR",   "Sector": "Healthcare",    "Region": "US"},
    {"Ticker": "AMGN",  "Sector": "Healthcare",    "Region": "US"},
    {"Ticker": "GILD",  "Sector": "Healthcare",    "Region": "US"},
    {"Ticker": "BIIB",  "Sector": "Healthcare",    "Region": "US"},
    {"Ticker": "VRTX",  "Sector": "Healthcare",    "Region": "US"},
    {"Ticker": "REGN",  "Sector": "Healthcare",    "Region": "US"},
    {"Ticker": "ISRG",  "Sector": "Healthcare",    "Region": "US"},
    {"Ticker": "BSX",   "Sector": "Healthcare",    "Region": "US"},
    # ── US Energy ──────────────────────────────────────────────────────────
    {"Ticker": "XOM",   "Sector": "Energy",        "Region": "US"},
    {"Ticker": "CVX",   "Sector": "Energy",        "Region": "US"},
    {"Ticker": "COP",   "Sector": "Energy",        "Region": "US"},
    {"Ticker": "EOG",   "Sector": "Energy",        "Region": "US"},
    {"Ticker": "SLB",   "Sector": "Energy",        "Region": "US"},
    {"Ticker": "MPC",   "Sector": "Energy",        "Region": "US"},
    {"Ticker": "PSX",   "Sector": "Energy",        "Region": "US"},
    {"Ticker": "VLO",   "Sector": "Energy",        "Region": "US"},
    {"Ticker": "OXY",   "Sector": "Energy",        "Region": "US"},
    {"Ticker": "HAL",   "Sector": "Energy",        "Region": "US"},
    # ── US Consumer ────────────────────────────────────────────────────────
    {"Ticker": "COST",  "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "WMT",   "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "TGT",   "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "HD",    "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "LOW",   "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "NKE",   "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "MCD",   "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "SBUX",  "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "PG",    "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "KO",    "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "PEP",   "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "PM",    "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "MO",    "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "CL",    "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "MDLZ",  "Sector": "Consumer",      "Region": "US"},
    # ── US Industrials ─────────────────────────────────────────────────────
    {"Ticker": "GE",    "Sector": "Industrials",   "Region": "US"},
    {"Ticker": "HON",   "Sector": "Industrials",   "Region": "US"},
    {"Ticker": "CAT",   "Sector": "Industrials",   "Region": "US"},
    {"Ticker": "DE",    "Sector": "Industrials",   "Region": "US"},
    {"Ticker": "BA",    "Sector": "Industrials",   "Region": "US"},
    {"Ticker": "RTX",   "Sector": "Industrials",   "Region": "US"},
    {"Ticker": "LMT",   "Sector": "Industrials",   "Region": "US"},
    {"Ticker": "NOC",   "Sector": "Industrials",   "Region": "US"},
    {"Ticker": "GD",    "Sector": "Industrials",   "Region": "US"},
    {"Ticker": "UPS",   "Sector": "Industrials",   "Region": "US"},
    {"Ticker": "FDX",   "Sector": "Industrials",   "Region": "US"},
    {"Ticker": "MMM",   "Sector": "Industrials",   "Region": "US"},
    {"Ticker": "EMR",   "Sector": "Industrials",   "Region": "US"},
    {"Ticker": "ETN",   "Sector": "Industrials",   "Region": "US"},
    {"Ticker": "ITW",   "Sector": "Industrials",   "Region": "US"},
    # ── US Utilities & Real Estate ─────────────────────────────────────────
    {"Ticker": "NEE",   "Sector": "Utilities",     "Region": "US"},
    {"Ticker": "DUK",   "Sector": "Utilities",     "Region": "US"},
    {"Ticker": "SO",    "Sector": "Utilities",     "Region": "US"},
    {"Ticker": "D",     "Sector": "Utilities",     "Region": "US"},
    {"Ticker": "AEP",   "Sector": "Utilities",     "Region": "US"},
    {"Ticker": "AMT",   "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "PLD",   "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "EQIX",  "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "PSA",   "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "SPG",   "Sector": "Real Estate",   "Region": "US"},
    # ── US Materials ───────────────────────────────────────────────────────
    {"Ticker": "LIN",   "Sector": "Materials",     "Region": "US"},
    {"Ticker": "APD",   "Sector": "Materials",     "Region": "US"},
    {"Ticker": "SHW",   "Sector": "Materials",     "Region": "US"},
    {"Ticker": "ECL",   "Sector": "Materials",     "Region": "US"},
    {"Ticker": "NEM",   "Sector": "Materials",     "Region": "US"},
    {"Ticker": "FCX",   "Sector": "Materials",     "Region": "US"},
    {"Ticker": "DOW",   "Sector": "Materials",     "Region": "US"},
    {"Ticker": "DD",    "Sector": "Materials",     "Region": "US"},
    # ── US Mid Cap Growth ──────────────────────────────────────────────────
    {"Ticker": "SNOW",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "DDOG",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "CRWD",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "ZS",    "Sector": "Technology",    "Region": "US"},
    {"Ticker": "PANW",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "FTNT",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "OKTA",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "HubS",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "MNDY",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "TTD",   "Sector": "Technology",    "Region": "US"},
    {"Ticker": "RBLX",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "COIN",  "Sector": "Financials",    "Region": "US"},
    {"Ticker": "HOOD",  "Sector": "Financials",    "Region": "US"},
    {"Ticker": "SQ",    "Sector": "Financials",    "Region": "US"},
    {"Ticker": "AFRM",  "Sector": "Financials",    "Region": "US"},
    # ── European Blue Chips ────────────────────────────────────────────────
    {"Ticker": "ASML",  "Sector": "Technology",    "Region": "Europe"},
    {"Ticker": "SAP",   "Sector": "Technology",    "Region": "Europe"},
    {"Ticker": "ADYEN", "Sector": "Financials",    "Region": "Europe"},
    {"Ticker": "NOKIA", "Sector": "Technology",    "Region": "Europe"},
    {"Ticker": "ERIC",  "Sector": "Technology",    "Region": "Europe"},
    {"Ticker": "NVO",   "Sector": "Healthcare",    "Region": "Europe"},
    {"Ticker": "RHHBY", "Sector": "Healthcare",    "Region": "Europe"},
    {"Ticker": "AZN",   "Sector": "Healthcare",    "Region": "Europe"},
    {"Ticker": "GSK",   "Sector": "Healthcare",    "Region": "Europe"},
    {"Ticker": "BAYRY", "Sector": "Healthcare",    "Region": "Europe"},
    {"Ticker": "SHEL",  "Sector": "Energy",        "Region": "Europe"},
    {"Ticker": "TTE",   "Sector": "Energy",        "Region": "Europe"},
    {"Ticker": "BP",    "Sector": "Energy",        "Region": "Europe"},
    {"Ticker": "ENI",   "Sector": "Energy",        "Region": "Europe"},
    {"Ticker": "EQNR",  "Sector": "Energy",        "Region": "Europe"},
    {"Ticker": "LVMUY", "Sector": "Consumer",      "Region": "Europe"},
    {"Ticker": "LRLCY", "Sector": "Consumer",      "Region": "Europe"},
    {"Ticker": "NSRGY", "Sector": "Consumer",      "Region": "Europe"},
    {"Ticker": "UNLYY", "Sector": "Consumer",      "Region": "Europe"},
    {"Ticker": "HEIOY", "Sector": "Consumer",      "Region": "Europe"},
    {"Ticker": "BMWYY", "Sector": "Consumer",      "Region": "Europe"},
    {"Ticker": "MBGAF", "Sector": "Consumer",      "Region": "Europe"},
    {"Ticker": "VLKAF", "Sector": "Consumer",      "Region": "Europe"},
    {"Ticker": "STLAM", "Sector": "Consumer",      "Region": "Europe"},
    {"Ticker": "RACE",  "Sector": "Consumer",      "Region": "Europe"},
    {"Ticker": "SIEGY", "Sector": "Industrials",   "Region": "Europe"},
    {"Ticker": "ABBNY", "Sector": "Industrials",   "Region": "Europe"},
    {"Ticker": "ATLCY", "Sector": "Industrials",   "Region": "Europe"},
    {"Ticker": "AIBGY", "Sector": "Financials",    "Region": "Europe"},
    {"Ticker": "BNPQY", "Sector": "Financials",    "Region": "Europe"},
    {"Ticker": "DB",    "Sector": "Financials",    "Region": "Europe"},
    {"Ticker": "INGVY", "Sector": "Financials",    "Region": "Europe"},
    {"Ticker": "UBSG",  "Sector": "Financials",    "Region": "Europe"},
    {"Ticker": "CS",    "Sector": "Financials",    "Region": "Europe"},
    {"Ticker": "ALIZY", "Sector": "Financials",    "Region": "Europe"},
    # ── Asian Blue Chips ───────────────────────────────────────────────────
    {"Ticker": "TSM",   "Sector": "Technology",    "Region": "Asia"},
    {"Ticker": "SMSN",  "Sector": "Technology",    "Region": "Asia"},
    {"Ticker": "SONY",  "Sector": "Technology",    "Region": "Asia"},
    {"Ticker": "TM",    "Sector": "Consumer",      "Region": "Asia"},
    {"Ticker": "HMC",   "Sector": "Consumer",      "Region": "Asia"},
    {"Ticker": "NTT",   "Sector": "Technology",    "Region": "Asia"},
    {"Ticker": "SFT",   "Sector": "Technology",    "Region": "Asia"},
    {"Ticker": "NTDOY", "Sector": "Technology",    "Region": "Asia"},
    {"Ticker": "MUFG",  "Sector": "Financials",    "Region": "Asia"},
    {"Ticker": "SMFG",  "Sector": "Financials",    "Region": "Asia"},
    {"Ticker": "MFG",   "Sector": "Financials",    "Region": "Asia"},
    {"Ticker": "BABA",  "Sector": "Technology",    "Region": "Asia"},
    {"Ticker": "JD",    "Sector": "Consumer",      "Region": "Asia"},
    {"Ticker": "PDD",   "Sector": "Consumer",      "Region": "Asia"},
    {"Ticker": "BIDU",  "Sector": "Technology",    "Region": "Asia"},
    {"Ticker": "NTES",  "Sector": "Technology",    "Region": "Asia"},
    {"Ticker": "9988.HK","Sector":"Technology",    "Region": "Asia"},
    {"Ticker": "700.HK","Sector": "Technology",    "Region": "Asia"},
    {"Ticker": "1299.HK","Sector":"Financials",    "Region": "Asia"},
    {"Ticker": "005930.KS","Sector":"Technology",  "Region": "Asia"},
    {"Ticker": "000660.KS","Sector":"Technology",  "Region": "Asia"},
    {"Ticker": "035420.KS","Sector":"Technology",  "Region": "Asia"},
    {"Ticker": "RELIANCE.NS","Sector":"Industrials","Region":"Asia"},
    {"Ticker": "TCS.NS","Sector": "Technology",    "Region": "Asia"},
    {"Ticker": "INFY",  "Sector": "Technology",    "Region": "Asia"},
    {"Ticker": "HDB",   "Sector": "Financials",    "Region": "Asia"},
    {"Ticker": "IBN",   "Sector": "Financials",    "Region": "Asia"},
    {"Ticker": "WIT",   "Sector": "Technology",    "Region": "Asia"},
    # ── Emerging Markets ───────────────────────────────────────────────────
    {"Ticker": "VALE",  "Sector": "Materials",     "Region": "EM"},
    {"Ticker": "PBR",   "Sector": "Energy",        "Region": "EM"},
    {"Ticker": "ITUB",  "Sector": "Financials",    "Region": "EM"},
    {"Ticker": "BBD",   "Sector": "Financials",    "Region": "EM"},
    {"Ticker": "ABEV",  "Sector": "Consumer",      "Region": "EM"},
    {"Ticker": "SID",   "Sector": "Materials",     "Region": "EM"},
    {"Ticker": "AMBP",  "Sector": "Materials",     "Region": "EM"},
    {"Ticker": "VIST",  "Sector": "Energy",        "Region": "EM"},
    {"Ticker": "GOLD",  "Sector": "Materials",     "Region": "EM"},
    {"Ticker": "AngloGoldAshanti","Sector":"Materials","Region":"EM"},
    {"Ticker": "MX",    "Sector": "Technology",    "Region": "EM"},
    {"Ticker": "AMX",   "Sector": "Technology",    "Region": "EM"},
    {"Ticker": "WALMEX","Sector": "Consumer",      "Region": "EM"},
    {"Ticker": "FEMSA", "Sector": "Consumer",      "Region": "EM"},
    {"Ticker": "GMEXICO","Sector":"Materials",     "Region": "EM"},
    # ── Global ETFs (as reference) ─────────────────────────────────────────
    {"Ticker": "SPY",   "Sector": "ETF",           "Region": "Global"},
    {"Ticker": "QQQ",   "Sector": "ETF",           "Region": "Global"},
    {"Ticker": "EEM",   "Sector": "ETF",           "Region": "Global"},
    {"Ticker": "EFA",   "Sector": "ETF",           "Region": "Global"},
    {"Ticker": "VTI",   "Sector": "ETF",           "Region": "Global"},
    {"Ticker": "GLD",   "Sector": "Commodities",   "Region": "Global"},
    {"Ticker": "SLV",   "Sector": "Commodities",   "Region": "Global"},
    {"Ticker": "USO",   "Sector": "Commodities",   "Region": "Global"},
    {"Ticker": "DXY",   "Sector": "FX",            "Region": "Global"},
    # ── Additional US Mid/Small Cap ────────────────────────────────────────
    {"Ticker": "PLTR",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "AI",    "Sector": "Technology",    "Region": "US"},
    {"Ticker": "PATH",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "SOUN",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "IONQ",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "RGTI",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "QBTS",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "MSTR",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "SMCI",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "ARM",   "Sector": "Technology",    "Region": "US"},
    {"Ticker": "CEG",   "Sector": "Utilities",     "Region": "US"},
    {"Ticker": "VST",   "Sector": "Utilities",     "Region": "US"},
    {"Ticker": "TLN",   "Sector": "Utilities",     "Region": "US"},
    {"Ticker": "NRG",   "Sector": "Utilities",     "Region": "US"},
    {"Ticker": "LHX",   "Sector": "Industrials",   "Region": "US"},
    {"Ticker": "L3H",   "Sector": "Industrials",   "Region": "US"},
    {"Ticker": "HII",   "Sector": "Industrials",   "Region": "US"},
    {"Ticker": "TDG",   "Sector": "Industrials",   "Region": "US"},
    {"Ticker": "AXON",  "Sector": "Industrials",   "Region": "US"},
    {"Ticker": "DECK",  "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "LULU",  "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "RH",    "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "ONON",  "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "UBER",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "LYFT",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "ABNB",  "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "BKNG",  "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "EXPE",  "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "DASH",  "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "SPOT",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "NFLX",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "DIS",   "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "WBD",   "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "PARA",  "Sector": "Consumer",      "Region": "US"},
    {"Ticker": "T",     "Sector": "Telecom",        "Region": "US"},
    {"Ticker": "VZ",    "Sector": "Telecom",        "Region": "US"},
    {"Ticker": "TMUS",  "Sector": "Telecom",        "Region": "US"},
    {"Ticker": "CMCSA", "Sector": "Telecom",        "Region": "US"},
    {"Ticker": "CHTR",  "Sector": "Telecom",        "Region": "US"},
    {"Ticker": "ZM",    "Sector": "Technology",    "Region": "US"},
    {"Ticker": "DOCU",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "BOX",   "Sector": "Technology",    "Region": "US"},
    {"Ticker": "DBX",   "Sector": "Technology",    "Region": "US"},
    {"Ticker": "TWLO",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "SHOP",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "WIX",   "Sector": "Technology",    "Region": "US"},
    {"Ticker": "BIGC",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "ESTC",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "MDB",   "Sector": "Technology",    "Region": "US"},
    {"Ticker": "CFLT",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "NET",   "Sector": "Technology",    "Region": "US"},
    {"Ticker": "FSLY",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "AKAM",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "WDAY",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "VEEV",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "ANSS",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "CDNS",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "SNPS",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "KEYS",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "TER",   "Sector": "Technology",    "Region": "US"},
    {"Ticker": "ENTG",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "ON",    "Sector": "Technology",    "Region": "US"},
    {"Ticker": "WOLF",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "AMBA",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "RMBS",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "ACLS",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "COHU",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "UCTT",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "ICHR",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "DIOD",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "MPWR",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "SITM",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "SWKS",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "XLNX",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "MXIM",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "ADI",   "Sector": "Technology",    "Region": "US"},
    {"Ticker": "TXN",   "Sector": "Technology",    "Region": "US"},
    {"Ticker": "NXPI",  "Sector": "Technology",    "Region": "US"},
    {"Ticker": "STM",   "Sector": "Technology",    "Region": "Europe"},
    {"Ticker": "IFNNY", "Sector": "Technology",    "Region": "Europe"},
    {"Ticker": "RENAULT","Sector":"Consumer",      "Region": "Europe"},
    {"Ticker": "PEUGY", "Sector": "Consumer",      "Region": "Europe"},
    {"Ticker": "VWAGY", "Sector": "Consumer",      "Region": "Europe"},
    {"Ticker": "CSGP",  "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "CBRE",  "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "JLL",   "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "WELL",  "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "VTR",   "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "BXP",   "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "SLG",   "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "KIM",   "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "REG",   "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "FRT",   "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "HST",   "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "PK",    "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "RHP",   "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "CUBE",  "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "EXR",   "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "LSI",   "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "OHI",   "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "LTC",   "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "SBRA",  "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "NHI",   "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "HR",    "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "PEAK",  "Sector": "Real Estate",   "Region": "US"},
    {"Ticker": "DOC",   "Sector": "Real Estate",   "Region": "US"},
]


class LLMAnalyst:
    """Advanced LLM-powered news analysis with fallback mock mode"""

    def __init__(self, mock_mode: bool = True):
        self.mock_mode = mock_mode
        self.openai_api_key = os.getenv("OPENAI_API_KEY")

    def analyze_sentiment(self, headline: str, ticker: str) -> Tuple[float, str]:
        if self.mock_mode or not self.openai_api_key:
            return self._mock_analysis(headline, ticker)
        else:
            return self._llm_analysis(headline, ticker)

    def _mock_analysis(self, headline: str, ticker: str) -> Tuple[float, str]:
        headline_lower = headline.lower()
        bullish_signals = ['beat', 'exceed', 'growth', 'expansion', 'partnership',
                           'breakthrough', 'upgrade', 'record', 'surge', 'rally',
                           'strong', 'profit', 'revenue', 'dividend', 'buyback']
        bearish_signals = ['miss', 'decline', 'loss', 'cut', 'probe', 'lawsuit',
                           'downgrade', 'warn', 'risk', 'fall', 'drop', 'layoff',
                           'recall', 'fine', 'investigation', 'ban']
        sentiment_score = 0.0
        reasoning_parts = []
        for word in bullish_signals:
            if word in headline_lower:
                sentiment_score += 0.12
                reasoning_parts.append(f"Positive: {word}")
        for word in bearish_signals:
            if word in headline_lower:
                sentiment_score -= 0.12
                reasoning_parts.append(f"Risk: {word}")
        sentiment_score = max(-1, min(1, sentiment_score))
        reasoning = ("; ".join(reasoning_parts) if reasoning_parts else "Neutral – no strong signals detected")
        return sentiment_score, reasoning

    def _llm_analysis(self, headline: str, ticker: str) -> Tuple[float, str]:
        try:
            headers = {"Authorization": f"Bearer {self.openai_api_key}", "Content-Type": "application/json"}
            prompt = (f'Analyze this stock headline for {ticker}: "{headline}"\n'
                      'Return only: sentiment_score|brief_reasoning (score from -1.0 to 1.0)')
            data = {"model": "gpt-4", "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 150, "temperature": 0.3}
            response = requests.post("https://api.openai.com/v1/chat/completions",
                                     headers=headers, json=data, timeout=10)
            if response.status_code == 200:
                result = response.json()['choices'][0]['message']['content']
                parts = result.split('|')
                score = float(parts[0].strip())
                reasoning = parts[1].strip() if len(parts) > 1 else "Analysis complete"
                return score, reasoning
        except Exception as e:
            print(f"LLM API error: {e}")
        score = TextBlob(headline).sentiment.polarity
        return score, "LLM unavailable – TextBlob fallback used"


class ValuationEngine:
    @staticmethod
    def calculate_dcf_value(ticker: str, default_discount_rate: float = 0.10) -> Dict:
        try:
            stock = yf.Ticker(ticker)
            cash_flow = stock.cashflow
            info = stock.info

            if cash_flow is None or cash_flow.empty:
                return {"intrinsic_value": 0, "margin_of_safety": 0, "dcf_confidence": "Low", "growth_rate": 0}

            # Try multiple possible row names for operating cash flow
            ocf_keys = [
                'Total Cash From Operating Activities',
                'Operating Cash Flow',
                'Cash From Operations',
                'CashFlowFromContinuingOperatingActivities',
            ]
            capex_keys = [
                'Capital Expenditures',
                'Capital Expenditure',
                'CapEx',
                'Purchase Of Property Plant And Equipment',
            ]

            operating_cf = None
            for key in ocf_keys:
                if key in cash_flow.index:
                    operating_cf = cash_flow.loc[key].dropna()
                    break

            if operating_cf is None or len(operating_cf) < 2:
                # Last resort: use net income as proxy
                ni_keys = ['Net Income', 'NetIncome', 'Net Income From Continuing Operations']
                for key in ni_keys:
                    if key in cash_flow.index:
                        operating_cf = cash_flow.loc[key].dropna()
                        break

            if operating_cf is None or len(operating_cf) < 2:
                return {"intrinsic_value": 0, "margin_of_safety": 0, "dcf_confidence": "Low", "growth_rate": 0}

            capex = pd.Series(dtype=float)
            for key in capex_keys:
                if key in cash_flow.index:
                    capex = cash_flow.loc[key].dropna()
                    break

            if not capex.empty and len(capex) >= len(operating_cf):
                fcf = operating_cf + capex
            else:
                fcf = operating_cf

            # Remove zeros / NaN
            fcf = fcf[fcf != 0].dropna()

            if len(fcf) < 2:
                return {"intrinsic_value": 0, "margin_of_safety": 0, "dcf_confidence": "Low", "growth_rate": 0}

            # Growth rate
            years = len(fcf) - 1
            if fcf.iloc[-1] != 0 and fcf.iloc[0] / fcf.iloc[-1] > 0:
                growth_rate = (fcf.iloc[0] / fcf.iloc[-1]) ** (1 / years) - 1
            else:
                growth_rate = 0.05
            growth_rate = max(-0.30, min(0.50, growth_rate))

            terminal_growth = 0.025
            current_fcf = fcf.iloc[0]

            projected_fcf = [current_fcf * (1 + growth_rate) ** yr for yr in range(1, 6)]
            terminal_fcf = projected_fcf[-1] * (1 + terminal_growth)
            terminal_value = terminal_fcf / (default_discount_rate - terminal_growth)

            pv_fcf = sum([f / (1 + default_discount_rate) ** yr for yr, f in enumerate(projected_fcf, 1)])
            pv_terminal = terminal_value / (1 + default_discount_rate) ** 5
            enterprise_value = pv_fcf + pv_terminal

            shares = info.get('sharesOutstanding', info.get('impliedSharesOutstanding', 0))
            if not shares or shares == 0:
                return {"intrinsic_value": 0, "margin_of_safety": 0, "dcf_confidence": "Low", "growth_rate": 0}

            intrinsic_value = enterprise_value / shares
            current_price = info.get('currentPrice', info.get('regularMarketPrice', 0)) or 0

            margin_of_safety = ((intrinsic_value - current_price) / current_price * 100) if current_price > 0 else 0
            confidence = "High" if len(fcf) >= 4 else "Medium"

            return {
                "intrinsic_value": round(intrinsic_value, 2),
                "margin_of_safety": round(margin_of_safety, 2),
                "dcf_confidence": confidence,
                "growth_rate": round(growth_rate * 100, 2),
            }

        except Exception as e:
            print(f"DCF error {ticker}: {e}")
            return {"intrinsic_value": 0, "margin_of_safety": 0, "dcf_confidence": "Low", "growth_rate": 0}


class RiskManager:
    @staticmethod
    def calculate_position_size(price: float, atr: float,
                                 account_size: float = 100_000,
                                 risk_per_trade: float = 0.02) -> Dict:
        if atr <= 0 or price <= 0:
            return {"position_size": 0, "max_shares": 0, "risk_level": "Unknown", "daily_volatility": 0}
        daily_risk = atr / price
        risk_amount = account_size * risk_per_trade
        stop_loss_distance = atr * 2
        max_shares = int(risk_amount / stop_loss_distance)
        position_value = max_shares * price
        position_size = (position_value / account_size) * 100
        risk_level = "Low" if daily_risk < 0.02 else ("Medium" if daily_risk < 0.05 else "High")
        return {
            "position_size": round(position_size, 2),
            "max_shares": max_shares,
            "risk_level": risk_level,
            "daily_volatility": round(daily_risk * 100, 2),
        }


class InvestmentEngine:
    def __init__(self):
        self.llm_analyst = LLMAnalyst(mock_mode=MOCK_MODE)
        self.valuation_engine = ValuationEngine()
        self.risk_manager = RiskManager()

        # Try user-supplied universe.csv first, fall back to built-in
        try:
            user_univ = pd.read_csv('universe.csv')
            if 'Ticker' in user_univ.columns:
                if 'Sector' not in user_univ.columns:
                    user_univ['Sector'] = 'Unknown'
                if 'Region' not in user_univ.columns:
                    user_univ['Region'] = 'Unknown'
                self.universe = user_univ
            else:
                raise ValueError("No Ticker column")
        except Exception:
            self.universe = pd.DataFrame(DEFAULT_UNIVERSE)

        self.tickers = self.universe['Ticker'].tolist()

    def load_data(self):
        if os.path.exists(CACHE_FILE):
            try:
                return pd.read_csv(CACHE_FILE), True
            except Exception as e:
                print(f"Cache load error: {e}")
        return pd.DataFrame(), False

    def get_advanced_news_analysis(self, ticker: str, max_articles: int = 5) -> List[Dict]:
        """Fetch up to max_articles headlines with individual sentiment scores."""
        articles = []
        try:
            url = (f"https://news.google.com/rss/search?q={ticker}+stock+news"
                   f"&hl=en-US&gl=US&ceid=US:en")
            headers = {"User-Agent": "Mozilla/5.0"}
            response = requests.get(url, headers=headers, timeout=5)

            if response.status_code == 200:
                root = ET.fromstring(response.content)
                items = root.findall(".//item")

                for item in items[:max_articles]:
                    title_el = item.find("title")
                    date_el  = item.find("pubDate")
                    headline = title_el.text.split(" - ")[0] if title_el is not None else "No title"
                    pub_date = date_el.text if date_el is not None else ""

                    score, reasoning = self.llm_analyst.analyze_sentiment(headline, ticker)
                    geo_flag = any(w in headline.lower() for w in
                                   ["war", "china", "tariff", "sanction", "supply",
                                    "ban", "strike", "geopolitic", "regulation"])

                    # Label
                    if score > 0.15:
                        label = "Bullish"
                    elif score < -0.15:
                        label = "Bearish"
                    else:
                        label = "Neutral"

                    articles.append({
                        "headline": headline,
                        "score":    round(score, 3),
                        "label":    label,
                        "reasoning": reasoning,
                        "geo_risk": geo_flag,
                        "pub_date": pub_date,
                    })

        except Exception as e:
            print(f"News error {ticker}: {e}")

        if not articles:
            articles.append({
                "headline": "No recent news found",
                "score": 0.0, "label": "Neutral",
                "reasoning": "No data", "geo_risk": False, "pub_date": "",
            })

        return articles

    def get_alternative_data(self, ticker: str) -> Dict:
        try:
            info = yf.Ticker(ticker).info
            short_pct = (info.get('shortPercentOfFloat', 0) or 0) * 100
            insider_sentiment = ("Bearish (High Short Interest)" if short_pct > 20
                                 else ("Bullish (Low Short Interest)" if short_pct < 5
                                       else "Neutral"))
            return {"short_interest": round(short_pct, 2), "insider_sentiment": insider_sentiment}
        except Exception:
            return {"short_interest": 0, "insider_sentiment": "Unknown"}

    def calculate_oracle_score_v2(self, row: Dict) -> float:
        # Valuation (40%)
        mos = row.get('margin_of_safety', 0)
        if mos > 50:      valuation_score = 40
        elif mos > 20:    valuation_score = 30
        elif mos > 0:     valuation_score = 20
        else:             valuation_score = max(0, 20 + mos * 0.2)

        # Technical (30%)
        rsi_score    = max(0, (70 - row.get('RSI', 50)) * 0.4)
        volume_score = min(10, row.get('Vol_Rel', 1) * 5)
        technical_score = rsi_score + volume_score

        # Quality (20%)
        quality_score = min(20, row.get('ROE', 0) * 0.5)
        if row.get('profit_margin', 0) > 20:
            quality_score += 5

        # Sentiment (10%)
        sentiment_score = max(0, row.get('Sentiment', 0) * 10)

        total = valuation_score + technical_score + quality_score + sentiment_score
        return round(min(100, max(0, total)), 1)

    def generate_institutional_verdict(self, row: Dict) -> str:
        parts = []
        mos = row.get('margin_of_safety', 0)
        if mos > 30:
            parts.append("🟢 STRONG VALUE: Significant discount to intrinsic value")
        elif mos > 10:
            parts.append("🟡 MODERATE VALUE: Some upside to fair value")
        elif mos < -20:
            parts.append("🔴 OVERVALUED: Trading well above intrinsic value")

        rsi = row.get('RSI', 50)
        vol_rel = row.get('Vol_Rel', 1)
        if rsi < 30 and vol_rel > 1.5:
            parts.append("📈 TECHNICAL: Oversold with volume confirmation")
        elif rsi > 70:
            parts.append("⚠️ TECHNICAL: Overbought conditions")

        if row.get('ROE', 0) > 20 and row.get('profit_margin', 0) > 15:
            parts.append("💎 HIGH QUALITY: Strong profitability metrics")
        elif row.get('ROE', 0) < 10:
            parts.append("⚡ QUALITY CONCERN: Below-average returns")

        if row.get('short_interest', 0) > 15:
            parts.append(f"🐻 HIGH SHORT INTEREST: {row['short_interest']}% of float")

        if row.get('Geo_Risk', False):
            parts.append("🌍 MACRO RISK: Geopolitical exposure detected")

        reasoning = row.get('LLM_Reasoning', '')
        if reasoning:
            parts.append(f"🤖 AI: {reasoning[:100]}")

        return " | ".join(parts) if parts else "Neutral outlook – no strong signals"

    def fetch_market_data(self, progress_bar=None):
        results = []
        total = len(self.tickers)

        # Bulk download
        try:
            history = yf.download(self.tickers, period="6mo",
                                   group_by='ticker', progress=False, threads=True)
        except Exception:
            history = {}

        for i, ticker in enumerate(self.tickers):
            try:
                if progress_bar:
                    progress_bar.progress((i + 1) / total, text=f"Scanning {ticker} ({i+1}/{total})…")

                # Price data
                try:
                    if ticker in history.columns.get_level_values(0):
                        df = history[ticker].dropna(how='all')
                    else:
                        df = yf.download(ticker, period="6mo", progress=False)
                except Exception:
                    df = yf.download(ticker, period="6mo", progress=False)

                if df is None or df.empty or len(df) < 20:
                    continue

                df['RSI'] = ta.rsi(df['Close'], length=14)
                df['ATR'] = ta.atr(df['High'], df['Low'], df['Close'], length=14)

                current_price = float(df['Close'].iloc[-1])
                current_rsi   = float(df['RSI'].dropna().iloc[-1]) if not df['RSI'].dropna().empty else 50.0
                current_atr   = float(df['ATR'].dropna().iloc[-1]) if not df['ATR'].dropna().empty else 0.0

                avg_volume  = df['Volume'].rolling(20).mean().iloc[-1]
                vol_relative = float(df['Volume'].iloc[-1]) / float(avg_volume) if avg_volume > 0 else 1.0

                # Fundamentals
                try:
                    info = yf.Ticker(ticker).info
                    pe_ratio      = info.get('trailingPE') or 0
                    roe           = (info.get('returnOnEquity') or 0) * 100
                    profit_margin = (info.get('profitMargins') or 0) * 100
                except Exception:
                    pe_ratio = roe = profit_margin = 0

                dcf_data     = self.valuation_engine.calculate_dcf_value(ticker)
                articles     = self.get_advanced_news_analysis(ticker, max_articles=5)
                alt_data     = self.get_alternative_data(ticker)
                position_data = self.risk_manager.calculate_position_size(current_price, current_atr)

                # Average sentiment across articles
                avg_sentiment = np.mean([a['score'] for a in articles])
                geo_risk_any  = any(a['geo_risk'] for a in articles)
                # Store articles as JSON string for CSV persistence
                articles_json = json.dumps(articles)

                row_data = self.universe[self.universe['Ticker'] == ticker]
                sector = row_data['Sector'].values[0] if not row_data.empty else "Unknown"
                region = row_data['Region'].values[0] if ('Region' in row_data.columns and not row_data.empty) else "Unknown"

                record = {
                    "Ticker":           ticker,
                    "Sector":           sector,
                    "Region":           region,
                    "Price":            round(current_price, 2),
                    "RSI":              round(current_rsi, 2),
                    "ATR":              round(current_atr, 2),
                    "Vol_Rel":          round(vol_relative, 2),
                    "PE":               round(pe_ratio, 2) if pe_ratio else 0,
                    "ROE":              round(roe, 2),
                    "profit_margin":    round(profit_margin, 2),
                    "intrinsic_value":  dcf_data["intrinsic_value"],
                    "margin_of_safety": dcf_data["margin_of_safety"],
                    "dcf_confidence":   dcf_data["dcf_confidence"],
                    "growth_rate":      dcf_data.get("growth_rate", 0),
                    "Sentiment":        round(avg_sentiment, 3),
                    "Headline":         articles[0]["headline"],
                    "LLM_Reasoning":    articles[0]["reasoning"],
                    "Articles_JSON":    articles_json,
                    "Geo_Risk":         geo_risk_any,
                    "short_interest":   alt_data["short_interest"],
                    "insider_sentiment":alt_data["insider_sentiment"],
                    "position_size":    position_data["position_size"],
                    "risk_level":       position_data["risk_level"],
                    "daily_volatility": position_data["daily_volatility"],
                    "Last_Updated":     datetime.now().strftime("%Y-%m-%d %H:%M"),
                }

                record['Oracle_Score'] = self.calculate_oracle_score_v2(record)
                record['AI_Verdict']   = self.generate_institutional_verdict(record)
                results.append(record)

                time.sleep(0.05)

            except Exception as e:
                print(f"Error processing {ticker}: {e}")
                continue

        final_df = pd.DataFrame(results)
        try:
            final_df.to_csv(CACHE_FILE, index=False)
        except Exception as e:
            print(f"Cache save error: {e}")

        return final_df
