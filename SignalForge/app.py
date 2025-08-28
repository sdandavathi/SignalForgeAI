import os, json
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

from tools.get_ticker_data import get_ticker_data
from tools.analyze_technical_indicators import analyze_technical_indicators
from tools.analyze_fundamentals import analyze_fundamentals
from tools.get_options_data import get_options_data
from tools.analyze_options_data import analyze_options_data
from tools.analyze_smart_money import analyze_smart_money
from tools.summarize_insights import summarize_insights

st.set_page_config(page_title="SignalForge AI", page_icon="📈", layout="wide")
st.title("📈 SignalForge AI — Public API Edition")

ticker = st.text_input("Ticker", "AAPL").strip().upper()
colA, colB = st.columns([1,1])
with colA:
    expiries = st.number_input("Options expiries to fetch", min_value=1, max_value=3, value=1)
with colB:
    run_btn = st.button("Run Analysis")

def plot_price_sma(df: pd.DataFrame):
    df = df.copy()
    df["SMA50"] = df["close"].rolling(50).mean()
    df["SMA200"] = df["close"].rolling(200).mean()
    fig, ax = plt.subplots(figsize=(10,4))
    ax.plot(df["date"], df["close"], label="Close")
    if df["SMA50"].notna().sum()>0:
        ax.plot(df["date"], df["SMA50"], label="SMA50")
    if df["SMA200"].notna().sum()>0:
        ax.plot(df["date"], df["SMA200"], label="SMA200")
    ax.legend()
    ax.set_title("Close with SMA 50/200")
    st.pyplot(fig)

if run_btn:
    with st.spinner("Fetching & analyzing..."):
        td = get_ticker_data(ticker)
        ohlcv = td.get("ohlcv", [])
        tech = analyze_technical_indicators(ticker, ohlcv)
        fnda = analyze_fundamentals(ticker)
        opt_raw = get_options_data(ticker, expiries=expiries)
        opt = analyze_options_data(ticker, opt_raw)
        sm = analyze_smart_money(ticker)
        final = summarize_insights(ticker, tech, fnda, opt, sm)

    st.success("Done.")

    st.header("🧠 Final Signal")
    st.json(final)

    st.header("📊 Price & Moving Averages")
    df = pd.DataFrame(ohlcv)
    if not df.empty:
        plot_price_sma(df)
    else:
        st.info("No OHLCV data available.")

    st.header("🔧 Technicals")
    st.json(tech)

    st.header("🏢 Fundamentals")
    st.json(fnda)

    st.header("🧾 Options Screen (Top Candidates)")
    st.json(opt)

    st.header("💼 Smart Money")
    st.json(sm)

st.caption("Tip: add optional FMP/AlphaVantage/QuiverQuant keys in .env for richer signals.")
