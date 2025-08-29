# SignalForge — Public API AI Signals

This project builds AI-driven Buy/Sell/Hold signals from **public data sources** and the **OpenAI SDK**.

## Features
- Technicals: MACD, SMA(50/200) cross, Bollinger width, latest OHLCV/volume
- Fundamentals: EPS/Revenue/PE/PEG/FCF Yield via FMP/AlphaVantage, fallback to Yahoo
- Options: Yahoo chain (IV/OI/Volume) + Black–Scholes delta & POP
- Smart Money: Insider & Institutional snapshots (Yahoo), optional Congressional (QuiverQuant)
- Streamlit dashboard and Docker support

## Quickstart
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # add your OPENAI_API_KEY
python run_pipeline.py AAPL
```

## Conda
```
conda create --name signal_forge python=3.10
conda activate signal_forge
pip install -r requirements.txt
````


## Streamlit
```bash
streamlit run app.py
```
Open http://localhost:8501

## Docker
```bash
docker build -t signalforge .
docker run --env-file .env -p 8501:8501 signalforge
```
