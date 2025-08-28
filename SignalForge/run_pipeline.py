import json, sys
from tools.get_ticker_data import get_ticker_data
from tools.analyze_technical_indicators import analyze_technical_indicators
from tools.analyze_fundamentals import analyze_fundamentals
from tools.get_options_data import get_options_data
from tools.analyze_options_data import analyze_options_data
from tools.analyze_smart_money import analyze_smart_money
from tools.summarize_insights import summarize_insights

def run_signal_pipeline(ticker: str):
    td = get_ticker_data(ticker)
    tech = analyze_technical_indicators(ticker, td.get("ohlcv", []))
    fnda = analyze_fundamentals(ticker)
    opt_raw = get_options_data(ticker)
    opt = analyze_options_data(ticker, opt_raw)
    sm = analyze_smart_money(ticker)
    final = summarize_insights(ticker, tech, fnda, opt, sm)
    return {
        "ticker": ticker,
        "final": final,
        "technical": tech,
        "fundamentals": fnda,
        "options": opt,
        "smart_money": sm
    }

if __name__ == "__main__":
    t = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    print(json.dumps(run_signal_pipeline(t), indent=2, default=str))
