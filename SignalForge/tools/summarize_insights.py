import os, json
from typing import Dict, Any
from openai import OpenAI
from .schemas import FinalSignalOut
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

_client = None
def _client_once():
    global _client
    if _client is None:
        _client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    return _client

def _convert(obj):
    if isinstance(obj, dict):
        return {k: _convert(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert(v) for v in obj]
    elif isinstance(obj, pd.Timestamp):
        return str(obj)
    else:
        return obj

def summarize_insights(ticker: str, technical: Dict[str, Any], fundamentals: Dict[str, Any], options_view: Dict[str, Any], smart_money: Dict[str, Any]) -> Dict[str, Any]:
    prompt = {
        "ticker": ticker,
        "guidance": [
            'Return STRICT JSON: {"signal":"Buy|Sell|Hold","confidence":int,"reason":"..."}',
            "Weigh longer-term trend (SMA cross) > MACD momentum > Bollinger context.",
            "Reward healthy fundamentals (EPS growth, reasonable PE, positive FCF yield).",
            "In options, reward candidates that meet POP≥0.65, credit/max loss≥0.33, max loss≤$500.",
            "Boost if recent insider net buys > 0 or institutional increases; consider congress trades if present."
        ],
        "technical": technical,
        "fundamentals": fundamentals,
        "options": options_view,
        "smart_money": smart_money,
    }
    prompt = _convert(prompt)

    client = _client_once()
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.2,
        messages=[
            {"role":"system","content":"You return valid JSON only, no prose."},
            {"role":"user","content": json.dumps(prompt)}
        ]
    )
    txt = resp.choices[0].message.content.strip()

    try:
        payload = json.loads(txt)
        return FinalSignalOut(**payload).dict()
    except Exception:
        return FinalSignalOut(signal="Hold", confidence=60, reason=f"Parse fallback: {txt[:300]}").dict()

if __name__ == "__main__":
    import json, sys
    from .get_ticker_data import get_ticker_data
    from .analyze_technical_indicators import analyze_technical_indicators
    from .analyze_fundamentals import analyze_fundamentals
    from .get_options_data import get_options_data
    from .analyze_options_data import analyze_options_data
    from .analyze_smart_money import analyze_smart_money

    t = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    td = get_ticker_data(t)
    tech = analyze_technical_indicators(t, td["ohlcv"])
    fnda = analyze_fundamentals(t)
    opt_raw = get_options_data(t)
    opt = analyze_options_data(t, opt_raw)
    sm = analyze_smart_money(t)
    print(json.dumps(summarize_insights(t, tech, fnda, opt, sm), indent=2))
