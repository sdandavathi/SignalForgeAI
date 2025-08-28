import os
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any
from tools.utils_http import fetch_json
from tools.schemas import SmartMoneyOut

def analyze_smart_money(ticker: str) -> Dict[str, Any]:
    out = SmartMoneyOut(ticker=ticker).dict()

    try:
        tk = yf.Ticker(ticker)
        itx = tk.insider_transactions
        net = 0
        if itx is not None and not itx.empty:
            itx = itx.reset_index().rename(columns=str.lower)
            itx["startdate"] = pd.to_datetime(itx["startdate"], errors="coerce", utc=True)
            cutoff = datetime.utcnow() - timedelta(days=90)
            recent = itx[itx["startdate"] >= cutoff]
            for _, r in recent.iterrows():
                shares = int(r.get("shares") or 0)
                trans = str(r.get("transaction", "")).lower()
                if "buy" in trans:
                    net += shares
                elif "sell" in trans:
                    net -= shares
        out["insider_90d_net_buy"] = net
    except Exception:
        pass

    try:
        inst = yf.Ticker(ticker).institutional_holders
        if inst is not None and not inst.empty:
            inst = inst.sort_values("Date Reported", ascending=False).head(10)
            out["institutional_holders"] = inst.to_dict(orient="records")
    except Exception:
        pass

    qk = os.getenv("QUANT_API_KEY")
    if qk:
        try:
            js = fetch_json(f"https://api.quiverquant.com/beta/historical/congresstrading/{ticker}", headers={"Authorization": f"Token {qk}"})
            cutoff = (datetime.utcnow() - timedelta(days=30)).date()
            recent = [x for x in js if x.get("TransactionDate") and x["TransactionDate"] >= str(cutoff)]
            out["congress_trades_30d"] = recent[:20]
        except Exception:
            pass

    return out

if __name__ == "__main__":
    import json, sys
    t = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    print(json.dumps(analyze_smart_money(t), indent=2, default=str))
