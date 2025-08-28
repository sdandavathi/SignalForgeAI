import os
import yfinance as yf
from typing import Dict, Any
from tools.utils_http import fetch_json  # Remove 'tools.' prefix
from tools.schemas import FundamentalsOut
from dotenv import load_dotenv
load_dotenv()
def _sf(x):
    try:
        return float(x)
    except:
        return None

def analyze_fundamentals(ticker: str) -> Dict[str, Any]:
    fmp = os.getenv("FMP_API_KEY")
    av  = os.getenv("ALPHAVANTAGE_API_KEY")

    if fmp:
        try:
            base = "https://financialmodelingprep.com/api/v3"
            profile = fetch_json(f"{base}/profile/{ticker}", {"apikey": fmp})
            key_metrics = fetch_json(f"{base}/key-metrics-ttm/{ticker}", {"apikey": fmp})
            income = fetch_json(f"{base}/income-statement/{ticker}", {"period":"annual","limit":1,"apikey": fmp})
            cf = fetch_json(f"{base}/cash-flow-statement/{ticker}", {"period":"annual","limit":1,"apikey": fmp})
            insiders = fetch_json(f"{base}/insider-trading", {"symbol":ticker,"apikey":fmp})

            prof = profile[0] if isinstance(profile, list) and profile else {}
            km = key_metrics[0] if isinstance(key_metrics, list) and key_metrics else {}
            inc = income[0] if isinstance(income, list) and income else {}
            cash = cf[0] if isinstance(cf, list) and cf else {}
            ins = insiders if isinstance(insiders, list) else []

            return FundamentalsOut(
                source="FMP",
                eps=_sf(inc.get("eps") or prof.get("eps")),
                revenue=_sf(inc.get("revenue")),
                net_income=_sf(inc.get("netIncome")),
                ebitda=_sf(inc.get("ebitda")),
                pe_ratio=_sf(prof.get("pe")),
                price_to_sales=_sf(km.get("priceToSalesRatioTTM")),
                gross_margin=_sf(km.get("grossProfitMarginTTM")),
                operating_margin=_sf(km.get("operatingMarginTTM")),
                free_cash_flow_yield=_sf(km.get("freeCashFlowYieldTTM")),
                peg_ratio=_sf(km.get("pegRatioTTM") or prof.get("pegRatio")),
                insider_transactions_count_90d=sum(1 for x in ins if x.get("transactionDate")),
                forward_guidance=str(prof.get("companyOfficers")) if prof.get("companyOfficers") else None
            ).dict()
        except Exception:
            pass

    if av:
        try:
            ov = fetch_json("https://www.alphavantage.co/query", {"function":"OVERVIEW","symbol":ticker,"apikey":av})
            return FundamentalsOut(
                source="AlphaVantage",
                eps=_sf(ov.get("EPS")),
                revenue=_sf(ov.get("RevenueTTM")),
                net_income=_sf(ov.get("NetIncomeTTM")),
                ebitda=_sf(ov.get("EBITDA")),
                pe_ratio=_sf(ov.get("PERatio")),
                price_to_sales=_sf(ov.get("PriceToSalesRatioTTM")),
                gross_margin=_sf(ov.get("GrossProfitTTM")),
                operating_margin=None,
                free_cash_flow_yield=None,
                peg_ratio=_sf(ov.get("PEGRatio")),
                forward_guidance=ov.get("AnalystTargetPrice")
            ).dict()
        except Exception:
            pass

    try:
        tk = yf.Ticker(ticker)
        info = tk.info or {}
        return FundamentalsOut(
            source="Yahoo",
            eps=_sf(info.get("trailingEps")),
            revenue=_sf(info.get("totalRevenue")),
            ebitda=_sf(info.get("ebitda")),
            pe_ratio=_sf(info.get("trailingPE") or info.get("forwardPE")),
            peg_ratio=_sf(info.get("pegRatio")),
            forward_guidance=str(info.get("targetMeanPrice")) if info.get("targetMeanPrice") else None
        ).dict()
    except Exception as e:
        return FundamentalsOut(source="none", error=str(e)).dict()

if __name__ == "__main__":
    import json, sys
    t = sys.argv[1] if len(sys.argv) > 1 else "APLD"
    print(json.dumps(analyze_fundamentals(t), indent=2))
