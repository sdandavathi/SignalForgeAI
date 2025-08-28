from typing import Dict, Any, List
from tools.schemas import OptionsScreenOut, OptionCandidate, OptionsDataOut

def analyze_options_data(ticker: str, options_payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        od = OptionsDataOut(**options_payload)
        under = od.underlying_price
        if under is None:
            return OptionsScreenOut(ticker=ticker, error="No underlying price").dict()

        cands: List[OptionCandidate] = []
        for oc in od.options:
            pop = oc.pop_itm
            last = oc.last or 0.0
            if pop is None:
                continue

            if oc.type == "call":
                distance = max(oc.strike - under, 0.5)
                max_loss = max(distance, 0.5) * 100
            else:
                max_loss = oc.strike * 100

            credit = last * 100
            ratio = (credit / max_loss) if max_loss > 0 else 0
            meets = (pop >= 0.65) and (ratio >= 0.33) and (max_loss <= 500)

            cands.append(OptionCandidate(
                type=oc.type, strike=oc.strike, pop_itm=pop,
                credit=round(credit,2), max_loss=round(max_loss,2),
                credit_to_max_loss=round(ratio,3), meets_rules=meets,
                volume=oc.volume, openInterest=oc.openInterest, expiry=oc.expiry
            ))

        cands.sort(key=lambda x: (x.meets_rules, (x.pop_itm or 0), (x.volume or 0)), reverse=True)
        return OptionsScreenOut(ticker=ticker, candidates=cands[:10]).dict()

    except Exception as e:
        return OptionsScreenOut(ticker=ticker, error=str(e)).dict()

if __name__ == "__main__":
    import json, sys
    from get_options_data import get_options_data
    t = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    raw = get_options_data(t)
    print(json.dumps(analyze_options_data(t, raw), indent=2))
