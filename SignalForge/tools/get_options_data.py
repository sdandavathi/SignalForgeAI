import os
import io
from typing import Dict, Any, List, Optional

import numpy as np
import pandas as pd
import yfinance as yf
import requests
from math import sqrt, erf

# Schemas
try:
    from schemas import OptionsDataOut, OptionContract
except Exception:
    from tools.schemas import OptionsDataOut, OptionContract  # type: ignore


# ---------------- Helpers: normal CDF & Greeks-lite ----------------

def _phi(x):
    return 0.5 * (1.0 + erf(x / sqrt(2.0)))


def _bs_d1(S, K, sigma, T):
    denom = sigma * sqrt(T)
    if denom <= 0:
        return np.nan
    return (np.log(S / K) + 0.5 * sigma * sigma * T) / (denom + 1e-12)


def _delta_call(S, K, sigma, T):
    if sigma is None or sigma <= 0 or T <= 0:
        return None
    d1 = _bs_d1(S, K, sigma, T)
    if np.isnan(d1):
        return None
    return float(_phi(d1))


def _delta_put(S, K, sigma, T):
    if sigma is None or sigma <= 0 or T <= 0:
        return None
    d1 = _bs_d1(S, K, sigma, T)
    if np.isnan(d1):
        return None
    return float(_phi(d1) - 1)


def _pop_itm_call(S, K, sigma, T):
    if sigma is None or sigma <= 0 or T <= 0:
        return None
    d2 = _bs_d1(S, K, sigma, T) - sigma * sqrt(T)
    if np.isnan(d2):
        return None
    return float(_phi(d2))


def _pop_itm_put(S, K, sigma, T):
    if sigma is None or sigma <= 0 or T <= 0:
        return None
    d2 = _bs_d1(S, K, sigma, T) - sigma * sqrt(T)
    if np.isnan(d2):
        return None
    return float(_phi(-d2))


# ---------------- Normalization helpers ----------------

def _flatten_hist(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize yfinance history/download frames and flatten MultiIndex columns."""
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.reset_index()
    # Flatten MultiIndex like ('Close','AAPL') -> 'close'
    if isinstance(df.columns, pd.MultiIndex):
        new_cols = []
        for col in df.columns:
            left = str(col[0]).lower() if isinstance(col, tuple) else str(col).lower()
            if left == 'adj close':
                new_cols.append('adj_close')
            elif left in ('date', 'datetime'):
                new_cols.append('date')
            else:
                new_cols.append(left)
        df.columns = new_cols
    else:
        df.columns = [str(c).lower() for c in df.columns]
    if 'adj close' in df.columns:
        df = df.rename(columns={'adj close': 'adj_close'})
    if 'datetime' in df.columns and 'date' not in df.columns:
        df = df.rename(columns={'datetime': 'date'})
    # Numeric coercion
    for c in ['open','high','low','close','adj_close','volume']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


def _last_price_from_history(tk: yf.Ticker, ticker: str) -> Optional[float]:
    # 1) Ticker.history (5d/1d)
    try:
        hist = tk.history(period="5d", interval="1d", auto_adjust=True, actions=False)
        df = _flatten_hist(hist)
        if not df.empty:
            s = df['close'] if 'close' in df.columns else df.get('adj_close')
            if s is not None and s.notna().any():
                return float(s.dropna().iloc[-1])
    except Exception:
        pass
    # 2) yf.download (5d/1d)
    try:
        d = yf.download(ticker, period="5d", interval="1d", auto_adjust=True, progress=False)
        df = _flatten_hist(d)
        if not df.empty and 'close' in df.columns and df['close'].notna().any():
            return float(df['close'].dropna().iloc[-1])
        if not df.empty and 'adj_close' in df.columns and df['adj_close'].notna().any():
            return float(df['adj_close'].dropna().iloc[-1])
    except Exception:
        pass
    # 3) fast_info / info
    try:
        fi = getattr(tk, 'fast_info', {}) or {}
        for k in ('lastPrice','last_price','regularMarketPrice','previousClose','last_close'):
            v = fi.get(k) if isinstance(fi, dict) else getattr(fi, k, None)
            if v is not None and not pd.isna(v):
                return float(v)
    except Exception:
        pass
    try:
        info = tk.info or {}
        for k in ('regularMarketPrice','currentPrice','previousClose'):
            v = info.get(k)
            if v is not None and not pd.isna(v):
                return float(v)
    except Exception:
        pass
    # 4) Alpha Vantage CSV fallback if key present
    key = os.getenv("ALPHAVANTAGE_API_KEY")
    if key:
        try:
            url = "https://www.alphavantage.co/query"
            params = {
                "function": "TIME_SERIES_DAILY_ADJUSTED",
                "symbol": ticker,
                "outputsize": "compact",
                "datatype": "csv",
                "apikey": key,
            }
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            df = pd.read_csv(io.StringIO(r.text))
            if not df.empty and 'close' in df.columns and df['close'].notna().any():
                return float(pd.to_numeric(df['close'], errors='coerce').dropna().iloc[0])
            if not df.empty and 'adjusted_close' in df.columns and df['adjusted_close'].notna().any():
                return float(pd.to_numeric(df['adjusted_close'], errors='coerce').dropna().iloc[0])
        except Exception:
            pass
    return None


# ---------------- Main: get options data ----------------

def get_options_data(ticker: str, expiries: int = 1) -> Dict[str, Any]:
    try:
        tk = yf.Ticker(ticker)
        last_price = _last_price_from_history(tk, ticker)
        if last_price is None:
            return OptionsDataOut(ticker=ticker, error="No underlying price").model_dump()

        all_exp = tk.options or []
        if not all_exp:
            return OptionsDataOut(ticker=ticker, underlying_price=last_price, options=[]).model_dump()

        out: List[OptionContract] = []
        for expiry in all_exp[: max(expiries, 1)]:
            try:
                chain = tk.option_chain(expiry)
            except Exception:
                continue
            for kind, df in [("call", getattr(chain, 'calls', None)), ("put", getattr(chain, 'puts', None))]:
                if df is None or df.empty:
                    continue
                df = df.copy()
                # Normalize expected fields
                for col in ["impliedVolatility", "lastPrice", "volume", "openInterest", "strike"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                df = df.rename(columns={"impliedVolatility": "iv"})
                # Rank by OI (top 15 per side/expiry)
                try:
                    df = df.sort_values("openInterest", ascending=False).head(15)
                except Exception:
                    df = df.head(15)

                # Time to expiry in years (min 1 trading day), handle tz-naive/aware safely
                try:
                    exp_ts = pd.to_datetime(expiry, utc=True)
                except Exception:
                    exp_ts = pd.to_datetime(expiry, errors="coerce")
                    if exp_ts.tzinfo is None:
                        exp_ts = exp_ts.tz_localize("UTC")
                now_utc = pd.Timestamp.now(tz="UTC")
                days_to_exp = (exp_ts - now_utc).total_seconds() / 86400.0
                if not np.isfinite(days_to_exp):
                    days_to_exp = 1.0
                days_to_exp = max(days_to_exp, 1.0)  # at least 1 day
                T = days_to_exp / 365.0

                for _, r in df.iterrows():
                    strike = float(r.get("strike")) if pd.notna(r.get("strike")) else None
                    iv = float(r.get("iv")) if pd.notna(r.get("iv")) and r.get("iv") > 0 else None
                    last = float(r.get("lastPrice")) if pd.notna(r.get("lastPrice")) else None
                    vol = int(r.get("volume")) if pd.notna(r.get("volume")) else None
                    oi = int(r.get("openInterest")) if pd.notna(r.get("openInterest")) else None

                    if strike is None:
                        continue

                    if iv and iv > 0 and iv < 5.0:
                        if kind == "call":
                            delta = _delta_call(last_price, strike, iv, T)
                            pop = _pop_itm_call(last_price, strike, iv, T)
                        else:
                            delta = _delta_put(last_price, strike, iv, T)
                            pop = _pop_itm_put(last_price, strike, iv, T)
                    else:
                        delta = None
                        pop = None

                    out.append(
                        OptionContract(
                            expiry=str(expiry),
                            type=kind,
                            strike=float(strike),
                            last=last,
                            volume=vol,
                            openInterest=oi,
                            iv=iv,
                            delta=delta,
                            pop_itm=pop,
                        )
                    )

        return OptionsDataOut(ticker=ticker, underlying_price=last_price, options=out).model_dump()
    except Exception as e:
        return OptionsDataOut(ticker=ticker, error=str(e)).model_dump()


if __name__ == "__main__":
    import json, sys
    t = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    print(json.dumps(get_options_data(t), indent=2))
