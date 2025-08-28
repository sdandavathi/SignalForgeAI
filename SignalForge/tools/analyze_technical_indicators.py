import os
import sys
import pandas as pd
import numpy as np
from typing import Dict, Any
from ta.trend import MACD, SMAIndicator
from ta.volatility import BollingerBands

# ---- Robust import for schemas.TechnicalsOut ----
try:
    from schemas import TechnicalsOut  # running from tools/ directory
except Exception:
    try:
        from tools.schemas import TechnicalsOut  # running from project root
    except Exception:  # last resort: patch sys.path
        here = os.path.dirname(os.path.abspath(__file__))
        root = os.path.dirname(here)
        if root not in sys.path:
            sys.path.append(root)
        from tools.schemas import TechnicalsOut  # type: ignore

# Optional yfinance fallback fetch
try:
    import yfinance as yf
except Exception:
    yf = None


def _debug(msg: str):
    if os.getenv("SIGNALFORGE_DEBUG"):
        print(f"[analyze_technical_indicators] {msg}")


def _import_get_ticker_data():
    """Try several import paths for get_ticker_data depending on how the script is invoked."""
    try:
        from get_ticker_data import get_ticker_data  # running from tools/
        return get_ticker_data
    except Exception:
        try:
            from tools.get_ticker_data import get_ticker_data  # running from project root
            return get_ticker_data
        except Exception:
            try:
                here = os.path.dirname(os.path.abspath(__file__))
                if here not in sys.path:
                    sys.path.append(here)
                from get_ticker_data import get_ticker_data  # type: ignore
                return get_ticker_data
            except Exception:
                return None


def _yf_fetch_ohlcv(ticker: str):
    if yf is None:
        return []
    try:
        hist = yf.download(ticker, period="6mo", interval="1d", auto_adjust=True, progress=False)
        if hist is None or hist.empty:
            return []
        df = hist.reset_index()
        df.columns = [str(c).lower() for c in df.columns]
        if "adj close" in df.columns:
            df = df.rename(columns={"adj close": "adj_close"})
        if "datetime" in df.columns and "date" not in df.columns:
            df = df.rename(columns={"datetime": "date"})
        # Enforce numerics before building rows
        for c in ["open", "high", "low", "close", "adj_close", "volume"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        # Drop any rows lacking close
        df = df.dropna(subset=["close"]).reset_index(drop=True)
        rows = []
        for r in df.itertuples(index=False):
            rows.append({
                "date": getattr(r, "date", None),
                "open": float(getattr(r, "open", np.nan)) if not pd.isna(getattr(r, "open", np.nan)) else None,
                "high": float(getattr(r, "high", np.nan)) if not pd.isna(getattr(r, "high", np.nan)) else None,
                "low": float(getattr(r, "low", np.nan)) if not pd.isna(getattr(r, "low", np.nan)) else None,
                "close": float(getattr(r, "close", np.nan)) if not pd.isna(getattr(r, "close", np.nan)) else None,
                "adj_close": float(getattr(r, "adj_close", np.nan)) if hasattr(r, "adj_close") and not pd.isna(getattr(r, "adj_close", np.nan)) else None,
                "volume": float(getattr(r, "volume", np.nan)) if not pd.isna(getattr(r, "volume", np.nan)) else None,
            })
        return rows
    except Exception as e:
        _debug(f"yfinance fallback error: {e}")
        return []


def _ensure_close_column(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure df has a numeric 'close' column. Try common fallbacks if it's missing/empty."""
    cols = [c.lower() for c in df.columns]
    df.columns = cols

    # Normalize known variants
    rename_map = {}
    if "adj close" in cols:
        rename_map["adj close"] = "adj_close"
    if "adjclose" in cols:
        rename_map["adjclose"] = "adj_close"
    if "datetime" in cols and "date" not in cols:
        rename_map["datetime"] = "date"
    if rename_map:
        df = df.rename(columns=rename_map)
        cols = df.columns.tolist()

    # If 'close' missing or all NaN, build it from the first non-empty candidate
    def first_nonempty(series_list):
        for s in series_list:
            if s is None:
                continue
            s = pd.to_numeric(s, errors="coerce")
            if s.notna().any():
                return s
        return None

    need_close = ("close" not in cols) or df["close"].isna().all()
    if need_close:
        candidates = [
            df["close"] if "close" in cols else None,
            df["adj_close"] if "adj_close" in cols else None,
            df["closing price"] if "closing price" in cols else None,
            df["price"] if "price" in cols else None,
            df["last"] if "last" in cols else None,
        ]
        s = first_nonempty(candidates)
        if s is not None:
            df["close"] = s

        # If still missing a usable close, attempt to synthesize from open/high/low
        if ("close" not in df.columns) or df["close"].isna().all():
            o = pd.to_numeric(df["open"], errors="coerce") if "open" in df.columns else None
            h = pd.to_numeric(df["high"], errors="coerce") if "high" in df.columns else None
            l = pd.to_numeric(df["low"], errors="coerce") if "low" in df.columns else None
            synth = None
            if o is not None and o.notna().any():
                synth = o
            elif h is not None and h.notna().any():
                synth = h
            elif l is not None and l.notna().any():
                synth = l
            if synth is not None:
                df["close"] = synth

    # Coerce numerics for main columns; create if missing
    for c in ["open", "high", "low", "close", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        else:
            df[c] = pd.Series([np.nan] * len(df))

    # Parse date if present
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Reduce isolated NaNs if we have a series
    if "close" in df.columns:
        df["close"] = df["close"].ffill().bfill()

    return df


def analyze_technical_indicators(ticker: str, ohlcv: list) -> Dict[str, Any]:
    try:
        # If caller passed nothing, try to fetch OHLCV via our tool
        if not ohlcv:
            get_ticker = _import_get_ticker_data()
            if get_ticker is not None:
                try:
                    td = get_ticker(ticker)
                    if isinstance(td, dict):
                        ohlcv = td.get("ohlcv", [])
                except Exception as e:
                    _debug(f"get_ticker_data call failed: {e}")

        # If still nothing, try direct yfinance fetch as last resort
        if not ohlcv:
            ohlcv = _yf_fetch_ohlcv(ticker)

        if not ohlcv:
            return TechnicalsOut(
                ticker=ticker,
                latest={},
                macd_diff=None,
                sma_cross=None,
                bollinger_band_width=None,
                error="No OHLCV"
            ).dict()

        # Build DataFrame and normalize
        df = pd.DataFrame([r if isinstance(r, dict) else r.dict() for r in ohlcv])
        df = _ensure_close_column(df)

        if "close" not in df.columns or df["close"].isna().all():
            _debug(f"No usable close values after normalization. Columns: {list(df.columns)}")
            # Final attempt: direct yfinance 1y/1d fetch and rebuild df
            if yf is not None:
                try:
                    hist3 = yf.download(ticker, period="1y", interval="1d", auto_adjust=True, progress=False)
                    if hist3 is not None and not hist3.empty:
                        d3 = hist3.reset_index()
                        d3.columns = [str(c).lower() for c in d3.columns]
                        if "adj close" in d3.columns:
                            d3 = d3.rename(columns={"adj close": "adj_close"})
                        if "datetime" in d3.columns and "date" not in d3.columns:
                            d3 = d3.rename(columns={"datetime": "date"})
                        df = _ensure_close_column(d3)
                except Exception as e:
                    _debug(f"final yfinance fetch failed: {e}")

        # If still no close, bail
        if "close" not in df.columns or df["close"].isna().all():
            _debug(f"Still no close values. notna count: {int(df['close'].notna().sum()) if 'close' in df.columns else 0}")
            return TechnicalsOut(
                ticker=ticker,
                latest={},
                macd_diff=None,
                sma_cross=None,
                bollinger_band_width=None,
                error="No close prices available"
            ).dict()

        df = df.dropna(subset=["close"]).reset_index(drop=True)

        # If too few rows, try to fetch more history and avoid failing
        if len(df) < 30:
            get_ticker = _import_get_ticker_data()
            if get_ticker is not None:
                try:
                    td_more = None
                    try:
                        td_more = get_ticker(ticker, period="5y", interval="1d")
                    except TypeError:
                        td_more = get_ticker(ticker)
                    if isinstance(td_more, dict):
                        more = td_more.get("ohlcv", [])
                        if more:
                            df_more = pd.DataFrame([r if isinstance(r, dict) else r.dict() for r in more])
                            df = _ensure_close_column(df_more)
                            df = df.dropna(subset=["close"]).reset_index(drop=True)
                except Exception as e:
                    _debug(f"extended get_ticker_data failed: {e}")

            if len(df) < 30 and yf is not None:
                try:
                    hist2 = yf.download(ticker, period="max", interval="1d", auto_adjust=True, progress=False)
                    if hist2 is not None and not hist2.empty:
                        d2 = hist2.reset_index()
                        d2.columns = [str(c).lower() for c in d2.columns]
                        if "adj close" in d2.columns:
                            d2 = d2.rename(columns={"adj close": "adj_close"})
                        if "datetime" in d2.columns and "date" not in d2.columns:
                            d2 = d2.rename(columns={"datetime": "date"})
                        df = _ensure_close_column(d2)
                        df = df.dropna(subset=["close"]).reset_index(drop=True)
                except Exception as e:
                    _debug(f"yfinance MAX fetch failed: {e}")

            if len(df) < 5:
                return TechnicalsOut(
                    ticker=ticker,
                    latest={},
                    macd_diff=None,
                    sma_cross=None,
                    bollinger_band_width=None,
                    error=f"Not enough bars ({len(df)})"
                ).dict()

        # ---- Compute indicators (tolerant to shorter history) ----
        macd_diff = None
        if len(df) >= 26:
            macd = MACD(df["close"])
            macd_diff = float(macd.macd_diff().iloc[-1])
        else:
            _debug("Insufficient bars for MACD (need ~26)")

        sma50 = SMAIndicator(df["close"], window=50).sma_indicator() if len(df)>=50 else pd.Series([np.nan]*len(df))
        sma200 = SMAIndicator(df["close"], window=200).sma_indicator() if len(df)>=200 else pd.Series([np.nan]*len(df))
        bb = BollingerBands(df["close"], window=20, window_dev=2) if len(df)>=20 else None

        latest = df.iloc[-1]
        sma_cross = None
        if len(df)>=200 and not np.isnan(sma50.iloc[-1]) and not np.isnan(sma200.iloc[-1]):
            sma_cross = "golden" if sma50.iloc[-1] > sma200.iloc[-1] else "death"
        band_width = None
        if bb is not None:
            h = bb.bollinger_hband().iloc[-1]
            l = bb.bollinger_lband().iloc[-1]
            if pd.notna(h) and pd.notna(l):
                band_width = float(h - l)

        latest_block = {
            "date": str(latest["date"]) if "date" in latest else None,
            "open": float(latest["open"]) if pd.notna(latest.get("open")) else None,
            "close": float(latest["close"]) if pd.notna(latest.get("close")) else None,
            "volume": float(latest["volume"]) if pd.notna(latest.get("volume")) else None,
        }
        return TechnicalsOut(
            ticker=ticker,
            latest=latest_block,
            macd_diff=macd_diff,
            sma_cross=sma_cross,
            bollinger_band_width=band_width
        ).dict()

    except Exception as e:
        return TechnicalsOut(
            ticker=ticker,
            latest={},
            macd_diff=None,
            sma_cross=None,
            bollinger_band_width=None,
            error=str(e)
        ).dict()


if __name__ == "__main__":
    import json
    t = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    get_ticker = _import_get_ticker_data()
    if get_ticker:
        td = get_ticker(t)
        ohlcv = td.get("ohlcv", []) if isinstance(td, dict) else []
    else:
        ohlcv = _yf_fetch_ohlcv(t)
    print(json.dumps(analyze_technical_indicators(t, ohlcv), indent=2, default=str))
