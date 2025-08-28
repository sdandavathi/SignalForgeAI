import os
import yfinance as yf
import pandas as pd
from typing import Dict, Any

# Support both package and script execution
try:
    from .schemas import TickerDataOut, OHLCVRow
except ImportError:  # running as a script from tools/
    from schemas import TickerDataOut, OHLCVRow


def _debug(msg: str):
    if os.getenv("SIGNALFORGE_DEBUG"):
        print(f"[get_ticker_data] {msg}")


def _normalize_hist(df: pd.DataFrame) -> pd.DataFrame:
    # yfinance may return columns like 'Open', 'Close', etc., sometimes as a MultiIndex
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.reset_index()

    # If yfinance returns MultiIndex columns (e.g., ('Close','AAPL')), flatten to first level
    if isinstance(df.columns, pd.MultiIndex):
        new_cols = []
        for col in df.columns:
            try:
                left = str(col[0]).lower()
            except Exception:
                left = str(col).lower()
            # Normalize a few common variants on the left level
            if left == 'adj close':
                new_cols.append('adj_close')
            elif left in ('date', 'datetime'):
                new_cols.append('date')
            else:
                new_cols.append(left)
        df.columns = new_cols
    else:
        df.columns = [str(c).lower() for c in df.columns]

    # Map common single-level names
    if 'adj close' in df.columns:
        df = df.rename(columns={'adj close': 'adj_close'})
    if 'datetime' in df.columns and 'date' not in df.columns:
        df = df.rename(columns={'datetime': 'date'})

    return df


def get_ticker_data(ticker: str, period: str = "1y", interval: str = "1d") -> Dict[str, Any]:
    """
    Robust OHLCV fetch with multiple fallbacks:
      1) yf.download(auto_adjust=True)
      2) Ticker.history(auto_adjust=True)
      3) retry with shorter period/standard interval (6mo/1d)
    Emits helpful debug logs when SIGNALFORGE_DEBUG is set.
    """
    try:
        hist = None
        # Attempt 1: yf.download
        try:
            _debug(f"Attempting yf.download ticker={ticker} period={period} interval={interval}")
            hist = yf.download(ticker, period=period, interval=interval, auto_adjust=True, progress=False)
            _debug(f"yf.download returned shape={None if hist is None else hist.shape}")
        except Exception as e:
            _debug(f"yf.download error: {e}")

        # Fallback 2: Ticker.history
        if hist is None or hist.empty:
            try:
                _debug(f"Fallback to Ticker.history ticker={ticker} period={period} interval={interval}")
                tk = yf.Ticker(ticker)
                hist = tk.history(period=period, interval=interval, auto_adjust=True)
                _debug(f"Ticker.history returned shape={None if hist is None else hist.shape}")
            except Exception as e:
                _debug(f"Ticker.history error: {e}")

        # Fallback 3: Relax period/interval
        if hist is None or hist.empty:
            try:
                _debug("Second fallback: yf.download 6mo/1d")
                hist = yf.download(ticker, period="6mo", interval="1d", auto_adjust=True, progress=False)
                _debug(f"second fallback shape={None if hist is None else hist.shape}")
            except Exception as e:
                _debug(f"second fallback error: {e}")

        if hist is None or hist.empty:
            return TickerDataOut(ticker=ticker, error="No OHLCV from yfinance (all attempts)").dict()

        df = _normalize_hist(hist)
        # Enforce numeric types and make sure we actually have usable closes
        if not df.empty:
            # Ensure expected columns exist
            if "datetime" in df.columns and "date" not in df.columns:
                df = df.rename(columns={"datetime": "date"})
            # Cast numerics
            for c in ["open", "high", "low", "close", "adj_close", "volume"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
            # If close is missing or empty, derive from adj_close
            if ("close" not in df.columns) or ("close" in df.columns and df["close"].isna().all()):
                if "adj_close" in df.columns and df["adj_close"].notna().any():
                    df["close"] = df["adj_close"]
            # If we still have no 'close' column, bail early with a clear error
            if "close" not in df.columns:
                _debug(f"No 'close' column present after normalization. Columns={list(df.columns)}")
                return TickerDataOut(ticker=ticker, error="No close column after normalization").dict()
            before = len(df)
            df = df.dropna(subset=["close"]).reset_index(drop=True)
            after = len(df)
            _debug(f"numeric cast done; dropped_no_close={before-after}; remaining={len(df)}")
        if df.empty:
            return TickerDataOut(ticker=ticker, error="Downloaded frame empty after normalization").dict()

        rows = []
        skipped = 0
        for r in df.itertuples(index=False):
            try:
                close_val = getattr(r, "close", None)
                if close_val is None or pd.isna(close_val):
                    skipped += 1
                    continue
                open_val = getattr(r, "open", None)
                high_val = getattr(r, "high", None)
                low_val  = getattr(r, "low", None)
                vol_val  = getattr(r, "volume", None)
                adj_val  = getattr(r, "adj_close", None) if hasattr(r, "adj_close") else None

                rows.append(OHLCVRow(
                    date=pd.to_datetime(getattr(r, "date", None) or getattr(r, "index", None)),
                    open=float(open_val) if open_val is not None and not pd.isna(open_val) else float(close_val),
                    high=float(high_val) if high_val is not None and not pd.isna(high_val) else float(close_val),
                    low=float(low_val) if low_val is not None and not pd.isna(low_val) else float(close_val),
                    close=float(close_val),
                    adj_close=float(adj_val) if adj_val is not None and not pd.isna(adj_val) else None,
                    volume=float(vol_val) if vol_val is not None and not pd.isna(vol_val) else 0.0,
                ))
            except Exception:
                skipped += 1
                continue
        _debug(f"Built {len(rows)} OHLCV rows (skipped={skipped})")

        info = {}
        try:
            tk = yf.Ticker(ticker)
            ii = tk.info or {}
            info = {
                "shortName": ii.get("shortName"),
                "sector": ii.get("sector"),
                "industry": ii.get("industry"),
                "marketCap": ii.get("marketCap"),
            }
        except Exception as e:
            _debug(f"info fetch error: {e}")
            info = {}

        if not rows:
            return TickerDataOut(ticker=ticker, error="No parsed OHLCV rows").dict()

        return TickerDataOut(ticker=ticker, ohlcv=rows, info=info).dict()

    except Exception as e:
        return TickerDataOut(ticker=ticker, error=str(e)).dict()


if __name__ == "__main__":
    import json, sys
    # Allow quick ad-hoc testing: SIGNALFORGE_DEBUG=1 python tools/get_ticker_data.py AAPL
    t = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    out = get_ticker_data(t)
    print(json.dumps(out, indent=2, default=str))
