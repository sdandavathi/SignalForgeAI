import time
import requests
from typing import Optional

DEFAULT_TIMEOUT = 15

def fetch_json(url: str, params: Optional[dict]=None, headers: Optional[dict]=None, retries: int=2, timeout: int=DEFAULT_TIMEOUT) -> dict:
    last_err = None
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            if r.ok:
                return r.json()
            last_err = RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
        except Exception as e:
            last_err = e
        time.sleep(1.5 * attempt)
    raise last_err
