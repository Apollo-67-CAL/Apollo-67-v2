import os
import time
import logging
from dataclasses import dataclass
from typing import Any, Optional

import requests

from app.providers.twelvedata import ProviderError  # reuse your existing error type

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FinnhubQuote:
    # Finnhub returns:
    # c = current
    # d = change
    # dp = percent change
    # h = high
    # l = low
    # o = open
    # pc = previous close
    # t = timestamp (unix seconds)
    symbol: str
    c: float
    t: int
    raw: dict[str, Any]


@dataclass(frozen=True)
class FinnhubBar:
    # Finnhub candle returns arrays keyed by:
    # c,h,l,o,t,v and s="ok"
    ts_event: str  # ISO string (we’ll convert from unix seconds)
    open: float
    high: float
    low: float
    close: float
    volume: float
    raw: dict[str, Any]


class FinnhubClient:
    def __init__(self, api_key: Optional[str] = None, base_url: str = "https://finnhub.io/api/v1"):
        self.api_key = (api_key or os.getenv("FINNHUB_API_KEY", "")).strip()
        self.base_url = base_url.rstrip("/")
        if not self.api_key:
            raise ProviderError("FINNHUB_API_KEY is missing")

        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        params = dict(params)
        params["token"] = self.api_key

        try:
            r = self.session.get(url, params=params, timeout=15)
        except Exception as e:
            raise ProviderError(f"Finnhub request failed: {e}") from e

        if r.status_code >= 400:
            # Finnhub sometimes returns plain text on errors
            try:
                body = r.json()
            except Exception:
                body = {"error": r.text.strip()}

            raise ProviderError(f"Finnhub error {r.status_code}: {body}")

        try:
            return r.json()
        except Exception as e:
            raise ProviderError(f"Finnhub invalid JSON: {e}") from e

    def fetch_quote(self, symbol: str) -> FinnhubQuote:
        symbol = symbol.strip().upper()
        data = self._get("/quote", {"symbol": symbol})

        # If invalid symbol, Finnhub often returns zeros
        c = float(data.get("c") or 0.0)
        t = int(data.get("t") or 0)

        if c <= 0 or t <= 0:
            raise ProviderError(f"Finnhub returned empty quote for {symbol}: {data}")

        return FinnhubQuote(symbol=symbol, c=c, t=t, raw=data)

    def fetch_daily_bars(self, symbol: str, days: int = 60) -> list[FinnhubBar]:
        symbol = symbol.strip().upper()

        # Finnhub candle endpoint needs unix timestamps
        now = int(time.time())
        frm = now - (days * 86400)

        data = self._get(
            "/stock/candle",
            {
                "symbol": symbol,
                "resolution": "D",
                "from": frm,
                "to": now,
            },
        )

        if data.get("s") != "ok":
            raise ProviderError(f"Finnhub candle not ok for {symbol}: {data}")

        t_list = data.get("t") or []
        o_list = data.get("o") or []
        h_list = data.get("h") or []
        l_list = data.get("l") or []
        c_list = data.get("c") or []
        v_list = data.get("v") or []

        bars: list[FinnhubBar] = []
        for i in range(min(len(t_list), len(o_list), len(h_list), len(l_list), len(c_list), len(v_list))):
            ts = int(t_list[i])
            iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))
            bars.append(
                FinnhubBar(
                    ts_event=iso,
                    open=float(o_list[i]),
                    high=float(h_list[i]),
                    low=float(l_list[i]),
                    close=float(c_list[i]),
                    volume=float(v_list[i]),
                    raw={
                        "t": ts,
                        "o": o_list[i],
                        "h": h_list[i],
                        "l": l_list[i],
                        "c": c_list[i],
                        "v": v_list[i],
                        "provider": "finnhub",
                    },
                )
            )

        if not bars:
            raise ProviderError(f"Finnhub returned no bars for {symbol}: {data}")

        return bars