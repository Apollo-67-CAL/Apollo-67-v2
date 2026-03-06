from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

try:
    import websockets
except Exception:  # pragma: no cover - import failure handled by status
    websockets = None  # type: ignore[assignment]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if isinstance(dt, datetime) else None


def _parse_ts(value: Any) -> datetime:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    if isinstance(value, str) and value.strip():
        txt = value.strip()
        try:
            if txt.isdigit():
                return datetime.fromtimestamp(float(txt), tz=timezone.utc)
            return datetime.fromisoformat(txt.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            return _utc_now()
    return _utc_now()


def _norm_symbol(symbol: Any) -> str:
    return str(symbol or "").strip().upper()


class TwelveDataWSClient:
    def __init__(self) -> None:
        self._api_key = os.getenv("TWELVEDATA_API_KEY", "").strip()
        self._ws_enabled_env = os.getenv("TWELVEDATA_WS_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
        base_url = os.getenv("TWELVEDATA_WS_URL", "wss://ws.twelvedata.com/v1/quotes/price").strip()
        self._ws_url = f"{base_url}?apikey={self._api_key}" if self._api_key else base_url
        self._max_subs = max(1, int(os.getenv("MAX_WS_SUBSCRIPTIONS", os.getenv("TWELVEDATA_WS_MAX_SUBS", "200"))))

        self.enabled = bool(self._ws_enabled_env and self._api_key and websockets is not None)
        self.started = False
        self.connected = False
        self.last_message_at: Optional[datetime] = None
        self.last_error: Optional[str] = None
        self.message_count_total = 0
        self.ws_entitlement = "unknown"

        self._stop_event = asyncio.Event()
        self._runner_task: Optional[asyncio.Task[Any]] = None
        self._heartbeat_task: Optional[asyncio.Task[Any]] = None
        self._ws = None
        self._lock = asyncio.Lock()

        self._sub_order: List[str] = []
        self._sub_set: Set[str] = set()
        self._msg_timestamps: Deque[float] = deque(maxlen=5000)
        self._recent_msgs: Deque[Dict[str, Any]] = deque(maxlen=500)
        self._prices: Dict[str, Dict[str, Any]] = {}

    def _messages_last_60s(self) -> int:
        now = time.monotonic()
        while self._msg_timestamps and (now - self._msg_timestamps[0]) > 60.0:
            self._msg_timestamps.popleft()
        return len(self._msg_timestamps)

    def _status(self) -> Dict[str, Any]:
        return {
            "enabled": bool(self.enabled),
            "started": bool(self.started),
            "connected": bool(self.connected),
            "subscriptions": len(self._sub_set),
            "message_count": int(self.message_count_total),
            "messages_last_60s": int(self._messages_last_60s()),
            "last_message_at": _iso(self.last_message_at),
            "last_error": self.last_error,
            "ws_entitlement": self.ws_entitlement,
        }

    def status(self) -> Dict[str, Any]:
        return self._status()

    def recent(self, limit: int = 50) -> List[Dict[str, Any]]:
        n = max(1, min(int(limit), 500))
        return list(self._recent_msgs)[-n:]

    def get_price(self, symbol: str, max_age_seconds: int = 15) -> Optional[Dict[str, Any]]:
        sym = _norm_symbol(symbol)
        row = self._prices.get(sym)
        if not isinstance(row, dict):
            return None
        ts = row.get("ts")
        if not isinstance(ts, datetime):
            return None
        if (_utc_now() - ts).total_seconds() > max(1, int(max_age_seconds)):
            return None
        return {
            "symbol": sym,
            "price": row.get("price"),
            "ts": ts,
            "source": "twelvedata_ws",
        }

    async def start(self) -> None:
        async with self._lock:
            if self.started:
                return
            self.started = True
            self._stop_event.clear()
            if not self.enabled:
                if websockets is None:
                    self.last_error = "websockets import failed"
                elif not self._api_key:
                    self.last_error = "TWELVEDATA_API_KEY missing"
                else:
                    self.last_error = "TWELVEDATA_WS disabled"
                logger.warning("twelvedata ws not started: %s", self.last_error)
                return
            self._runner_task = asyncio.create_task(self.run_forever())

    async def stop(self) -> None:
        self._stop_event.set()
        self.connected = False
        task = self._runner_task
        hb = self._heartbeat_task
        ws = self._ws
        self._runner_task = None
        self._heartbeat_task = None
        self._ws = None
        if hb:
            hb.cancel()
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass
        if task:
            task.cancel()
            try:
                await task
            except Exception:
                pass
        self.started = False

    async def subscribe(self, symbols: List[str]) -> Dict[str, Any]:
        incoming = [_norm_symbol(s) for s in (symbols or []) if _norm_symbol(s)]
        if not incoming:
            return self._status()

        added: List[str] = []
        removed: List[str] = []
        for sym in incoming:
            if sym in self._sub_set:
                continue
            self._sub_set.add(sym)
            self._sub_order.append(sym)
            added.append(sym)

        if len(self._sub_order) > self._max_subs:
            keep = self._sub_order[: self._max_subs]
            keep_set = set(keep)
            for sym in list(self._sub_order):
                if sym not in keep_set:
                    removed.append(sym)
                    self._sub_set.discard(sym)
            self._sub_order = keep

        if self.connected and self._ws is not None and added:
            await self._send_subscribe(added)
        if self.connected and self._ws is not None and removed:
            await self._send_unsubscribe(removed)
        return self._status()

    async def unsubscribe(self, symbols: List[str]) -> Dict[str, Any]:
        outgoing = [_norm_symbol(s) for s in (symbols or []) if _norm_symbol(s)]
        if not outgoing:
            return self._status()
        removed = []
        for sym in outgoing:
            if sym in self._sub_set:
                removed.append(sym)
            self._sub_set.discard(sym)
        if removed:
            self._sub_order = [s for s in self._sub_order if s not in set(removed)]
        if self.connected and self._ws is not None and removed:
            await self._send_unsubscribe(removed)
        return self._status()

    async def _send_json(self, payload: Dict[str, Any]) -> None:
        if self._ws is None:
            return
        await self._ws.send(json.dumps(payload))

    async def _send_subscribe(self, symbols: List[str]) -> None:
        if not symbols:
            return
        payload = {"action": "subscribe", "params": {"symbols": ",".join(symbols)}}
        await self._send_json(payload)

    async def _send_unsubscribe(self, symbols: List[str]) -> None:
        if not symbols:
            return
        payload = {"action": "unsubscribe", "params": {"symbols": ",".join(symbols)}}
        await self._send_json(payload)

    async def _heartbeat_loop(self) -> None:
        while not self._stop_event.is_set() and self.connected and self._ws is not None:
            try:
                await self._send_json({"action": "heartbeat"})
            except Exception as exc:
                self.last_error = f"heartbeat failed: {exc}"
                return
            await asyncio.sleep(10)

    def _handle_payload(self, payload: Dict[str, Any]) -> None:
        self.message_count_total += 1
        self._msg_timestamps.append(time.monotonic())
        self.last_message_at = _utc_now()
        self._recent_msgs.append({
            "ts": _iso(self.last_message_at),
            "payload": payload,
        })

        event_name = str(payload.get("event") or "").strip().lower()
        if event_name == "subscribe-status":
            fails = payload.get("fails")
            if isinstance(fails, list):
                failed_symbols = {_norm_symbol(item.get("symbol")) for item in fails if isinstance(item, dict)}
                failed_symbols.discard("")
                if failed_symbols:
                    self._sub_set.difference_update(failed_symbols)
                    self._sub_order = [sym for sym in self._sub_order if sym not in failed_symbols]

        msg_text = json.dumps(payload).lower()
        if any(k in msg_text for k in ("not authorized", "forbidden", "plan", "not permitted")):
            self.ws_entitlement = "restricted"
            self.last_error = "WS not permitted on current plan"
            return

        data_rows = payload.get("data")
        if isinstance(data_rows, list):
            for row in data_rows:
                if isinstance(row, dict):
                    self._upsert_price_from_row(row)
            return
        symbol = _norm_symbol(payload.get("symbol"))
        price_raw = payload.get("price")
        if not symbol:
            data = payload.get("data")
            if isinstance(data, dict):
                symbol = _norm_symbol(data.get("symbol"))
                price_raw = data.get("price", price_raw)
        self._upsert_price(symbol=symbol, price_raw=price_raw, payload=payload)

    def _upsert_price_from_row(self, row: Dict[str, Any]) -> None:
        symbol = _norm_symbol(row.get("symbol"))
        price_raw = row.get("price")
        self._upsert_price(symbol=symbol, price_raw=price_raw, payload=row)

    def _upsert_price(self, symbol: str, price_raw: Any, payload: Dict[str, Any]) -> None:
        try:
            price = float(price_raw) if price_raw not in (None, "") else None
        except Exception:
            price = None
        if not symbol or price is None or price <= 0:
            return
        ts = _parse_ts(payload.get("timestamp") or payload.get("ts") or payload.get("datetime"))
        self._prices[symbol] = {"price": float(price), "ts": ts, "source": "twelvedata_ws"}

    async def run_forever(self) -> None:
        if not self.enabled:
            return
        backoff = 1
        while not self._stop_event.is_set():
            try:
                logger.info("twelvedata ws connecting to %s", self._ws_url)
                async with websockets.connect(self._ws_url, ping_interval=None, close_timeout=5) as ws:  # type: ignore[union-attr]
                    self._ws = ws
                    self.connected = True
                    self.last_error = None
                    self.ws_entitlement = "active"
                    backoff = 1
                    if self._sub_order:
                        await self._send_subscribe(self._sub_order)
                    self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
                    async for message in ws:
                        if self._stop_event.is_set():
                            break
                        try:
                            payload = json.loads(message)
                        except Exception:
                            continue
                        if isinstance(payload, dict):
                            self._handle_payload(payload)
                        elif isinstance(payload, list):
                            for item in payload:
                                if isinstance(item, dict):
                                    self._handle_payload(item)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.connected = False
                self.last_error = str(exc)
                logger.warning("twelvedata ws disconnected: %s", exc)
            finally:
                self.connected = False
                if self._heartbeat_task:
                    self._heartbeat_task.cancel()
                    self._heartbeat_task = None
                self._ws = None
            if self._stop_event.is_set():
                break
            wait_for = 300 if self.ws_entitlement == "restricted" else min(30, backoff)
            await asyncio.sleep(wait_for)
            if self.ws_entitlement != "restricted":
                backoff = min(30, backoff * 2)


_ws_singleton: Optional[TwelveDataWSClient] = None


def get_ws_client() -> TwelveDataWSClient:
    global _ws_singleton
    if _ws_singleton is None:
        _ws_singleton = TwelveDataWSClient()
    return _ws_singleton
