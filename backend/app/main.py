from __future__ import annotations

import asyncio
from typing import Any

import ccxt
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ValidationError

from .engine import LiveStateEngine, now_utc_text
from .market_data_loader import market_matches_mode, market_type_for_market
from .models import ClientEnvelope, StartPayload, UpdateFiltersPayload

app = FastAPI(title="CEX Live Engine API", version="0.1.0")

CHART_TIMEFRAMES = {"1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d"}
EXCLUDED_EXCHANGE_TOKENS = ("kraken", "mexc")
_chart_exchange_cache: dict[str, ccxt.Exchange] = {}
_chart_exchange_locks: dict[str, asyncio.Lock] = {}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"ok": True, "time_utc": now_utc_text()}


def normalize_exchange_id(exchange_id: str) -> str:
    return str(exchange_id or "").strip().lower()


def is_exchange_allowed(exchange_id: str) -> bool:
    normalized = normalize_exchange_id(exchange_id)
    if not normalized:
        return False
    return not any(token in normalized for token in EXCLUDED_EXCHANGE_TOKENS)


def filter_allowed_exchanges(exchanges: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for exchange_id in exchanges:
        normalized = normalize_exchange_id(exchange_id)
        if not is_exchange_allowed(normalized):
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


@app.get("/meta/exchanges")
async def meta_exchanges() -> dict[str, Any]:
    exchanges = sorted(
        [exchange_id for exchange_id in ccxt.exchanges if is_exchange_allowed(exchange_id)],
        key=lambda value: value.lower(),
    )
    return {"count": len(exchanges), "exchanges": exchanges}


async def _get_chart_exchange(exchange_id: str) -> ccxt.Exchange:
    normalized = normalize_exchange_id(exchange_id)
    if not normalized:
        raise HTTPException(status_code=400, detail="exchange_id is required")
    if not is_exchange_allowed(normalized):
        raise HTTPException(status_code=403, detail=f"exchange blocked: {normalized}")

    cached = _chart_exchange_cache.get(normalized)
    if cached is not None:
        return cached

    lock = _chart_exchange_locks.setdefault(normalized, asyncio.Lock())
    async with lock:
        cached = _chart_exchange_cache.get(normalized)
        if cached is not None:
            return cached

        try:
            exchange_class = getattr(ccxt, normalized)
        except AttributeError as exc:
            raise HTTPException(status_code=404, detail=f"unknown exchange_id: {normalized}") from exc

        exchange = exchange_class(
            {
                "enableRateLimit": True,
                "timeout": 20000,
                "options": {"adjustForTimeDifference": True},
            }
        )
        _chart_exchange_cache[normalized] = exchange
        return exchange


@app.get("/symbols")
async def symbols(
    exchange_id: str = Query(..., description="CCXT exchange id"),
    market_type: str = Query("both", description="both|perpetual|spot"),
    search: str = Query("", description="Case-insensitive pair filter"),
    limit: int = Query(500, ge=1, le=5000),
) -> dict[str, Any]:
    normalized_market_type = str(market_type or "both").strip().lower()
    if normalized_market_type not in {"both", "perpetual", "spot"}:
        raise HTTPException(status_code=400, detail="invalid market_type. expected both|perpetual|spot")

    exchange = await _get_chart_exchange(exchange_id)
    try:
        markets = await asyncio.to_thread(exchange.load_markets)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"{exchange_id}: load_markets failed ({exc})") from exc

    query = str(search or "").strip().lower()
    items: list[dict[str, Any]] = []
    for symbol, market in markets.items():
        if not market.get("active", True):
            continue
        if not market_matches_mode(market, normalized_market_type):
            continue
        if query and query not in symbol.lower():
            continue
        items.append(
            {
                "id": f"{normalize_exchange_id(exchange_id)}|{symbol}",
                "exchange_id": normalize_exchange_id(exchange_id),
                "symbol": symbol,
                "market_type": market_type_for_market(market),
            }
        )

    items.sort(
        key=lambda item: (
            str(item.get("exchange_id") or ""),
            str(item.get("symbol") or ""),
        )
    )
    return {
        "exchange_id": normalize_exchange_id(exchange_id),
        "market_type": normalized_market_type,
        "count": len(items),
        "items": items[:limit],
    }


@app.get("/chart/ohlcv")
async def chart_ohlcv(
    exchange_id: str = Query(..., description="CCXT exchange id"),
    symbol: str = Query(..., description="Market symbol (e.g. BTC/USDT:USDT)"),
    timeframe: str = Query("1m", description="Candle timeframe"),
    limit: int = Query(300, ge=50, le=1000, description="Max candles"),
) -> dict[str, Any]:
    tf = timeframe.strip().lower()
    if tf not in CHART_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"unsupported timeframe '{timeframe}'. supported: {sorted(CHART_TIMEFRAMES)}",
        )

    exchange = await _get_chart_exchange(exchange_id)
    if not exchange.has.get("fetchOHLCV"):
        raise HTTPException(status_code=400, detail=f"{exchange_id}: fetchOHLCV not supported")

    try:
        await asyncio.to_thread(exchange.load_markets)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"{exchange_id}: load_markets failed ({exc})") from exc

    if symbol not in exchange.markets:
        raise HTTPException(status_code=404, detail=f"{exchange_id}: symbol not found: {symbol}")

    try:
        raw = await asyncio.to_thread(exchange.fetch_ohlcv, symbol, tf, None, limit)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"{exchange_id}: fetch_ohlcv failed ({exc})") from exc

    candles: list[dict[str, Any]] = []
    for item in raw or []:
        if not isinstance(item, list | tuple) or len(item) < 5:
            continue

        try:
            ts_ms = int(item[0])
            open_price = float(item[1])
            high_price = float(item[2])
            low_price = float(item[3])
            close_price = float(item[4])
            volume = float(item[5]) if len(item) > 5 and item[5] is not None else None
        except Exception:
            continue

        candles.append(
            {
                "time": ts_ms // 1000,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
            }
        )

    if not candles:
        raise HTTPException(status_code=404, detail=f"{exchange_id}: no candles returned for {symbol} {tf}")

    return {
        "exchange_id": exchange_id,
        "symbol": symbol,
        "timeframe": tf,
        "count": len(candles),
        "candles": candles,
    }


class ClientSession:
    def __init__(self, websocket: WebSocket) -> None:
        self.websocket = websocket
        self.out_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.engine: LiveStateEngine | None = None
        self.running = False

    async def start_engine(self, payload: StartPayload) -> None:
        await self.stop_engine()
        self.engine = LiveStateEngine(payload=payload, out_queue=self.out_queue)
        await self.engine.start()
        self.running = True
        await self.out_queue.put(
            {
                "type": "status",
                "state": "running",
                "server_time_utc": now_utc_text(),
                "details": {"mode": "live"},
            }
        )

    async def stop_engine(self) -> None:
        if self.engine:
            await self.engine.stop()
        self.engine = None
        self.running = False

    async def receive_loop(self) -> None:
        while True:
            raw = await self.websocket.receive_json()
            try:
                envelope = ClientEnvelope.model_validate(raw)
            except ValidationError as exc:
                await self.out_queue.put(
                    {"type": "error", "server_time_utc": now_utc_text(), "message": str(exc)}
                )
                continue

            msg_type = envelope.type.lower()
            if msg_type == "start":
                try:
                    payload = StartPayload.model_validate(envelope.payload)
                except ValidationError as exc:
                    await self.out_queue.put(
                        {
                            "type": "error",
                            "server_time_utc": now_utc_text(),
                            "message": f"invalid start payload: {exc}",
                        }
                    )
                    continue
                blocked_exchanges = sorted(
                    {
                        normalize_exchange_id(exchange_id)
                        for exchange_id in payload.exchanges
                        if not is_exchange_allowed(exchange_id)
                    }
                )
                payload.exchanges = filter_allowed_exchanges(payload.exchanges)
                if blocked_exchanges:
                    await self.out_queue.put(
                        {
                            "type": "error",
                            "server_time_utc": now_utc_text(),
                            "message": f"excluded exchanges removed: {', '.join(blocked_exchanges)}",
                        }
                    )
                if not payload.exchanges:
                    await self.out_queue.put(
                        {
                            "type": "error",
                            "server_time_utc": now_utc_text(),
                            "message": "no allowed exchanges selected (kraken/mexc are excluded)",
                        }
                    )
                    continue
                await self.start_engine(payload)
            elif msg_type == "pause":
                if self.engine:
                    self.engine.pause()
                await self.out_queue.put(
                    {
                        "type": "status",
                        "state": "paused",
                        "server_time_utc": now_utc_text(),
                        "details": {},
                    }
                )
            elif msg_type == "resume":
                if self.engine:
                    self.engine.resume()
                await self.out_queue.put(
                    {
                        "type": "status",
                        "state": "running",
                        "server_time_utc": now_utc_text(),
                        "details": {},
                    }
                )
            elif msg_type == "stop":
                await self.stop_engine()
                await self.out_queue.put(
                    {
                        "type": "status",
                        "state": "stopped",
                        "server_time_utc": now_utc_text(),
                        "details": {},
                    }
                )
            elif msg_type == "update_filters":
                if not self.engine:
                    await self.out_queue.put(
                        {
                            "type": "error",
                            "server_time_utc": now_utc_text(),
                            "message": "engine not running",
                        }
                    )
                    continue
                try:
                    payload = UpdateFiltersPayload.model_validate(envelope.payload)
                except ValidationError as exc:
                    await self.out_queue.put(
                        {
                            "type": "error",
                            "server_time_utc": now_utc_text(),
                            "message": f"invalid filter payload: {exc}",
                        }
                    )
                    continue
                self.engine.update_filters(payload.filters)
                await self.out_queue.put(
                    {
                        "type": "status",
                        "state": "filters_updated",
                        "server_time_utc": now_utc_text(),
                        "details": payload.filters.model_dump(),
                    }
                )
            else:
                await self.out_queue.put(
                    {
                        "type": "error",
                        "server_time_utc": now_utc_text(),
                        "message": f"unsupported message type: {envelope.type}",
                    }
                )

    async def send_loop(self) -> None:
        while True:
            event = await self.out_queue.get()
            await self.websocket.send_json(event)

    async def shutdown(self) -> None:
        await self.stop_engine()


@app.websocket("/ws/live")
async def ws_live(websocket: WebSocket) -> None:
    await websocket.accept()
    session = ClientSession(websocket)
    await session.out_queue.put(
        {
            "type": "status",
            "state": "connected",
            "server_time_utc": now_utc_text(),
            "details": {"hint": "send {type:'start', payload:{...}}"},
        }
    )

    recv_task = asyncio.create_task(session.receive_loop(), name="ws-receive")
    send_task = asyncio.create_task(session.send_loop(), name="ws-send")

    try:
        done, pending = await asyncio.wait({recv_task, send_task}, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        for task in done:
            exc = task.exception()
            if exc and not isinstance(exc, WebSocketDisconnect):
                raise exc
    except WebSocketDisconnect:
        pass
    finally:
        await session.shutdown()
        recv_task.cancel()
        send_task.cancel()


@app.on_event("shutdown")
async def close_chart_exchanges() -> None:
    for exchange in _chart_exchange_cache.values():
        close_method = getattr(exchange, "close", None)
        if callable(close_method):
            try:
                await asyncio.to_thread(close_method)
            except Exception:
                pass
    _chart_exchange_cache.clear()
    _chart_exchange_locks.clear()
