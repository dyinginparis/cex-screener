from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

import ccxt
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ValidationError

from .engine import LiveStateEngine, now_utc_text
from .models import ClientEnvelope, StartPayload, UpdateFiltersPayload

app = FastAPI(title="CEX Live Engine API", version="0.1.0")

CHART_TIMEFRAMES = {"1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d"}
EXCLUDED_EXCHANGE_TOKENS = ("kraken", "mexc")
_chart_exchange_cache: dict[str, ccxt.Exchange] = {}
_chart_exchange_locks: dict[str, asyncio.Lock] = {}

HEATMAP_PROVIDER = os.getenv("HEATMAP_PROVIDER", "auto").strip().lower()
COINAPI_REST_BASE = os.getenv("COINAPI_REST_BASE", "https://rest.coinapi.io").strip().rstrip("/")
COINAPI_API_KEY = os.getenv("COINAPI_API_KEY", "").strip()
HEATMAP_CCXT_DEFAULT_EXCHANGE = str(
    os.getenv("HEATMAP_CCXT_DEFAULT_EXCHANGE", "bybit")
).strip().lower()
HEATMAP_SYMBOL_CACHE_TTL_SEC = 300.0
HEATMAP_HTTP_TIMEOUT_SEC = 20.0
_heatmap_symbol_cache: tuple[float, list[dict[str, Any]]] | None = None
_heatmap_symbol_lock = asyncio.Lock()
_heatmap_ccxt_symbol_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_heatmap_ccxt_symbol_locks: dict[str, asyncio.Lock] = {}

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


def _heatmap_provider_info() -> dict[str, Any]:
    provider = _resolve_heatmap_provider()
    configured = False
    notes: list[str] = []

    if provider == "coinapi":
        configured = bool(COINAPI_API_KEY)
        notes = [
            "CoinAPI mode requires a commercial CoinAPI plan and valid API key.",
            "Redistribution/resale rights depend on your CoinAPI contract.",
        ]
    elif provider == "ccxt":
        configured = True
        notes = [
            "CCXT mode uses direct exchange APIs (snapshot orderbooks).",
            "Commercial rights depend on each exchange API Terms.",
        ]
    else:
        notes = [f"Unsupported provider '{provider}'."]

    return {
        "provider": provider,
        "configured": configured,
        "legal_mode": "provider-terms-required",
        "default_exchange": HEATMAP_CCXT_DEFAULT_EXCHANGE,
        "notes": notes,
    }


@app.get("/meta/heatmap")
async def meta_heatmap() -> dict[str, Any]:
    return _heatmap_provider_info()


def _resolve_heatmap_provider() -> str:
    raw = str(HEATMAP_PROVIDER or "").strip().lower()
    if raw in {"", "auto"}:
        return "coinapi" if COINAPI_API_KEY else "ccxt"
    return raw


def _ensure_heatmap_provider_ready() -> str:
    provider = _resolve_heatmap_provider()
    if provider not in {"coinapi", "ccxt"}:
        raise HTTPException(
            status_code=501,
            detail=f"unsupported HEATMAP_PROVIDER '{HEATMAP_PROVIDER}'. expected: auto|coinapi|ccxt",
        )
    if provider == "coinapi" and not COINAPI_API_KEY:
        raise HTTPException(
            status_code=503,
            detail=(
                "heatmap provider is not configured. Set COINAPI_API_KEY in backend environment "
                "to use licensed heatmap data."
            ),
        )
    return provider


def _coinapi_headers() -> dict[str, str]:
    return {
        "Accept": "application/json",
        "X-CoinAPI-Key": COINAPI_API_KEY,
    }


def _http_get_json(url: str, headers: dict[str, str], timeout_sec: float = HEATMAP_HTTP_TIMEOUT_SEC) -> Any:
    request = urllib_request.Request(url=url, method="GET", headers=headers)
    with urllib_request.urlopen(request, timeout=timeout_sec) as response:  # noqa: S310 - fixed provider URL
        raw = response.read()
        text = raw.decode("utf-8", errors="replace")
        return json.loads(text)


def _extract_coinapi_symbols(payload: Any) -> list[dict[str, Any]]:
    rows = payload if isinstance(payload, list) else payload.get("data", []) if isinstance(payload, dict) else []
    out: list[dict[str, Any]] = []

    for item in rows:
        if not isinstance(item, dict):
            continue
        symbol_id = str(item.get("symbol_id") or "").strip()
        if not symbol_id:
            continue

        symbol_type = str(item.get("symbol_type") or "").strip().upper()
        if symbol_type and symbol_type not in {"SPOT", "PERPETUAL", "FUTURES"}:
            continue

        base = str(item.get("asset_id_base") or "").strip().upper()
        quote = str(item.get("asset_id_quote") or "").strip().upper()
        exchange_id = str(item.get("exchange_id") or "").strip().lower()
        out.append(
            {
                "symbol_id": symbol_id,
                "exchange_id": exchange_id,
                "base": base,
                "quote": quote,
                "symbol_type": symbol_type or "UNKNOWN",
            }
        )
    return out


def _coinapi_fetch_all_symbols() -> list[dict[str, Any]]:
    url = f"{COINAPI_REST_BASE}/v1/symbols"
    payload = _http_get_json(url, _coinapi_headers())
    rows = _extract_coinapi_symbols(payload)
    rows.sort(
        key=lambda item: (
            str(item.get("exchange_id") or ""),
            str(item.get("base") or ""),
            str(item.get("quote") or ""),
            str(item.get("symbol_id") or ""),
        )
    )
    return rows


async def _get_heatmap_symbols_cached() -> list[dict[str, Any]]:
    global _heatmap_symbol_cache

    now_ts = time.time()
    cached = _heatmap_symbol_cache
    if cached and (now_ts - cached[0]) < HEATMAP_SYMBOL_CACHE_TTL_SEC:
        return cached[1]

    async with _heatmap_symbol_lock:
        now_ts = time.time()
        cached = _heatmap_symbol_cache
        if cached and (now_ts - cached[0]) < HEATMAP_SYMBOL_CACHE_TTL_SEC:
            return cached[1]

        try:
            symbols = await asyncio.to_thread(_coinapi_fetch_all_symbols)
        except urllib_error.HTTPError as exc:
            status = int(getattr(exc, "code", 502))
            try:
                detail_text = exc.read().decode("utf-8", errors="replace")
            except Exception:
                detail_text = str(exc)
            raise HTTPException(
                status_code=502,
                detail=f"coinapi symbols request failed (HTTP {status}): {detail_text[:200]}",
            ) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"coinapi symbols request failed: {exc}") from exc

        _heatmap_symbol_cache = (now_ts, symbols)
        return symbols


def _heatmap_market_type_matches(market: dict[str, Any], market_type: str) -> bool:
    mode = str(market_type or "both").strip().lower()
    if mode == "perpetual":
        return bool(market.get("swap"))
    if mode == "spot":
        return bool(market.get("spot"))
    return bool(market.get("swap") or market.get("spot") or market.get("future"))


def _heatmap_market_type_label(market: dict[str, Any]) -> str:
    if market.get("swap"):
        return "PERPETUAL"
    if market.get("future"):
        return "FUTURES"
    if market.get("spot"):
        return "SPOT"
    return "UNKNOWN"


def _build_ccxt_heatmap_symbols(
    exchange: ccxt.Exchange,
    exchange_id: str,
    market_type: str,
) -> list[dict[str, Any]]:
    markets = exchange.load_markets()
    rows: list[dict[str, Any]] = []

    for symbol, market in markets.items():
        if not isinstance(market, dict):
            continue
        if not market.get("active", True):
            continue
        if not _heatmap_market_type_matches(market, market_type):
            continue

        base = str(market.get("base") or "").strip().upper()
        quote = str(market.get("quote") or "").strip().upper()
        symbol_type = _heatmap_market_type_label(market)
        rows.append(
            {
                "symbol_id": f"{exchange_id}|{symbol}",
                "exchange_id": exchange_id,
                "base": base,
                "quote": quote,
                "symbol_type": symbol_type,
            }
        )

    rows.sort(
        key=lambda item: (
            str(item.get("exchange_id") or ""),
            str(item.get("base") or ""),
            str(item.get("quote") or ""),
            str(item.get("symbol_id") or ""),
        )
    )
    return rows


async def _get_ccxt_heatmap_symbols_cached(exchange_id: str, market_type: str) -> list[dict[str, Any]]:
    cache_key = f"{exchange_id}|{market_type}"
    now_ts = time.time()
    cached = _heatmap_ccxt_symbol_cache.get(cache_key)
    if cached and (now_ts - cached[0]) < HEATMAP_SYMBOL_CACHE_TTL_SEC:
        return cached[1]

    lock = _heatmap_ccxt_symbol_locks.setdefault(cache_key, asyncio.Lock())
    async with lock:
        now_ts = time.time()
        cached = _heatmap_ccxt_symbol_cache.get(cache_key)
        if cached and (now_ts - cached[0]) < HEATMAP_SYMBOL_CACHE_TTL_SEC:
            return cached[1]

        exchange = await _get_chart_exchange(exchange_id)
        try:
            rows = await asyncio.to_thread(_build_ccxt_heatmap_symbols, exchange, exchange_id, market_type)
        except Exception as exc:
            raise HTTPException(
                status_code=502,
                detail=f"{exchange_id}: heatmap symbols build failed ({exc})",
            ) from exc
        _heatmap_ccxt_symbol_cache[cache_key] = (now_ts, rows)
        return rows


def _parse_ccxt_symbol_id(symbol_id: str) -> tuple[str, str]:
    raw = str(symbol_id or "").strip()
    if "|" not in raw:
        raise HTTPException(
            status_code=400,
            detail="ccxt heatmap symbol_id must be formatted as 'exchange_id|symbol'",
        )
    exchange_id, symbol = raw.split("|", 1)
    normalized_exchange = normalize_exchange_id(exchange_id)
    normalized_symbol = str(symbol or "").strip()
    if not normalized_exchange or not normalized_symbol:
        raise HTTPException(
            status_code=400,
            detail="invalid ccxt symbol_id format. expected 'exchange_id|symbol'",
        )
    if not is_exchange_allowed(normalized_exchange):
        raise HTTPException(status_code=403, detail=f"exchange blocked: {normalized_exchange}")
    return normalized_exchange, normalized_symbol

@app.get("/heatmap/symbols")
async def heatmap_symbols(
    search: str = Query("", description="Case-insensitive symbol filter"),
    limit: int = Query(120, ge=10, le=500),
    exchange_id: str | None = Query(None, description="Required for ccxt mode; ignored by coinapi mode"),
    market_type: str = Query("both", description="both|perpetual|spot (used in ccxt mode)"),
) -> dict[str, Any]:
    provider = _ensure_heatmap_provider_ready()
    normalized_market_type = str(market_type or "both").strip().lower()
    if normalized_market_type not in {"both", "perpetual", "spot"}:
        raise HTTPException(status_code=400, detail="invalid market_type. expected both|perpetual|spot")

    if provider == "coinapi":
        symbols = await _get_heatmap_symbols_cached()
        selected_exchange = None
    else:
        selected_exchange = normalize_exchange_id(exchange_id or HEATMAP_CCXT_DEFAULT_EXCHANGE)
        if not selected_exchange:
            raise HTTPException(status_code=400, detail="exchange_id is required for ccxt heatmap mode")
        if not is_exchange_allowed(selected_exchange):
            raise HTTPException(status_code=403, detail=f"exchange blocked: {selected_exchange}")
        symbols = await _get_ccxt_heatmap_symbols_cached(selected_exchange, normalized_market_type)

    query = str(search or "").strip().lower()

    if query:
        filtered = [
            item
            for item in symbols
            if query in str(item.get("symbol_id") or "").lower()
            or query in str(item.get("base") or "").lower()
            or query in str(item.get("quote") or "").lower()
            or query in str(item.get("exchange_id") or "").lower()
        ]
    else:
        filtered = symbols

    return {
        "provider": provider,
        "exchange_id": selected_exchange,
        "market_type": normalized_market_type,
        "count": len(filtered),
        "items": filtered[:limit],
    }


def _parse_orderbook_side(raw_side: Any) -> list[tuple[float, float]]:
    out: list[tuple[float, float]] = []
    if not isinstance(raw_side, list):
        return out

    for level in raw_side:
        price: float | None = None
        size: float | None = None

        if isinstance(level, (list, tuple)) and len(level) >= 2:
            try:
                price = float(level[0])
                size = float(level[1])
            except Exception:
                continue
        elif isinstance(level, dict):
            for key in ("price", "rate", "limit_price", "px"):
                if key in level:
                    try:
                        price = float(level[key])
                        break
                    except Exception:
                        price = None
            for key in ("size", "quantity", "qty", "volume", "amount"):
                if key in level:
                    try:
                        size = float(level[key])
                        break
                    except Exception:
                        size = None
        if price is None or size is None:
            continue
        if price <= 0 or size <= 0:
            continue
        out.append((price, size))
    return out


def _aggregate_side_bins(
    side_levels: list[tuple[float, float]],
    mid_price: float,
    side: str,
    bucket_count: int,
    range_bps: float,
) -> list[dict[str, Any]]:
    if mid_price <= 0 or bucket_count <= 0 or range_bps <= 0:
        return []

    bucket_width = range_bps / bucket_count
    buckets = [
        {
            "bucket_index": index,
            "from_bps": index * bucket_width,
            "to_bps": (index + 1) * bucket_width,
            "total_size": 0.0,
            "total_notional": 0.0,
            "level_count": 0,
        }
        for index in range(bucket_count)
    ]

    for price, size in side_levels:
        if side == "bid":
            distance_bps = ((mid_price - price) / mid_price) * 10000.0
        else:
            distance_bps = ((price - mid_price) / mid_price) * 10000.0
        if distance_bps < 0:
            continue
        if distance_bps > range_bps:
            continue

        bucket_index = min(bucket_count - 1, int(distance_bps / bucket_width))
        bucket = buckets[bucket_index]
        bucket["total_size"] += size
        bucket["total_notional"] += price * size
        bucket["level_count"] += 1

    max_notional = max((float(bucket["total_notional"]) for bucket in buckets), default=0.0)

    for bucket in buckets:
        mid_bps = (float(bucket["from_bps"]) + float(bucket["to_bps"])) / 2.0
        if side == "bid":
            ref_price = mid_price * (1.0 - mid_bps / 10000.0)
        else:
            ref_price = mid_price * (1.0 + mid_bps / 10000.0)
        bucket["reference_price"] = ref_price
        bucket["intensity"] = (
            float(bucket["total_notional"]) / max_notional if max_notional > 0 else 0.0
        )

    if side == "ask":
        buckets.sort(key=lambda item: float(item["from_bps"]))
    else:
        buckets.sort(key=lambda item: float(item["from_bps"]))
    return buckets


def _extract_orderbook_payload(raw_payload: Any) -> dict[str, Any]:
    if isinstance(raw_payload, dict):
        return raw_payload
    if isinstance(raw_payload, list) and raw_payload and isinstance(raw_payload[0], dict):
        return raw_payload[0]
    return {}


def _fetch_coinapi_orderbook(symbol_id: str, limit_levels: int) -> dict[str, Any]:
    encoded_symbol = urllib_parse.quote(symbol_id, safe="")
    urls = [
        f"{COINAPI_REST_BASE}/v1/orderbooks/{encoded_symbol}/current?limit_levels={limit_levels}",
        f"{COINAPI_REST_BASE}/v1/orderbooks3/{encoded_symbol}/current?limit_levels={limit_levels}",
        f"{COINAPI_REST_BASE}/v1/orderbooks/{encoded_symbol}/snapshot?limit_levels={limit_levels}",
    ]
    headers = _coinapi_headers()

    last_error = "unknown orderbook error"
    for url in urls:
        try:
            payload = _http_get_json(url, headers)
            parsed = _extract_orderbook_payload(payload)
            if parsed:
                return parsed
        except urllib_error.HTTPError as exc:
            status = int(getattr(exc, "code", 502))
            try:
                detail_text = exc.read().decode("utf-8", errors="replace")
            except Exception:
                detail_text = str(exc)
            last_error = f"HTTP {status}: {detail_text[:180]}"
            continue
        except Exception as exc:
            last_error = str(exc)
            continue

    raise HTTPException(status_code=502, detail=f"coinapi orderbook request failed: {last_error}")


@app.get("/heatmap/orderbook")
async def heatmap_orderbook(
    symbol_id: str = Query(
        ...,
        description=(
            "Provider symbol id. coinapi: BINANCE_SPOT_BTC_USDT; "
            "ccxt: exchange_id|symbol (e.g. bybit|BTC/USDT:USDT)"
        ),
    ),
    levels: int = Query(32, ge=12, le=120, description="Heatmap bucket count per side"),
    range_bps: float = Query(300.0, ge=25.0, le=2500.0, description="Visible range in basis points from mid"),
) -> dict[str, Any]:
    provider = _ensure_heatmap_provider_ready()
    normalized_symbol = str(symbol_id or "").strip()
    if not normalized_symbol:
        raise HTTPException(status_code=400, detail="symbol_id is required")

    if provider == "coinapi":
        raw_book = await asyncio.to_thread(_fetch_coinapi_orderbook, normalized_symbol, max(120, levels * 3))
        resolved_exchange_id: str | None = None
    else:
        exchange_id, ccxt_symbol = _parse_ccxt_symbol_id(normalized_symbol)
        exchange = await _get_chart_exchange(exchange_id)
        if not exchange.has.get("fetchOrderBook"):
            raise HTTPException(status_code=400, detail=f"{exchange_id}: fetchOrderBook not supported")
        try:
            await asyncio.to_thread(exchange.load_markets)
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"{exchange_id}: load_markets failed ({exc})") from exc
        if ccxt_symbol not in exchange.markets:
            raise HTTPException(status_code=404, detail=f"{exchange_id}: symbol not found: {ccxt_symbol}")
        try:
            raw_book = await asyncio.to_thread(exchange.fetch_order_book, ccxt_symbol, max(120, levels * 3))
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"{exchange_id}: fetch_order_book failed ({exc})") from exc
        resolved_exchange_id = exchange_id

    bids = _parse_orderbook_side(raw_book.get("bids"))
    asks = _parse_orderbook_side(raw_book.get("asks"))
    bids.sort(key=lambda item: item[0], reverse=True)
    asks.sort(key=lambda item: item[0])

    top_bid = bids[0][0] if bids else None
    top_ask = asks[0][0] if asks else None
    if top_bid and top_ask:
        mid_price = (top_bid + top_ask) / 2.0
    elif top_bid:
        mid_price = top_bid
    elif top_ask:
        mid_price = top_ask
    else:
        raise HTTPException(status_code=404, detail=f"no orderbook levels returned for {normalized_symbol}")

    spread_bps = ((top_ask - top_bid) / mid_price) * 10000.0 if top_bid and top_ask and mid_price > 0 else None
    bid_bins = _aggregate_side_bins(bids, mid_price, "bid", levels, range_bps)
    ask_bins = _aggregate_side_bins(asks, mid_price, "ask", levels, range_bps)

    return {
        "provider": provider,
        "exchange_id": resolved_exchange_id,
        "symbol_id": normalized_symbol,
        "mid_price": mid_price,
        "spread_bps": spread_bps,
        "range_bps": range_bps,
        "bucket_count": levels,
        "bids": bid_bins,
        "asks": ask_bins,
        "fetched_at_utc": now_utc_text(),
        "legal_mode": "derived-display",
    }


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
        done, pending = await asyncio.wait(
            {recv_task, send_task}, return_when=asyncio.FIRST_COMPLETED
        )
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
    global _heatmap_symbol_cache
    for exchange in _chart_exchange_cache.values():
        close_method = getattr(exchange, "close", None)
        if callable(close_method):
            try:
                await asyncio.to_thread(close_method)
            except Exception:
                pass
    _chart_exchange_cache.clear()
    _chart_exchange_locks.clear()
    _heatmap_symbol_cache = None
    _heatmap_ccxt_symbol_cache.clear()
    _heatmap_ccxt_symbol_locks.clear()
