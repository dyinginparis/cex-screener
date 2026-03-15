from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import ccxt

from .models import FilterConfig

FALLBACK_FETCH_TICKER_LIMIT = 80
FALLBACK_FETCH_TICKER_BUDGET_MULTIPLIER = 1.2
FALLBACK_FETCH_TICKER_BUDGET_MIN_SEC = 0.8
FETCH_TIMEOUT_SEC_MIN = 0.8
FETCH_TIMEOUT_SEC_MAX = 4.0
FETCH_TIMEOUT_MULTIPLIER = 1.35
DEFAULT_RSI_TIMEFRAME = "1d"
DEFAULT_RSI_PERIOD = 14
RSI_OHLCV_LIMIT = 200
RSI_CACHE_TTL_SEC = 90.0
RSI_TIMEOUT_SEC = 3.0
RSI_RECALC_PER_TICK = 12
RSI_REFRESH_INTERVAL_SEC = 8.0
DEFAULT_NATR_TIMEFRAME = "1h"
DEFAULT_NATR_PERIOD = 14
NATR_OHLCV_LIMIT = 200
NATR_CACHE_TTL_SEC = 90.0
NATR_TIMEOUT_SEC = 3.0
NATR_RECALC_PER_TICK = 12
NATR_REFRESH_INTERVAL_SEC = 8.0
TICKER_TASK_MAX_AGE_SEC = 20.0
INITIAL_TICKER_WAIT_MIN_SEC = 0.35
INITIAL_TICKER_WAIT_MAX_SEC = 1.25


def now_utc_text() -> str:
    return datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S")


def to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def ms_to_utc_text(timestamp_ms: Any) -> str | None:
    ts = to_float(timestamp_ms)
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(ts / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def compute_rsi_wilder(closes: list[float], period: int = DEFAULT_RSI_PERIOD) -> float | None:
    if period <= 0 or len(closes) < period + 1:
        return None

    initial_changes = [closes[idx] - closes[idx - 1] for idx in range(1, period + 1)]
    avg_gain = sum(max(change, 0.0) for change in initial_changes) / period
    avg_loss = sum(max(-change, 0.0) for change in initial_changes) / period

    for idx in range(period + 1, len(closes)):
        change = closes[idx] - closes[idx - 1]
        gain = max(change, 0.0)
        loss = max(-change, 0.0)
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period

    if avg_loss == 0:
        if avg_gain == 0:
            return 50.0
        return 100.0

    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def compute_natr_wilder(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    period: int = DEFAULT_NATR_PERIOD,
) -> float | None:
    if period <= 0 or len(closes) < period + 1:
        return None
    if not (len(highs) == len(lows) == len(closes)):
        return None

    true_ranges: list[float] = []
    for idx in range(1, len(closes)):
        high = highs[idx]
        low = lows[idx]
        prev_close = closes[idx - 1]
        true_range = max(high - low, abs(high - prev_close), abs(low - prev_close))
        true_ranges.append(true_range)

    if len(true_ranges) < period:
        return None

    atr = sum(true_ranges[:period]) / period
    for tr in true_ranges[period:]:
        atr = ((atr * (period - 1)) + tr) / period

    last_close = closes[-1]
    if last_close <= 0:
        return None

    return (atr / last_close) * 100.0


def market_matches_mode(market: dict[str, Any], market_mode: str) -> bool:
    if market_mode == "perpetual":
        return bool(market.get("swap"))
    if market_mode == "spot":
        return bool(market.get("spot"))
    return bool(market.get("swap") or market.get("spot"))


def market_type_for_market(market: dict[str, Any]) -> str:
    if market.get("swap"):
        return "perpetual"
    if market.get("spot"):
        return "spot"
    return "other"


@dataclass
class SymbolMeta:
    symbol: str
    market_type: str


class MarketDataLoader:
    """
    Dedicated market data stage:
    - instruments/universe via load_markets
    - tickers via fetch_tickers/fetch_ticker
    - klines via fetch_ohlcv for indicators
    """

    def __init__(self) -> None:
        self._exchanges: dict[str, ccxt.Exchange] = {}
        self._cached_exchange_rows: dict[str, list[dict[str, Any]]] = {}
        self._ticker_fetch_tasks: dict[str, asyncio.Task[tuple[list[dict[str, Any]], str]]] = {}
        self._ticker_fetch_started_ts: dict[str, float] = {}
        self._rsi_cache: dict[str, tuple[float | None, float]] = {}
        self._natr_cache: dict[str, tuple[float | None, float]] = {}
        self._exchange_locks: dict[str, asyncio.Lock] = {}
        self._last_rsi_refresh_ts = 0.0
        self._last_natr_refresh_ts = 0.0

    async def ensure_exchanges(self, exchange_ids: list[str]) -> list[str]:
        errors: list[str] = []
        for exchange_id in exchange_ids:
            if exchange_id in self._exchanges:
                continue
            try:
                exchange_class = getattr(ccxt, exchange_id)
            except AttributeError:
                errors.append(f"{exchange_id}: exchange id not found in ccxt")
                continue

            self._exchanges[exchange_id] = exchange_class(
                {
                    "enableRateLimit": True,
                    "timeout": int(FETCH_TIMEOUT_SEC_MAX * 1000),
                    "options": {"adjustForTimeDifference": True},
                }
            )
            self._exchange_locks[exchange_id] = asyncio.Lock()
        return errors

    async def build_universe(
        self,
        exchange_ids: list[str],
        market_type: str,
        pair_search: str,
        max_pairs_per_exchange: int,
    ) -> tuple[dict[str, list[SymbolMeta]], list[str]]:
        universe: dict[str, list[SymbolMeta]] = {}
        errors = await self.ensure_exchanges(exchange_ids)

        tasks: list[tuple[str, asyncio.Future[tuple[list[SymbolMeta], str]]]] = []
        for exchange_id in exchange_ids:
            exchange = self._exchanges.get(exchange_id)
            if exchange is None:
                continue
            tasks.append(
                (
                    exchange_id,
                    asyncio.to_thread(
                        self._build_exchange_universe,
                        exchange,
                        market_type,
                        pair_search,
                        max_pairs_per_exchange,
                    ),
                )
            )

        if not tasks:
            return universe, errors

        results = await asyncio.gather(*(task for _, task in tasks), return_exceptions=True)
        for index, result in enumerate(results):
            exchange_id = tasks[index][0]
            if isinstance(result, Exception):
                errors.append(f"{exchange_id}: universe build failed ({result})")
                continue
            metas, err = result
            if err:
                errors.append(f"{exchange_id}: {err}")
            if metas:
                universe[exchange_id] = metas

        return universe, errors

    def _build_exchange_universe(
        self,
        exchange: ccxt.Exchange,
        market_type: str,
        pair_search: str,
        max_pairs_per_exchange: int,
    ) -> tuple[list[SymbolMeta], str]:
        try:
            markets = exchange.load_markets()
            normalized_search = pair_search.strip().lower()
            metas: list[SymbolMeta] = []
            for symbol, market in markets.items():
                if not market.get("active", True):
                    continue
                if not market_matches_mode(market, market_type):
                    continue
                if normalized_search and normalized_search not in symbol.lower():
                    continue
                metas.append(SymbolMeta(symbol=symbol, market_type=market_type_for_market(market)))
                if len(metas) >= max_pairs_per_exchange:
                    break
            return metas, ""
        except Exception as exc:
            return [], f"universe build failed ({exc})"

    async def collect_ticker_rows(
        self,
        universe: dict[str, list[SymbolMeta]],
        tick_interval_sec: float,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        rows: list[dict[str, Any]] = []
        errors: list[str] = []
        if not universe:
            return rows, errors

        now_ts = asyncio.get_running_loop().time()
        active_exchanges: set[str] = set()

        for exchange_id, metas in universe.items():
            if not metas:
                continue

            exchange = self._exchanges.get(exchange_id)
            if not exchange:
                continue

            active_exchanges.add(exchange_id)
            symbols = [meta.symbol for meta in metas]
            meta_map = {meta.symbol: meta for meta in metas}
            allowed_ids = {f"{exchange_id}|{symbol}" for symbol in symbols}
            task = self._ticker_fetch_tasks.get(exchange_id)

            if task and task.done():
                try:
                    exchange_rows, exchange_error = task.result()
                except Exception as exc:
                    exchange_rows, exchange_error = [], f"{exchange_id}: ticker fetch worker error ({exc})"

                self._ticker_fetch_tasks.pop(exchange_id, None)
                self._ticker_fetch_started_ts.pop(exchange_id, None)

                if exchange_rows:
                    self._cached_exchange_rows[exchange_id] = exchange_rows
                if exchange_error:
                    errors.append(exchange_error)
                task = None
            elif task and not task.done():
                started_ts = self._ticker_fetch_started_ts.get(exchange_id, now_ts)
                if (now_ts - started_ts) > TICKER_TASK_MAX_AGE_SEC:
                    errors.append(
                        f"{exchange_id}: ticker fetch is taking unusually long ({now_ts - started_ts:.1f}s)"
                    )

            if task is None:
                task = asyncio.create_task(
                    self._fetch_exchange_rows_with_timeout(exchange_id, exchange, symbols, meta_map, tick_interval_sec),
                    name=f"ticker-fetch-{exchange_id}",
                )
                self._ticker_fetch_tasks[exchange_id] = task
                self._ticker_fetch_started_ts[exchange_id] = now_ts

            cached_rows = self._cached_exchange_rows.get(exchange_id, [])
            if not cached_rows and task and not task.done():
                wait_timeout = min(
                    max(tick_interval_sec * 0.5, INITIAL_TICKER_WAIT_MIN_SEC),
                    INITIAL_TICKER_WAIT_MAX_SEC,
                )
                try:
                    await asyncio.wait_for(asyncio.shield(task), timeout=wait_timeout)
                except asyncio.TimeoutError:
                    pass
                except Exception:
                    pass

                if task.done():
                    try:
                        exchange_rows, exchange_error = task.result()
                    except Exception as exc:
                        exchange_rows, exchange_error = [], f"{exchange_id}: ticker fetch worker error ({exc})"

                    self._ticker_fetch_tasks.pop(exchange_id, None)
                    self._ticker_fetch_started_ts.pop(exchange_id, None)
                    if exchange_rows:
                        self._cached_exchange_rows[exchange_id] = exchange_rows
                        cached_rows = exchange_rows
                    if exchange_error:
                        errors.append(exchange_error)

            if cached_rows:
                rows.extend(row for row in cached_rows if str(row.get("id")) in allowed_ids)

        inactive_exchanges = set(self._ticker_fetch_tasks.keys()) - active_exchanges
        for exchange_id in inactive_exchanges:
            task = self._ticker_fetch_tasks.pop(exchange_id, None)
            if task and not task.done():
                task.cancel()
            self._ticker_fetch_started_ts.pop(exchange_id, None)
            self._cached_exchange_rows.pop(exchange_id, None)

        return rows, errors

    async def enrich_rows_with_rsi(
        self,
        rows: list[dict[str, Any]],
        filters: FilterConfig,
        tick_interval_sec: float,
    ) -> list[str]:
        now_ts = asyncio.get_running_loop().time()
        by_id: dict[str, dict[str, Any]] = {str(row.get("id")): row for row in rows if row.get("id")}
        stale_rows: list[dict[str, Any]] = []
        active_rsi_modes = self._active_rsi_modes(filters)

        for row in rows:
            row_id = str(row.get("id") or "")
            if not row_id:
                row["rsi"] = None
                continue

            cache_key = self._rsi_cache_key(row_id, filters)
            cached = self._rsi_cache.get(cache_key)
            if cached and (now_ts - cached[1]) < RSI_CACHE_TTL_SEC:
                row["rsi"] = cached[0]
                continue

            row["rsi"] = cached[0] if cached else None
            stale_rows.append(row)

        refresh_interval_sec = RSI_REFRESH_INTERVAL_SEC
        if active_rsi_modes:
            refresh_interval_sec = max(tick_interval_sec * 2.0, 2.0)
        if (now_ts - self._last_rsi_refresh_ts) < refresh_interval_sec or not stale_rows:
            return []

        self._last_rsi_refresh_ts = now_ts
        stale_rows.sort(key=lambda row: to_float(row.get("quote_volume_24h")) or 0.0, reverse=True)
        target_rows = self._pick_indicator_rows(stale_rows, RSI_RECALC_PER_TICK)
        if not target_rows:
            return []

        tasks = [self._fetch_symbol_rsi_for_row(row, filters) for row in target_rows]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        unique_errors: list[str] = []
        updated_ts = asyncio.get_running_loop().time()
        for result in results:
            if isinstance(result, Exception):
                message = str(result).strip()
                if message and message not in unique_errors:
                    unique_errors.append(message)
                continue

            row_id, cache_key, rsi_value, error_text = result
            self._rsi_cache[cache_key] = (rsi_value, updated_ts)
            if row_id in by_id:
                by_id[row_id]["rsi"] = rsi_value
            if error_text and error_text not in unique_errors:
                unique_errors.append(error_text)

        return unique_errors[:10]

    async def enrich_rows_with_natr(
        self,
        rows: list[dict[str, Any]],
        filters: FilterConfig,
        tick_interval_sec: float,
    ) -> list[str]:
        active_natr_modes = self._active_natr_modes(filters)
        natr_required = bool(active_natr_modes) or bool(filters.natr_enabled)
        if not natr_required:
            for row in rows:
                row["natr"] = None
            return []

        now_ts = asyncio.get_running_loop().time()
        by_id: dict[str, dict[str, Any]] = {str(row.get("id")): row for row in rows if row.get("id")}
        stale_rows: list[dict[str, Any]] = []

        for row in rows:
            row_id = str(row.get("id") or "")
            if not row_id:
                row["natr"] = None
                continue

            cache_key = self._natr_cache_key(row_id, filters)
            cached = self._natr_cache.get(cache_key)
            if cached and (now_ts - cached[1]) < NATR_CACHE_TTL_SEC:
                row["natr"] = cached[0]
                continue

            row["natr"] = cached[0] if cached else None
            stale_rows.append(row)

        refresh_interval_sec = NATR_REFRESH_INTERVAL_SEC
        if active_natr_modes:
            refresh_interval_sec = max(tick_interval_sec * 2.0, 2.0)
        if (now_ts - self._last_natr_refresh_ts) < refresh_interval_sec or not stale_rows:
            return []

        self._last_natr_refresh_ts = now_ts
        stale_rows.sort(key=lambda row: to_float(row.get("quote_volume_24h")) or 0.0, reverse=True)
        target_rows = self._pick_indicator_rows(stale_rows, NATR_RECALC_PER_TICK)
        if not target_rows:
            return []

        tasks = [self._fetch_symbol_natr_for_row(row, filters) for row in target_rows]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        unique_errors: list[str] = []
        updated_ts = asyncio.get_running_loop().time()
        for result in results:
            if isinstance(result, Exception):
                message = str(result).strip()
                if message and message not in unique_errors:
                    unique_errors.append(message)
                continue

            row_id, cache_key, natr_value, error_text = result
            self._natr_cache[cache_key] = (natr_value, updated_ts)
            if row_id in by_id:
                by_id[row_id]["natr"] = natr_value
            if error_text and error_text not in unique_errors:
                unique_errors.append(error_text)

        return unique_errors[:10]

    def _pick_indicator_rows(self, stale_rows: list[dict[str, Any]], per_tick_cap: int) -> list[dict[str, Any]]:
        target_rows: list[dict[str, Any]] = []
        seen_exchanges: set[str] = set()
        for row in stale_rows:
            exchange_id = str(row.get("exchange_id") or "")
            if not exchange_id or exchange_id in seen_exchanges:
                continue
            seen_exchanges.add(exchange_id)
            target_rows.append(row)
            if len(target_rows) >= per_tick_cap:
                break
        return target_rows

    def _active_rsi_modes(self, filters: FilterConfig) -> set[str]:
        configured = set(filters.rsi_modes or [])
        if not configured and filters.rsi_mode != "all":
            configured.add(filters.rsi_mode)
        configured.discard("all")
        return {mode for mode in configured if mode in {"lt30", "gt70", "between30_70"}}

    def _active_rsi_timeframe(self, filters: FilterConfig) -> str:
        return str(filters.rsi_timeframe or DEFAULT_RSI_TIMEFRAME)

    def _active_rsi_period(self, filters: FilterConfig) -> int:
        return max(int(filters.rsi_period or DEFAULT_RSI_PERIOD), 2)

    def _rsi_cache_key(self, row_id: str, filters: FilterConfig) -> str:
        return f"{row_id}|{self._active_rsi_timeframe(filters)}|{self._active_rsi_period(filters)}"

    def _active_natr_modes(self, filters: FilterConfig) -> set[str]:
        configured = set(filters.natr_modes or [])
        return {mode for mode in configured if mode in {"compression", "normal", "high", "extreme"}}

    def _active_natr_timeframe(self, filters: FilterConfig) -> str:
        return str(filters.natr_timeframe or DEFAULT_NATR_TIMEFRAME)

    def _active_natr_period(self, filters: FilterConfig) -> int:
        return max(int(filters.natr_period or DEFAULT_NATR_PERIOD), 2)

    def _natr_cache_key(self, row_id: str, filters: FilterConfig) -> str:
        return f"{row_id}|{self._active_natr_timeframe(filters)}|{self._active_natr_period(filters)}"

    async def _fetch_symbol_rsi_for_row(
        self,
        row: dict[str, Any],
        filters: FilterConfig,
    ) -> tuple[str, str, float | None, str]:
        exchange_id = str(row.get("exchange_id") or "")
        symbol = str(row.get("symbol") or "")
        row_id = str(row.get("id") or "")
        cache_key = self._rsi_cache_key(row_id, filters)
        timeframe = self._active_rsi_timeframe(filters)
        period = self._active_rsi_period(filters)
        if not row_id:
            return "", cache_key, None, "missing row id for RSI"

        exchange = self._exchanges.get(exchange_id)
        lock = self._exchange_locks.get(exchange_id)
        if not exchange or not lock:
            return row_id, cache_key, None, ""

        try:
            async with lock:
                rsi_value = await asyncio.wait_for(
                    asyncio.to_thread(self._fetch_symbol_rsi, exchange, symbol, timeframe, period),
                    timeout=RSI_TIMEOUT_SEC,
                )
            return row_id, cache_key, rsi_value, ""
        except asyncio.TimeoutError:
            return row_id, cache_key, None, ""
        except Exception:
            return row_id, cache_key, None, ""

    def _fetch_symbol_rsi(
        self,
        exchange: ccxt.Exchange,
        symbol: str,
        timeframe: str,
        period: int,
    ) -> float | None:
        if not exchange.has.get("fetchOHLCV"):
            return None

        candles = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=RSI_OHLCV_LIMIT)
        if not candles:
            return None

        closes: list[float] = []
        for candle in candles:
            if not isinstance(candle, list | tuple) or len(candle) < 5:
                continue
            close_price = to_float(candle[4])
            if close_price is None:
                continue
            closes.append(close_price)

        return compute_rsi_wilder(closes, period=period)

    async def _fetch_symbol_natr_for_row(
        self,
        row: dict[str, Any],
        filters: FilterConfig,
    ) -> tuple[str, str, float | None, str]:
        exchange_id = str(row.get("exchange_id") or "")
        symbol = str(row.get("symbol") or "")
        row_id = str(row.get("id") or "")
        cache_key = self._natr_cache_key(row_id, filters)
        timeframe = self._active_natr_timeframe(filters)
        period = self._active_natr_period(filters)
        if not row_id:
            return "", cache_key, None, "missing row id for NATR"

        exchange = self._exchanges.get(exchange_id)
        lock = self._exchange_locks.get(exchange_id)
        if not exchange or not lock:
            return row_id, cache_key, None, ""

        try:
            async with lock:
                natr_value = await asyncio.wait_for(
                    asyncio.to_thread(self._fetch_symbol_natr, exchange, symbol, timeframe, period),
                    timeout=NATR_TIMEOUT_SEC,
                )
            return row_id, cache_key, natr_value, ""
        except asyncio.TimeoutError:
            return row_id, cache_key, None, ""
        except Exception:
            return row_id, cache_key, None, ""

    def _fetch_symbol_natr(
        self,
        exchange: ccxt.Exchange,
        symbol: str,
        timeframe: str,
        period: int,
    ) -> float | None:
        if not exchange.has.get("fetchOHLCV"):
            return None

        candles = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=NATR_OHLCV_LIMIT)
        if not candles:
            return None

        highs: list[float] = []
        lows: list[float] = []
        closes: list[float] = []
        for candle in candles:
            if not isinstance(candle, list | tuple) or len(candle) < 5:
                continue
            high_price = to_float(candle[2])
            low_price = to_float(candle[3])
            close_price = to_float(candle[4])
            if high_price is None or low_price is None or close_price is None:
                continue
            highs.append(high_price)
            lows.append(low_price)
            closes.append(close_price)

        return compute_natr_wilder(highs, lows, closes, period=period)

    async def _fetch_exchange_rows_with_timeout(
        self,
        exchange_id: str,
        exchange: ccxt.Exchange,
        symbols: list[str],
        meta_map: dict[str, SymbolMeta],
        tick_interval_sec: float,
    ) -> tuple[list[dict[str, Any]], str]:
        timeout_sec = min(
            max(max(tick_interval_sec, 0.2) * FETCH_TIMEOUT_MULTIPLIER, FETCH_TIMEOUT_SEC_MIN),
            FETCH_TIMEOUT_SEC_MAX,
        )
        exchange.timeout = int(timeout_sec * 1000)
        try:
            exchange_rows, exchange_error = await asyncio.to_thread(
                self._fetch_exchange_rows,
                exchange_id,
                exchange,
                symbols,
                meta_map,
                tick_interval_sec,
            )
            if exchange_rows:
                self._cached_exchange_rows[exchange_id] = exchange_rows
                return exchange_rows, exchange_error

            cached_rows = self._cached_exchange_rows.get(exchange_id, [])
            if cached_rows:
                stale_msg = f"{exchange_id}: using cached rows after empty fetch"
                return cached_rows, exchange_error if exchange_error else stale_msg
            return exchange_rows, exchange_error
        except Exception as exc:
            cached_rows = self._cached_exchange_rows.get(exchange_id, [])
            message = f"{exchange_id}: ticker fetch worker error ({exc})"
            if cached_rows:
                message = f"{message}; using cached rows"
            return cached_rows, message

    def _fetch_exchange_rows(
        self,
        exchange_id: str,
        exchange: ccxt.Exchange,
        symbols: list[str],
        meta_map: dict[str, SymbolMeta],
        tick_interval_sec: float,
    ) -> tuple[list[dict[str, Any]], str]:
        tickers: dict[str, Any] = {}
        fallback_budget_sec = max(
            max(tick_interval_sec, 0.2) * FALLBACK_FETCH_TICKER_BUDGET_MULTIPLIER,
            FALLBACK_FETCH_TICKER_BUDGET_MIN_SEC,
        )
        fallback_started_ts = time.monotonic()
        try:
            if exchange.has.get("fetchTickers"):
                try:
                    tickers = exchange.fetch_tickers(symbols)
                except Exception as first_error:
                    if exchange.has.get("fetchTicker"):
                        for symbol in symbols[:FALLBACK_FETCH_TICKER_LIMIT]:
                            if (time.monotonic() - fallback_started_ts) >= fallback_budget_sec:
                                break
                            try:
                                ticker_payload = exchange.fetch_ticker(symbol)
                                if isinstance(ticker_payload, dict):
                                    tickers[symbol] = ticker_payload
                            except Exception:
                                continue

                    if not tickers:
                        try:
                            tickers = exchange.fetch_tickers()
                        except Exception as second_error:
                            return [], (
                                f"{exchange_id}: ticker fetch failed "
                                f"(symbol call: {first_error}; full call: {second_error})"
                            )
            elif exchange.has.get("fetchTicker"):
                for symbol in symbols[:FALLBACK_FETCH_TICKER_LIMIT]:
                    if (time.monotonic() - fallback_started_ts) >= fallback_budget_sec:
                        break
                    try:
                        tickers[symbol] = exchange.fetch_ticker(symbol)
                    except Exception:
                        continue
            else:
                return [], f"{exchange_id}: no ticker endpoint available"
        except Exception as exc:
            return [], f"{exchange_id}: ticker fetch failed ({exc})"

        rows: list[dict[str, Any]] = []
        for symbol in symbols:
            payload = tickers.get(symbol)
            if not isinstance(payload, dict):
                continue

            last_price = to_float(payload.get("last"))
            if last_price is None:
                last_price = to_float(payload.get("close"))
            if last_price is None:
                continue

            bid = to_float(payload.get("bid"))
            ask = to_float(payload.get("ask"))
            pct_change_24h = to_float(payload.get("percentage"))
            quote_volume_24h = to_float(payload.get("quoteVolume"))
            base_volume_24h = to_float(payload.get("baseVolume"))
            if quote_volume_24h is None and base_volume_24h is not None:
                quote_volume_24h = base_volume_24h * last_price
            ticker_timestamp_utc = ms_to_utc_text(payload.get("timestamp")) or now_utc_text()

            market_type = meta_map.get(symbol).market_type if symbol in meta_map else "other"
            row_id = f"{exchange_id}|{symbol}"
            rows.append(
                {
                    "id": row_id,
                    "exchange_id": exchange_id,
                    "symbol": symbol,
                    "market_type": market_type,
                    "last_price": last_price,
                    "bid": bid,
                    "ask": ask,
                    "pct_change_24h": pct_change_24h,
                    "quote_volume_24h": quote_volume_24h,
                    "base_volume_24h": base_volume_24h,
                    "rsi": None,
                    "natr": None,
                    "ticker_timestamp_utc": ticker_timestamp_utc,
                }
            )
        return rows, ""

    async def close(self) -> None:
        for task in self._ticker_fetch_tasks.values():
            if not task.done():
                task.cancel()
        self._ticker_fetch_tasks.clear()
        self._ticker_fetch_started_ts.clear()
        for exchange in self._exchanges.values():
            close_method = getattr(exchange, "close", None)
            if callable(close_method):
                try:
                    await asyncio.to_thread(close_method)
                except Exception:
                    pass
        self._exchanges.clear()
        self._cached_exchange_rows.clear()
        self._rsi_cache.clear()
        self._natr_cache.clear()
        self._exchange_locks.clear()
