from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from .market_data_loader import MarketDataLoader, to_float
from .models import FilterConfig, StartPayload

UPDATE_FIELDS = (
    "last_price",
    "bid",
    "ask",
    "pct_change_24h",
    "quote_volume_24h",
    "base_volume_24h",
    "rsi",
    "natr",
    "ticker_timestamp_utc",
)


def now_utc_text() -> str:
    return datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S")


class LiveStateEngine:
    """
    Scanner stage:
    - requests market data from MarketDataLoader
    - applies screener filters
    - emits stable diff updates for the UI
    """

    def __init__(self, payload: StartPayload, out_queue: asyncio.Queue[dict[str, Any]]) -> None:
        self.payload = payload
        self.out_queue = out_queue
        self.filters = payload.filters
        self.loader = MarketDataLoader()
        self._stop = asyncio.Event()
        self._paused = False
        self._task: asyncio.Task | None = None
        self._universe: dict[str, list[Any]] = {}
        self._visible_rows: dict[str, dict[str, Any]] = {}
        self._last_universe_refresh_ts = 0.0
        self._pending_universe_errors: list[str] = []

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="live-state-engine")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await self._task
        await self.loader.close()

    def pause(self) -> None:
        self._paused = True

    def resume(self) -> None:
        self._paused = False

    def update_filters(self, filters: FilterConfig) -> None:
        self.filters = filters

    async def _run(self) -> None:
        await self._push_status(
            state="starting",
            details={
                "exchanges": self.payload.exchanges,
                "market_type": self.payload.market_type,
                "tick_interval_sec": self.payload.tick_interval_sec,
            },
        )

        while not self._stop.is_set():
            tick_started_ts = asyncio.get_running_loop().time()
            try:
                if self._paused:
                    await asyncio.sleep(0.25)
                    continue

                await self._maybe_refresh_universe()
                rows, fetch_errors = await self.loader.collect_ticker_rows(
                    self._universe,
                    self.payload.tick_interval_sec,
                )

                rsi_errors: list[str] = []
                natr_errors: list[str] = []
                if rows:
                    rsi_errors = await self.loader.enrich_rows_with_rsi(
                        rows,
                        self.filters,
                        self.payload.tick_interval_sec,
                    )
                    natr_errors = await self.loader.enrich_rows_with_natr(
                        rows,
                        self.filters,
                        self.payload.tick_interval_sec,
                    )

                next_visible = self._apply_filters(rows)
                added, updated, removed = self._build_diff(next_visible)
                self._visible_rows = next_visible

                errors_for_tick = (self._pending_universe_errors + fetch_errors + rsi_errors + natr_errors)[:20]
                self._pending_universe_errors = []

                await self.out_queue.put(
                    {
                        "type": "diff",
                        "server_time_utc": now_utc_text(),
                        "added": added,
                        "updated": updated,
                        "removed": removed,
                        "stats": {
                            "visible_count": len(next_visible),
                            "tracked_rows": len(rows),
                            "added_count": len(added),
                            "updated_count": len(updated),
                            "removed_count": len(removed),
                            "paused": self._paused,
                        },
                        "errors": errors_for_tick,
                    }
                )
                elapsed = asyncio.get_running_loop().time() - tick_started_ts
                sleep_for = max(max(self.payload.tick_interval_sec, 0.2) - elapsed, 0.05)
                await asyncio.sleep(sleep_for)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                await self.out_queue.put(
                    {
                        "type": "error",
                        "server_time_utc": now_utc_text(),
                        "message": f"engine loop error: {exc}",
                    }
                )
                await asyncio.sleep(1.0)

        await self._push_status(state="stopped", details={})

    async def _push_status(self, state: str, details: dict[str, Any]) -> None:
        await self.out_queue.put(
            {
                "type": "status",
                "state": state,
                "server_time_utc": now_utc_text(),
                "details": details,
            }
        )

    async def _maybe_refresh_universe(self) -> None:
        now_ts = asyncio.get_running_loop().time()
        refresh_due = (
            (now_ts - self._last_universe_refresh_ts) >= self.payload.universe_refresh_sec
            if self._last_universe_refresh_ts > 0
            else True
        )
        if not refresh_due:
            return

        new_universe, universe_errors = await self.loader.build_universe(
            exchange_ids=self.payload.exchanges,
            market_type=self.payload.market_type,
            pair_search=self.payload.pair_search,
            max_pairs_per_exchange=self.payload.max_pairs_per_exchange,
        )
        self._universe = new_universe
        self._pending_universe_errors = list(universe_errors)
        self._last_universe_refresh_ts = now_ts
        await self._push_status(
            state="universe_refreshed",
            details={
                "exchange_count": len(self._universe),
                "symbol_count": sum(len(items) for items in self._universe.values()),
            },
        )

    def _apply_filters(self, rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        symbol_contains = self.filters.symbol_contains.strip().lower()
        rsi_modes = self._active_rsi_modes()
        natr_modes = self._active_natr_modes()

        for row in rows:
            if symbol_contains and symbol_contains not in row["symbol"].lower():
                continue

            quote_volume = to_float(row.get("quote_volume_24h")) or 0.0
            if quote_volume < self.filters.min_quote_volume:
                continue
            if self.filters.max_quote_volume > 0 and quote_volume > self.filters.max_quote_volume:
                continue

            pct = to_float(row.get("pct_change_24h")) or 0.0
            if pct < self.filters.min_pct_change or pct > self.filters.max_pct_change:
                continue

            if rsi_modes:
                rsi_value = to_float(row.get("rsi"))
                if rsi_value is None:
                    continue
                matches_rsi = (
                    ("lt30" in rsi_modes and rsi_value < 30.0)
                    or ("gt70" in rsi_modes and rsi_value > 70.0)
                    or ("between30_70" in rsi_modes and 30.0 <= rsi_value <= 70.0)
                )
                if not matches_rsi:
                    continue

            if natr_modes:
                natr_value = to_float(row.get("natr"))
                if natr_value is None:
                    continue
                matches_natr = (
                    ("compression" in natr_modes and natr_value < 0.8)
                    or ("normal" in natr_modes and 0.8 <= natr_value <= 1.8)
                    or ("high" in natr_modes and natr_value > 1.8)
                    or ("extreme" in natr_modes and natr_value > 3.5)
                )
                if not matches_natr:
                    continue

            out[row["id"]] = row

        return out

    def _active_rsi_modes(self) -> set[str]:
        configured = set(self.filters.rsi_modes or [])
        if not configured and self.filters.rsi_mode != "all":
            configured.add(self.filters.rsi_mode)
        configured.discard("all")
        return {mode for mode in configured if mode in {"lt30", "gt70", "between30_70"}}

    def _active_natr_modes(self) -> set[str]:
        configured = set(self.filters.natr_modes or [])
        return {mode for mode in configured if mode in {"compression", "normal", "high", "extreme"}}

    def _build_diff(
        self,
        next_visible: dict[str, dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
        prev_ids = set(self._visible_rows.keys())
        next_ids = set(next_visible.keys())

        added_ids = next_ids - prev_ids
        removed_ids = prev_ids - next_ids
        common_ids = prev_ids & next_ids

        added = [next_visible[row_id] for row_id in added_ids]
        removed = sorted(removed_ids)

        updated: list[dict[str, Any]] = []
        for row_id in common_ids:
            old = self._visible_rows[row_id]
            new = next_visible[row_id]
            if any(old.get(field) != new.get(field) for field in UPDATE_FIELDS):
                updated.append(new)

        return added, updated, removed
