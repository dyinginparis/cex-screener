from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import altair as alt
import ccxt
import pandas as pd
import streamlit as st

DEFAULT_EXCHANGES = ["binanceusdm", "bybit", "krakenfutures", "mexc"]
MOVE_TIMEFRAMES = ["1m", "5m", "15m", "1h", "4h", "1d"]
FALLBACK_FETCH_TICKER_LIMIT = 80


def build_exchange(exchange_id: str) -> ccxt.Exchange:
    exchange_class = getattr(ccxt, exchange_id)
    return exchange_class(
        {
            "enableRateLimit": True,
            "timeout": 20000,
            "options": {"adjustForTimeDifference": True},
        }
    )


def close_exchange(exchange: ccxt.Exchange) -> None:
    close_method = getattr(exchange, "close", None)
    if callable(close_method):
        try:
            close_method()
        except Exception:
            pass


def to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def ms_to_utc_text(timestamp_ms: Any) -> str | None:
    ts_float = to_float(timestamp_ms)
    if ts_float is None:
        return None
    try:
        return datetime.fromtimestamp(ts_float / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def market_type_for_market(market: dict[str, Any]) -> str:
    if market.get("swap"):
        return "Perpetual"
    if market.get("spot"):
        return "Spot"
    return "Other"


def market_matches_mode(market: dict[str, Any], market_mode: str) -> bool:
    if market_mode == "Perpetual":
        return bool(market.get("swap"))
    if market_mode == "Spot":
        return bool(market.get("spot"))
    if market_mode == "Both":
        return bool(market.get("swap") or market.get("spot"))
    return False


def parse_funding_map(raw_funding: Any) -> dict[str, dict[str, Any]]:
    if isinstance(raw_funding, dict):
        return {
            symbol: payload
            for symbol, payload in raw_funding.items()
            if isinstance(symbol, str) and isinstance(payload, dict)
        }
    if isinstance(raw_funding, list):
        out: dict[str, dict[str, Any]] = {}
        for item in raw_funding:
            if isinstance(item, dict) and item.get("symbol"):
                out[item["symbol"]] = item
        return out
    return {}


@st.cache_data(ttl=5, show_spinner=False)
def load_exchange_snapshot(
    exchange_id: str,
    market_mode: str,
    pair_search: str,
    max_pairs_per_exchange: int,
) -> tuple[pd.DataFrame, dict[str, bool], str]:
    exchange = build_exchange(exchange_id)
    capabilities = {
        "fetch_tickers": bool(exchange.has.get("fetchTickers")),
        "fetch_ticker": bool(exchange.has.get("fetchTicker")),
        "fetch_funding_rates": bool(exchange.has.get("fetchFundingRates")),
        "fetch_ohlcv": bool(exchange.has.get("fetchOHLCV")),
        "fetch_order_book": bool(exchange.has.get("fetchOrderBook")),
        "fetch_liquidations": bool(exchange.has.get("fetchLiquidations")),
    }
    try:
        markets = exchange.load_markets()
        search = pair_search.strip().lower()
        candidate_symbols: list[str] = []
        for symbol, market in markets.items():
            if not market.get("active", True):
                continue
            if not market_matches_mode(market, market_mode):
                continue
            if search and search not in symbol.lower():
                continue
            candidate_symbols.append(symbol)

        candidate_symbols = sorted(candidate_symbols)
        if max_pairs_per_exchange > 0:
            candidate_symbols = candidate_symbols[:max_pairs_per_exchange]

        if not candidate_symbols:
            return pd.DataFrame(), capabilities, ""

        tickers: dict[str, Any] = {}
        ticker_warning = ""
        if capabilities["fetch_tickers"]:
            try:
                raw_tickers = exchange.fetch_tickers(candidate_symbols)
                if isinstance(raw_tickers, dict):
                    tickers = raw_tickers
            except Exception:
                try:
                    raw_tickers = exchange.fetch_tickers()
                    if isinstance(raw_tickers, dict):
                        tickers = raw_tickers
                except Exception as exc:
                    ticker_warning = f"{exchange_id}: fetch_tickers failed ({exc})"

        if not tickers and capabilities["fetch_ticker"]:
            for symbol in candidate_symbols[:FALLBACK_FETCH_TICKER_LIMIT]:
                try:
                    tickers[symbol] = exchange.fetch_ticker(symbol)
                except Exception:
                    continue

        funding_map: dict[str, dict[str, Any]] = {}
        perp_symbols = [
            symbol for symbol in candidate_symbols if markets.get(symbol, {}).get("swap")
        ]
        if capabilities["fetch_funding_rates"] and perp_symbols:
            try:
                raw_funding = exchange.fetch_funding_rates(perp_symbols)
                funding_map = parse_funding_map(raw_funding)
            except Exception:
                try:
                    raw_funding = exchange.fetch_funding_rates()
                    funding_map = parse_funding_map(raw_funding)
                except Exception:
                    funding_map = {}

        rows: list[dict[str, Any]] = []
        for symbol in candidate_symbols:
            market = markets.get(symbol, {})
            ticker = tickers.get(symbol, {})
            if not isinstance(ticker, dict):
                ticker = {}

            last_price = to_float(ticker.get("last"))
            if last_price is None:
                last_price = to_float(ticker.get("close"))

            bid = to_float(ticker.get("bid"))
            ask = to_float(ticker.get("ask"))
            pct_change_24h = to_float(ticker.get("percentage"))
            quote_volume_24h = to_float(ticker.get("quoteVolume"))
            base_volume_24h = to_float(ticker.get("baseVolume"))

            if quote_volume_24h is None and base_volume_24h is not None and last_price is not None:
                quote_volume_24h = base_volume_24h * last_price

            market_type = market_type_for_market(market)
            funding_rate: float | None = None
            next_funding_utc: str | None = None
            if market_type == "Perpetual":
                funding_payload = funding_map.get(symbol, {})
                funding_rate = to_float(funding_payload.get("fundingRate"))
                next_funding_utc = ms_to_utc_text(funding_payload.get("nextFundingTimestamp"))

            if last_price is None and bid is None and ask is None:
                continue

            rows.append(
                {
                    "exchange_id": exchange_id,
                    "symbol": symbol,
                    "base": market.get("base"),
                    "quote": market.get("quote"),
                    "market_type": market_type,
                    "last_price": last_price,
                    "bid": bid,
                    "ask": ask,
                    "pct_change_24h": pct_change_24h,
                    "quote_volume_24h": quote_volume_24h,
                    "base_volume_24h": base_volume_24h,
                    "funding_rate": funding_rate,
                    "next_funding_utc": next_funding_utc,
                    "ticker_timestamp_utc": ms_to_utc_text(ticker.get("timestamp")),
                }
            )

        return pd.DataFrame(rows), capabilities, ticker_warning
    except Exception as exc:
        return pd.DataFrame(), capabilities, f"{exchange_id}: {exc}"
    finally:
        close_exchange(exchange)


def prepare_numeric_columns(data: pd.DataFrame) -> pd.DataFrame:
    out = data.copy()
    numeric_columns = [
        "last_price",
        "bid",
        "ask",
        "pct_change_24h",
        "quote_volume_24h",
        "base_volume_24h",
        "funding_rate",
    ]
    for column in numeric_columns:
        out[column] = pd.to_numeric(out[column], errors="coerce")
    return out


def apply_base_filters(
    data: pd.DataFrame,
    symbol_query: str,
    min_quote_volume: float,
    max_quote_volume: float,
    pct_change_min: float,
    pct_change_max: float,
    funding_direction: str,
) -> pd.DataFrame:
    out = data.copy()

    if symbol_query.strip():
        out = out[out["symbol"].str.contains(symbol_query.strip(), case=False, na=False)]

    out = out[out["quote_volume_24h"].fillna(0) >= float(min_quote_volume)]
    if max_quote_volume > 0:
        out = out[out["quote_volume_24h"].fillna(0) <= float(max_quote_volume)]

    out = out[
        out["pct_change_24h"].fillna(0).between(float(pct_change_min), float(pct_change_max))
    ]

    if funding_direction == "Positive":
        out = out[out["funding_rate"] > 0]
    elif funding_direction == "Negative":
        out = out[out["funding_rate"] < 0]
    elif funding_direction == "Unavailable":
        out = out[out["funding_rate"].isna()]

    return out


def hour_in_window(hour: int, start_hour: int, end_hour: int) -> bool:
    if start_hour <= end_hour:
        return start_hour <= hour <= end_hour
    return hour >= start_hour or hour <= end_hour


@st.cache_data(ttl=600, show_spinner=False)
def compute_utc_slice_quote_volume(
    exchange_id: str,
    symbol: str,
    start_hour_utc: int,
    end_hour_utc: int,
    lookback_days: int,
) -> float | None:
    exchange = build_exchange(exchange_id)
    try:
        if not exchange.has.get("fetchOHLCV"):
            return None

        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        since_ms = now_ms - (lookback_days * 24 * 60 * 60 * 1000)
        limit = min(lookback_days * 24 + 4, 1000)

        candles = exchange.fetch_ohlcv(symbol, timeframe="1h", since=since_ms, limit=limit)
        total_quote_volume = 0.0
        matches = 0
        for candle in candles:
            if len(candle) < 6:
                continue
            timestamp_ms, _open, _high, _low, close, base_volume = candle[:6]
            close_f = to_float(close)
            base_f = to_float(base_volume)
            if close_f is None or base_f is None:
                continue
            hour_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).hour
            if not hour_in_window(hour_utc, start_hour_utc, end_hour_utc):
                continue
            total_quote_volume += close_f * base_f
            matches += 1

        if matches == 0:
            return None
        return total_quote_volume
    except Exception:
        return None
    finally:
        close_exchange(exchange)


@st.cache_data(ttl=120, show_spinner=False)
def compute_custom_pct_change(
    exchange_id: str,
    symbol: str,
    timeframe: str,
    candles_back: int,
) -> float | None:
    exchange = build_exchange(exchange_id)
    try:
        if not exchange.has.get("fetchOHLCV"):
            return None

        limit = max(2, int(candles_back) + 1)
        candles = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        if len(candles) < 2:
            return None

        first_close = to_float(candles[0][4])
        last_close = to_float(candles[-1][4])
        if first_close is None or last_close is None or first_close == 0:
            return None

        return ((last_close - first_close) / first_close) * 100
    except Exception:
        return None
    finally:
        close_exchange(exchange)


@st.cache_data(ttl=8, show_spinner=False)
def fetch_orderbook_snapshot(exchange_id: str, symbol: str, depth_levels: int) -> dict[str, float | None]:
    exchange = build_exchange(exchange_id)
    try:
        if not exchange.has.get("fetchOrderBook"):
            return {}

        orderbook = exchange.fetch_order_book(symbol, limit=depth_levels)
        bids = orderbook.get("bids", [])
        asks = orderbook.get("asks", [])

        best_bid = to_float(bids[0][0]) if bids else None
        best_ask = to_float(asks[0][0]) if asks else None
        spread_abs = None
        spread_pct = None
        mid_price = None
        if best_bid is not None and best_ask is not None and best_ask > 0:
            spread_abs = best_ask - best_bid
            spread_pct = (spread_abs / best_ask) * 100
            mid_price = (best_bid + best_ask) / 2

        bid_depth_quote = 0.0
        for level in bids[:depth_levels]:
            if len(level) < 2:
                continue
            price = to_float(level[0])
            amount = to_float(level[1])
            if price is None or amount is None:
                continue
            bid_depth_quote += price * amount

        ask_depth_quote = 0.0
        for level in asks[:depth_levels]:
            if len(level) < 2:
                continue
            price = to_float(level[0])
            amount = to_float(level[1])
            if price is None or amount is None:
                continue
            ask_depth_quote += price * amount

        imbalance = None
        total_depth = bid_depth_quote + ask_depth_quote
        if total_depth > 0:
            imbalance = (bid_depth_quote - ask_depth_quote) / total_depth

        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid_price": mid_price,
            "spread_abs": spread_abs,
            "spread_pct": spread_pct,
            "bid_depth_quote": bid_depth_quote,
            "ask_depth_quote": ask_depth_quote,
            "depth_imbalance": imbalance,
        }
    except Exception:
        return {}
    finally:
        close_exchange(exchange)


@st.cache_data(ttl=8, show_spinner=False)
def fetch_orderbook_levels(exchange_id: str, symbol: str, depth_levels: int) -> pd.DataFrame:
    exchange = build_exchange(exchange_id)
    try:
        if not exchange.has.get("fetchOrderBook"):
            return pd.DataFrame()

        orderbook = exchange.fetch_order_book(symbol, limit=depth_levels)
        bids = orderbook.get("bids", [])
        asks = orderbook.get("asks", [])
        best_bid = to_float(bids[0][0]) if bids else None
        best_ask = to_float(asks[0][0]) if asks else None
        mid = None
        if best_bid is not None and best_ask is not None:
            mid = (best_bid + best_ask) / 2

        rows: list[dict[str, float | str | None]] = []
        for side, levels in (("bid", bids), ("ask", asks)):
            for level in levels[:depth_levels]:
                if len(level) < 2:
                    continue
                price = to_float(level[0])
                amount = to_float(level[1])
                if price is None or amount is None:
                    continue
                quote = price * amount
                distance_bps = None
                if mid is not None and mid > 0:
                    distance_bps = ((price - mid) / mid) * 10000
                rows.append(
                    {
                        "side": side,
                        "price": price,
                        "amount": amount,
                        "quote": quote,
                        "distance_bps": distance_bps,
                    }
                )
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()
    finally:
        close_exchange(exchange)


def build_heatmap_frame(
    orderbook_levels: pd.DataFrame,
    bucket_size_bps: float,
    max_distance_bps: float,
) -> pd.DataFrame:
    if orderbook_levels.empty:
        return pd.DataFrame()

    work = orderbook_levels.copy()
    work = work.dropna(subset=["distance_bps", "quote"])
    work = work[work["distance_bps"].abs() <= max_distance_bps]
    if work.empty:
        return pd.DataFrame()

    work["bucket_bps"] = (work["distance_bps"] / bucket_size_bps).round() * bucket_size_bps
    agg = work.groupby(["side", "bucket_bps"], as_index=False)["quote"].sum()
    agg["quote"] = pd.to_numeric(agg["quote"], errors="coerce")
    return agg


@st.cache_data(ttl=20, show_spinner=False)
def fetch_recent_liquidations(
    exchange_id: str,
    symbol: str,
    since_ms: int,
    limit: int,
) -> tuple[pd.DataFrame, str]:
    exchange = build_exchange(exchange_id)
    try:
        if not exchange.has.get("fetchLiquidations"):
            return pd.DataFrame(), f"{exchange_id}: fetchLiquidations not supported"

        raw = exchange.fetch_liquidations(symbol, since_ms, limit)
        if not isinstance(raw, list):
            return pd.DataFrame(), ""

        rows: list[dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            timestamp_ms = item.get("timestamp")
            price = to_float(item.get("price"))
            amount = to_float(item.get("amount"))
            notional = to_float(item.get("cost"))
            if notional is None and price is not None and amount is not None:
                notional = price * amount
            rows.append(
                {
                    "exchange_id": exchange_id,
                    "symbol": item.get("symbol") or symbol,
                    "timestamp_ms": timestamp_ms,
                    "timestamp_utc": ms_to_utc_text(timestamp_ms),
                    "side": item.get("side"),
                    "price": price,
                    "amount": amount,
                    "notional": notional,
                }
            )
        return pd.DataFrame(rows), ""
    except Exception as exc:
        return pd.DataFrame(), f"{exchange_id} {symbol}: {exc}"
    finally:
        close_exchange(exchange)


def build_arbitrage_table(
    data: pd.DataFrame,
    min_spread_pct: float,
    trade_size_usd: float,
) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame()

    opportunities: list[dict[str, Any]] = []
    grouped = data.groupby(["market_type", "symbol"], sort=False)

    for (market_type, symbol), group in grouped:
        asks = group[group["ask"] > 0].copy()
        bids = group[group["bid"] > 0].copy()
        if asks.empty or bids.empty:
            continue

        asks = asks.sort_values("ask", ascending=True)
        bids = bids.sort_values("bid", ascending=False)

        chosen_buy = None
        chosen_sell = None
        for ask_row in asks.itertuples(index=False):
            for bid_row in bids.itertuples(index=False):
                if ask_row.exchange_id == bid_row.exchange_id:
                    continue
                chosen_buy = ask_row
                chosen_sell = bid_row
                break
            if chosen_buy is not None:
                break

        if chosen_buy is None or chosen_sell is None:
            continue

        buy_ask = to_float(chosen_buy.ask)
        sell_bid = to_float(chosen_sell.bid)
        if buy_ask is None or sell_bid is None or buy_ask <= 0:
            continue

        spread_pct = ((sell_bid - buy_ask) / buy_ask) * 100
        if spread_pct < min_spread_pct:
            continue

        gross_usd = (trade_size_usd * spread_pct) / 100
        opportunities.append(
            {
                "market_type": market_type,
                "symbol": symbol,
                "buy_exchange": chosen_buy.exchange_id,
                "buy_ask": buy_ask,
                "sell_exchange": chosen_sell.exchange_id,
                "sell_bid": sell_bid,
                "spread_pct": spread_pct,
                "trade_size_usd": trade_size_usd,
                "gross_usd": gross_usd,
            }
        )

    if not opportunities:
        return pd.DataFrame()

    out = pd.DataFrame(opportunities)
    out = out.sort_values("spread_pct", ascending=False, na_position="last")
    return out


def build_pair_alerts(
    data: pd.DataFrame,
    enable_move_alerts: bool,
    long_move_threshold: float,
    short_move_threshold: float,
    enable_funding_alerts: bool,
    positive_funding_threshold: float,
    negative_funding_threshold: float,
    enable_utc_volume_alerts: bool,
    utc_volume_threshold: float,
) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for row in data.itertuples(index=False):
        reasons: list[str] = []

        custom_move = to_float(getattr(row, "custom_pct_change", None))
        if enable_move_alerts and custom_move is not None:
            if custom_move >= long_move_threshold:
                reasons.append(f"Move >= {long_move_threshold:.2f}%")
            if custom_move <= -abs(short_move_threshold):
                reasons.append(f"Move <= -{abs(short_move_threshold):.2f}%")

        funding_rate = to_float(getattr(row, "funding_rate", None))
        if enable_funding_alerts and funding_rate is not None:
            if funding_rate >= positive_funding_threshold:
                reasons.append(f"Funding >= {positive_funding_threshold:.4f}")
            if funding_rate <= negative_funding_threshold:
                reasons.append(f"Funding <= {negative_funding_threshold:.4f}")

        utc_volume = to_float(getattr(row, "utc_slice_quote_volume", None))
        if enable_utc_volume_alerts and utc_volume is not None:
            if utc_volume >= utc_volume_threshold:
                reasons.append(f"UTC slice vol >= {utc_volume_threshold:,.0f}")

        if reasons:
            rows.append(
                {
                    "type": "Pair Alert",
                    "exchange_id": getattr(row, "exchange_id", None),
                    "market_type": getattr(row, "market_type", None),
                    "symbol": getattr(row, "symbol", None),
                    "last_price": getattr(row, "last_price", None),
                    "quote_volume_24h": getattr(row, "quote_volume_24h", None),
                    "custom_pct_change": custom_move,
                    "funding_rate": funding_rate,
                    "utc_slice_quote_volume": utc_volume,
                    "reasons": " | ".join(reasons),
                }
            )

    return pd.DataFrame(rows)


def render_orderbook_tab(
    data: pd.DataFrame,
    exchange_capabilities: dict[str, dict[str, bool]],
    enabled: bool,
    top_pairs: int,
    depth_levels: int,
) -> None:
    st.subheader("Orderbook Snapshot")
    if not enabled:
        st.info("Enable `Orderbook section` in the sidebar.")
        return
    if data.empty:
        st.warning("No pairs available after filters.")
        return

    candidates = data.head(top_pairs).copy()
    records: list[dict[str, Any]] = []
    progress = st.progress(0.0)
    for idx, row in enumerate(candidates.itertuples(index=False), start=1):
        caps = exchange_capabilities.get(row.exchange_id, {})
        if not caps.get("fetch_order_book", False):
            progress.progress(idx / len(candidates))
            continue
        snapshot = fetch_orderbook_snapshot(row.exchange_id, row.symbol, depth_levels)
        if snapshot:
            records.append(
                {
                    "exchange_id": row.exchange_id,
                    "market_type": row.market_type,
                    "symbol": row.symbol,
                    "best_bid": snapshot.get("best_bid"),
                    "best_ask": snapshot.get("best_ask"),
                    "spread_abs": snapshot.get("spread_abs"),
                    "spread_pct": snapshot.get("spread_pct"),
                    "bid_depth_quote": snapshot.get("bid_depth_quote"),
                    "ask_depth_quote": snapshot.get("ask_depth_quote"),
                    "depth_imbalance": snapshot.get("depth_imbalance"),
                }
            )
        progress.progress(idx / len(candidates))
    progress.empty()

    if not records:
        st.warning("No orderbook data returned. Try fewer exchanges or fewer pairs.")
        return

    orderbook_df = pd.DataFrame(records).sort_values(
        "spread_pct", ascending=True, na_position="last"
    )
    st.dataframe(
        orderbook_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "best_bid": st.column_config.NumberColumn(format="%.8f"),
            "best_ask": st.column_config.NumberColumn(format="%.8f"),
            "spread_abs": st.column_config.NumberColumn(format="%.8f"),
            "spread_pct": st.column_config.NumberColumn(format="%.4f"),
            "bid_depth_quote": st.column_config.NumberColumn(format="%.2f"),
            "ask_depth_quote": st.column_config.NumberColumn(format="%.2f"),
            "depth_imbalance": st.column_config.NumberColumn(format="%.4f"),
        },
    )

    st.subheader("Orderbook Heatmap")
    options = [
        f"{row.exchange_id} | {row.symbol}" for row in orderbook_df.itertuples(index=False)
    ]
    selected_pair = st.selectbox("Select pair for heatmap", options=options, index=0)
    max_distance_bps = st.slider("Max distance from mid (bps)", 20, 1500, 250, step=10)
    bucket_size_bps = st.slider("Bucket size (bps)", 1, 100, 10)

    selected_exchange, selected_symbol = selected_pair.split(" | ", 1)
    levels = fetch_orderbook_levels(selected_exchange, selected_symbol, depth_levels)
    if levels.empty:
        st.warning("No levels available for this pair.")
        return

    heatmap_df = build_heatmap_frame(
        levels,
        bucket_size_bps=float(bucket_size_bps),
        max_distance_bps=float(max_distance_bps),
    )
    if heatmap_df.empty:
        st.warning("No levels inside selected heatmap distance window.")
        return

    chart = (
        alt.Chart(heatmap_df)
        .mark_rect()
        .encode(
            x=alt.X("bucket_bps:Q", title="Distance From Mid (bps)"),
            y=alt.Y("side:N", title="Side"),
            color=alt.Color("quote:Q", title="Quote Volume"),
            tooltip=[
                alt.Tooltip("side:N", title="Side"),
                alt.Tooltip("bucket_bps:Q", title="Bucket (bps)", format=".2f"),
                alt.Tooltip("quote:Q", title="Quote Vol", format=",.2f"),
            ],
        )
        .properties(height=180)
    )
    st.altair_chart(chart, use_container_width=True)


def render_liquidation_tab(
    data: pd.DataFrame,
    exchange_capabilities: dict[str, dict[str, bool]],
    enabled: bool,
    top_pairs: int,
    window_minutes: int,
    limit_per_pair: int,
) -> None:
    st.subheader("Liquidation Tracker")
    if not enabled:
        st.info("Enable `Liquidation section` in the sidebar.")
        return

    perp_data = data[data["market_type"] == "Perpetual"].copy()
    if perp_data.empty:
        st.warning("No perpetual pairs available for liquidation tracking.")
        return

    candidates = perp_data.head(top_pairs).copy()
    since_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000) - (window_minutes * 60 * 1000)
    frames: list[pd.DataFrame] = []
    errors: list[str] = []
    skipped: list[str] = []
    progress = st.progress(0.0)

    for idx, row in enumerate(candidates.itertuples(index=False), start=1):
        caps = exchange_capabilities.get(row.exchange_id, {})
        if not caps.get("fetch_liquidations", False):
            skipped.append(row.exchange_id)
            progress.progress(idx / len(candidates))
            continue

        liquidation_df, error_text = fetch_recent_liquidations(
            exchange_id=row.exchange_id,
            symbol=row.symbol,
            since_ms=since_ms,
            limit=limit_per_pair,
        )
        if error_text:
            errors.append(error_text)
        if not liquidation_df.empty:
            frames.append(liquidation_df)
        progress.progress(idx / len(candidates))
    progress.empty()

    if skipped:
        unique_skipped = ", ".join(sorted(set(skipped)))
        st.caption(f"Exchanges without unified liquidation endpoint: {unique_skipped}")

    if errors:
        with st.expander("Liquidation API errors"):
            for err in errors[:15]:
                st.code(err)

    if not frames:
        st.warning("No liquidation events returned for the selected scope/time window.")
        return

    liquidations = pd.concat(frames, ignore_index=True)
    liquidations["notional"] = pd.to_numeric(liquidations["notional"], errors="coerce")
    liquidations["price"] = pd.to_numeric(liquidations["price"], errors="coerce")
    liquidations["amount"] = pd.to_numeric(liquidations["amount"], errors="coerce")
    liquidations = liquidations.sort_values("timestamp_ms", ascending=False, na_position="last")

    c1, c2, c3 = st.columns(3)
    c1.metric("Events", int(len(liquidations)))
    c2.metric("Total Notional", f"{liquidations['notional'].fillna(0).sum():,.0f}")
    c3.metric("Symbols", int(liquidations["symbol"].nunique()))

    summary = (
        liquidations.groupby(["exchange_id", "symbol", "side"], as_index=False)["notional"]
        .sum()
        .sort_values("notional", ascending=False, na_position="last")
    )
    st.write("Aggregated by side")
    st.dataframe(
        summary,
        use_container_width=True,
        hide_index=True,
        column_config={"notional": st.column_config.NumberColumn(format="%.2f")},
    )

    st.write("Recent events")
    st.dataframe(
        liquidations[
            ["timestamp_utc", "exchange_id", "symbol", "side", "price", "amount", "notional"]
        ],
        use_container_width=True,
        hide_index=True,
        column_config={
            "price": st.column_config.NumberColumn(format="%.8f"),
            "amount": st.column_config.NumberColumn(format="%.8f"),
            "notional": st.column_config.NumberColumn(format="%.2f"),
        },
    )


def render_arbitrage_tab(
    arbitrage_df: pd.DataFrame,
    enabled: bool,
) -> None:
    st.subheader("Arbitrage Scanner")
    if not enabled:
        st.info("Enable `Arbitrage section` in the sidebar.")
        return

    if arbitrage_df.empty:
        st.warning("No cross-exchange opportunities above the configured spread threshold.")
        return

    c1, c2, c3 = st.columns(3)
    c1.metric("Opportunities", int(len(arbitrage_df)))
    c2.metric("Top Spread", f"{arbitrage_df['spread_pct'].max():.3f}%")
    c3.metric("Symbols", int(arbitrage_df["symbol"].nunique()))

    st.caption("Gross spread only. Fees, slippage, and transfer constraints are not included.")
    st.dataframe(
        arbitrage_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "buy_ask": st.column_config.NumberColumn(format="%.8f"),
            "sell_bid": st.column_config.NumberColumn(format="%.8f"),
            "spread_pct": st.column_config.NumberColumn(format="%.4f"),
            "trade_size_usd": st.column_config.NumberColumn(format="%.2f"),
            "gross_usd": st.column_config.NumberColumn(format="%.2f"),
        },
    )


def render_alert_tab(
    data: pd.DataFrame,
    arbitrage_df: pd.DataFrame,
    enabled: bool,
    enable_move_alerts: bool,
    long_move_threshold: float,
    short_move_threshold: float,
    enable_funding_alerts: bool,
    positive_funding_threshold: float,
    negative_funding_threshold: float,
    enable_utc_volume_alerts: bool,
    utc_volume_threshold: float,
    include_arbitrage_alerts: bool,
    enable_toasts: bool,
) -> None:
    st.subheader("Alerts")
    if not enabled:
        st.info("Enable `Alerts section` in the sidebar.")
        return

    pair_alerts = build_pair_alerts(
        data=data,
        enable_move_alerts=enable_move_alerts,
        long_move_threshold=long_move_threshold,
        short_move_threshold=short_move_threshold,
        enable_funding_alerts=enable_funding_alerts,
        positive_funding_threshold=positive_funding_threshold,
        negative_funding_threshold=negative_funding_threshold,
        enable_utc_volume_alerts=enable_utc_volume_alerts,
        utc_volume_threshold=utc_volume_threshold,
    )

    alert_frames: list[pd.DataFrame] = []
    if not pair_alerts.empty:
        alert_frames.append(pair_alerts)

    if include_arbitrage_alerts and not arbitrage_df.empty:
        arb_alerts = arbitrage_df.copy()
        arb_alerts["type"] = "Arbitrage Alert"
        arb_alerts["exchange_id"] = (
            arb_alerts["buy_exchange"].astype(str) + " -> " + arb_alerts["sell_exchange"].astype(str)
        )
        arb_alerts["reasons"] = "Cross-exchange spread above threshold"
        arb_alerts["last_price"] = pd.NA
        arb_alerts["quote_volume_24h"] = pd.NA
        arb_alerts["custom_pct_change"] = pd.NA
        arb_alerts["funding_rate"] = pd.NA
        arb_alerts["utc_slice_quote_volume"] = pd.NA
        alert_frames.append(
            arb_alerts[
                [
                    "type",
                    "exchange_id",
                    "market_type",
                    "symbol",
                    "last_price",
                    "quote_volume_24h",
                    "custom_pct_change",
                    "funding_rate",
                    "utc_slice_quote_volume",
                    "reasons",
                ]
            ]
        )

    if not alert_frames:
        st.success("No active alerts for the current snapshot.")
        return

    alerts = pd.concat(alert_frames, ignore_index=True)
    alerts = alerts.sort_values(["type", "exchange_id", "symbol"], na_position="last")

    st.metric("Active Alerts", int(len(alerts)))
    st.dataframe(
        alerts,
        use_container_width=True,
        hide_index=True,
        column_config={
            "last_price": st.column_config.NumberColumn(format="%.8f"),
            "quote_volume_24h": st.column_config.NumberColumn(format="%.2f"),
            "custom_pct_change": st.column_config.NumberColumn(format="%.4f"),
            "funding_rate": st.column_config.NumberColumn(format="%.6f"),
            "utc_slice_quote_volume": st.column_config.NumberColumn(format="%.2f"),
        },
    )

    if enable_toasts:
        if "seen_alert_keys" not in st.session_state:
            st.session_state["seen_alert_keys"] = set()
        seen_alert_keys: set[str] = st.session_state["seen_alert_keys"]

        for row in alerts.head(6).itertuples(index=False):
            key = f"{row.type}|{row.exchange_id}|{row.symbol}|{row.reasons}"
            if key in seen_alert_keys:
                continue
            st.toast(f"{row.type}: {row.exchange_id} {row.symbol} | {row.reasons}")
            seen_alert_keys.add(key)


def render_screener_view(filtered: pd.DataFrame) -> None:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Pairs", int(len(filtered)))
    c2.metric("Exchanges", int(filtered["exchange_id"].nunique()) if not filtered.empty else 0)
    c3.metric("Perpetual", int((filtered["market_type"] == "Perpetual").sum()))
    c4.metric("Spot", int((filtered["market_type"] == "Spot").sum()))

    if filtered.empty:
        st.warning("No pairs match the current filters.")
        return

    display = filtered[
        [
            "exchange_id",
            "market_type",
            "symbol",
            "last_price",
            "bid",
            "ask",
            "pct_change_24h",
            "quote_volume_24h",
            "funding_rate",
            "next_funding_utc",
            "custom_pct_change",
            "utc_slice_quote_volume",
            "ticker_timestamp_utc",
        ]
    ].rename(
        columns={
            "exchange_id": "Exchange",
            "market_type": "Market Type",
            "symbol": "Symbol",
            "last_price": "Last Price",
            "bid": "Bid",
            "ask": "Ask",
            "pct_change_24h": "24h %",
            "quote_volume_24h": "24h Quote Volume",
            "funding_rate": "Funding Rate",
            "next_funding_utc": "Next Funding (UTC)",
            "custom_pct_change": "Custom Move %",
            "utc_slice_quote_volume": "UTC Slice Quote Volume",
            "ticker_timestamp_utc": "Ticker Timestamp (UTC)",
        }
    )

    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Last Price": st.column_config.NumberColumn(format="%.8f"),
            "Bid": st.column_config.NumberColumn(format="%.8f"),
            "Ask": st.column_config.NumberColumn(format="%.8f"),
            "24h %": st.column_config.NumberColumn(format="%.3f"),
            "24h Quote Volume": st.column_config.NumberColumn(format="%.2f"),
            "Funding Rate": st.column_config.NumberColumn(format="%.6f"),
            "Custom Move %": st.column_config.NumberColumn(format="%.4f"),
            "UTC Slice Quote Volume": st.column_config.NumberColumn(format="%.2f"),
        },
    )

    csv_data = display.to_csv(index=False).encode("utf-8")
    now_utc = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    st.download_button(
        "Download CSV",
        data=csv_data,
        file_name=f"cex_screener_{now_utc}.csv",
        mime="text/csv",
    )


def fetch_live_ticker_updates(
    exchange_id: str,
    symbols: list[str],
) -> tuple[pd.DataFrame, str]:
    exchange = build_exchange(exchange_id)
    try:
        if not symbols:
            return pd.DataFrame(), ""

        tickers: dict[str, Any] = {}

        if exchange.has.get("fetchTickers"):
            try:
                raw = exchange.fetch_tickers(symbols)
                if isinstance(raw, dict):
                    tickers = raw
            except Exception:
                try:
                    raw = exchange.fetch_tickers()
                    if isinstance(raw, dict):
                        tickers = raw
                except Exception as exc:
                    return pd.DataFrame(), f"{exchange_id}: live ticker update failed ({exc})"

        if not tickers and exchange.has.get("fetchTicker"):
            for symbol in symbols:
                try:
                    tickers[symbol] = exchange.fetch_ticker(symbol)
                except Exception:
                    continue

        if not tickers:
            return pd.DataFrame(), f"{exchange_id}: no ticker endpoint available for live updates"

        rows: list[dict[str, Any]] = []
        for symbol in symbols:
            payload = tickers.get(symbol, {})
            if not isinstance(payload, dict):
                payload = {}

            last_price = to_float(payload.get("last"))
            if last_price is None:
                last_price = to_float(payload.get("close"))

            bid = to_float(payload.get("bid"))
            ask = to_float(payload.get("ask"))
            pct_change_24h = to_float(payload.get("percentage"))
            quote_volume_24h = to_float(payload.get("quoteVolume"))
            base_volume_24h = to_float(payload.get("baseVolume"))
            if quote_volume_24h is None and base_volume_24h is not None and last_price is not None:
                quote_volume_24h = base_volume_24h * last_price

            rows.append(
                {
                    "exchange_id": exchange_id,
                    "symbol": symbol,
                    "last_price": last_price,
                    "bid": bid,
                    "ask": ask,
                    "pct_change_24h": pct_change_24h,
                    "quote_volume_24h": quote_volume_24h,
                    "base_volume_24h": base_volume_24h,
                    "ticker_timestamp_utc": ms_to_utc_text(payload.get("timestamp")),
                }
            )

        return pd.DataFrame(rows), ""
    except Exception as exc:
        return pd.DataFrame(), f"{exchange_id}: {exc}"
    finally:
        close_exchange(exchange)


def apply_live_updates_to_generated_list(
    data: pd.DataFrame,
) -> tuple[pd.DataFrame, list[str]]:
    if data.empty:
        return data.copy(), []

    out = data.copy()
    errors: list[str] = []
    update_frames: list[pd.DataFrame] = []

    grouped = out.groupby("exchange_id", as_index=False)["symbol"].apply(list)
    for row in grouped.itertuples(index=False):
        exchange_id = row.exchange_id
        symbols = row.symbol
        frame, error_text = fetch_live_ticker_updates(exchange_id=exchange_id, symbols=symbols)
        if error_text:
            errors.append(error_text)
        if not frame.empty:
            update_frames.append(frame)

    if not update_frames:
        return out, errors

    updates = pd.concat(update_frames, ignore_index=True)
    merged = out.merge(updates, on=["exchange_id", "symbol"], how="left", suffixes=("", "_live"))

    live_cols = [
        "last_price",
        "bid",
        "ask",
        "pct_change_24h",
        "quote_volume_24h",
        "base_volume_24h",
        "ticker_timestamp_utc",
    ]
    for col in live_cols:
        live_col = f"{col}_live"
        if live_col not in merged.columns:
            continue
        merged[col] = merged[live_col].where(pd.notna(merged[live_col]), merged[col])
        merged = merged.drop(columns=[live_col])

    merged = prepare_numeric_columns(merged)
    return merged, errors


def run() -> None:
    st.set_page_config(page_title="CEX Multi-Market Screener", layout="wide")
    st.title("CEX Multi-Market Screener")
    st.caption(
        "Live ticker screener across ccxt exchanges with Spot + Perpetual support, "
        "advanced filters, orderbooks, liquidations, arbitrage, and alerts."
    )

    all_exchanges = sorted(ccxt.exchanges)
    default_selected = [ex for ex in DEFAULT_EXCHANGES if ex in all_exchanges]
    if "selected_exchanges" not in st.session_state:
        st.session_state["selected_exchanges"] = default_selected
    if "generated_df" not in st.session_state:
        st.session_state["generated_df"] = pd.DataFrame()
    if "generated_capabilities" not in st.session_state:
        st.session_state["generated_capabilities"] = {}
    if "generated_errors" not in st.session_state:
        st.session_state["generated_errors"] = []
    if "generated_live_errors" not in st.session_state:
        st.session_state["generated_live_errors"] = []
    if "generated_at_utc" not in st.session_state:
        st.session_state["generated_at_utc"] = None
    if "last_live_update_utc" not in st.session_state:
        st.session_state["last_live_update_utc"] = None

    with st.sidebar:
        st.header("Live")
        live_refresh = st.toggle("Live ticker refresh", value=True)
        refresh_interval_seconds = st.slider("Refresh interval (seconds)", 5, 120, 15)
        manual_live_update = st.button("Update generated prices now")
        if st.button("Clear cache / force refresh"):
            st.cache_data.clear()

        st.header("Exchange Universe")
        exchange_search = st.text_input("Exchange search", value="")
        exchange_matches = [
            exchange_id
            for exchange_id in all_exchanges
            if exchange_search.strip().lower() in exchange_id.lower()
        ]
        quick_pick = st.selectbox(
            "Exchange dropdown (search results)",
            options=[""] + exchange_matches,
            index=0,
        )
        c_add, c_all, c_clear = st.columns(3)
        if c_add.button("Add", use_container_width=True) and quick_pick:
            if quick_pick not in st.session_state["selected_exchanges"]:
                st.session_state["selected_exchanges"] = st.session_state["selected_exchanges"] + [
                    quick_pick
                ]
        if c_all.button("Add all", use_container_width=True):
            merged = sorted(set(st.session_state["selected_exchanges"] + exchange_matches))
            st.session_state["selected_exchanges"] = merged
        if c_clear.button("Clear", use_container_width=True):
            st.session_state["selected_exchanges"] = []

        selected_exchanges = st.multiselect(
            "Selected exchanges",
            options=all_exchanges,
            key="selected_exchanges",
        )

        st.header("Market Scope")
        market_mode = st.selectbox("Market type", ["Perpetual", "Spot", "Both"], index=2)
        pair_search = st.text_input("Pair search", value="")
        max_pairs_per_exchange = st.slider(
            "Max pairs loaded per exchange",
            min_value=20,
            max_value=2500,
            value=400,
            step=20,
        )

        st.header("Base Filters")
        symbol_query = st.text_input("Symbol contains", value="")
        min_quote_volume = st.number_input(
            "Min 24h quote volume (USD)",
            min_value=0.0,
            value=0.0,
            step=100000.0,
            format="%.0f",
        )
        max_quote_volume = st.number_input(
            "Max 24h quote volume (USD, 0 = no cap)",
            min_value=0.0,
            value=0.0,
            step=100000.0,
            format="%.0f",
        )
        pct_change_min, pct_change_max = st.slider(
            "24h change (%)",
            min_value=-80.0,
            max_value=80.0,
            value=(-15.0, 15.0),
            step=0.1,
        )
        funding_direction = st.selectbox(
            "Funding filter",
            options=["All", "Positive", "Negative", "Unavailable"],
            index=0,
        )

        st.header("Advanced Analytics Scope")
        analysis_pair_cap = st.slider(
            "Max pairs for advanced analytics",
            min_value=5,
            max_value=500,
            value=80,
        )

        st.header("UTC Slice Volume Filter")
        enable_utc_slice_filter = st.toggle("Enable UTC slice volume filter", value=False)
        utc_start_hour = st.slider("UTC start hour", 0, 23, 8)
        utc_end_hour = st.slider("UTC end hour", 0, 23, 12)
        utc_lookback_days = st.slider("UTC lookback days", 1, 30, 7)
        utc_volume_min = st.number_input(
            "UTC slice min quote volume",
            min_value=0.0,
            value=0.0,
            step=100000.0,
            format="%.0f",
        )
        utc_volume_max = st.number_input(
            "UTC slice max quote volume (0 = no cap)",
            min_value=0.0,
            value=0.0,
            step=100000.0,
            format="%.0f",
        )

        st.header("Custom Move Filter")
        enable_custom_move_filter = st.toggle("Enable custom move filter", value=False)
        move_timeframe = st.selectbox("Move timeframe", MOVE_TIMEFRAMES, index=2)
        move_candles_back = st.slider("Candles back", 2, 500, 48)
        move_direction = st.selectbox("Move direction", ["All", "Long", "Short"], index=0)
        move_min, move_max = st.slider(
            "Custom move range (%)",
            min_value=-80.0,
            max_value=80.0,
            value=(-5.0, 5.0),
            step=0.1,
        )

        st.header("Sections")
        orderbook_enabled = st.toggle("Orderbook section", value=True)
        orderbook_top_pairs = st.slider("Orderbook top pairs", 1, 40, 12)
        orderbook_depth_levels = st.slider("Orderbook depth levels", 10, 200, 60)

        liquidation_enabled = st.toggle("Liquidation section", value=True)
        liquidation_top_pairs = st.slider("Liquidation top pairs", 1, 40, 12)
        liquidation_window_minutes = st.slider("Liquidation window (minutes)", 5, 240, 60)
        liquidation_limit_per_pair = st.slider("Liquidation events per pair", 10, 200, 50)

        arbitrage_enabled = st.toggle("Arbitrage section", value=True)
        arbitrage_min_spread_pct = st.number_input(
            "Arbitrage min spread (%)",
            min_value=0.0,
            value=0.15,
            step=0.01,
            format="%.3f",
        )
        arbitrage_trade_size = st.number_input(
            "Arbitrage notional (USD)",
            min_value=100.0,
            value=10000.0,
            step=100.0,
            format="%.0f",
        )

        alerts_enabled = st.toggle("Alerts section", value=True)
        alerts_toasts = st.toggle("Toast popups", value=True)
        alert_move_enabled = st.toggle("Alert on custom move", value=True)
        alert_long_move_threshold = st.number_input(
            "Long move alert threshold (%)",
            min_value=0.0,
            value=2.0,
            step=0.1,
        )
        alert_short_move_threshold = st.number_input(
            "Short move alert threshold (%)",
            min_value=0.0,
            value=2.0,
            step=0.1,
        )
        alert_funding_enabled = st.toggle("Alert on funding", value=False)
        alert_positive_funding_threshold = st.number_input(
            "Positive funding alert",
            value=0.01,
            step=0.001,
            format="%.4f",
        )
        alert_negative_funding_threshold = st.number_input(
            "Negative funding alert",
            value=-0.01,
            step=0.001,
            format="%.4f",
        )
        alert_utc_volume_enabled = st.toggle("Alert on UTC slice volume", value=False)
        alert_utc_volume_threshold = st.number_input(
            "UTC slice volume alert threshold",
            min_value=0.0,
            value=10000000.0,
            step=100000.0,
            format="%.0f",
        )
        alert_include_arbitrage = st.toggle("Include arbitrage alerts", value=True)

        st.header("Generate")
        generate_pair_list = st.button("Generate / refresh pair list", type="primary")
        clear_generated_list = st.button("Clear generated list")

    if clear_generated_list:
        st.session_state["generated_df"] = pd.DataFrame()
        st.session_state["generated_capabilities"] = {}
        st.session_state["generated_errors"] = []
        st.session_state["generated_live_errors"] = []
        st.session_state["generated_at_utc"] = None
        st.session_state["last_live_update_utc"] = None

    if not selected_exchanges:
        st.warning("Select at least one exchange.")
        return

    generated_df = st.session_state.get("generated_df", pd.DataFrame())
    if generated_df.empty and not generate_pair_list:
        st.info("Set filters and click `Generate / refresh pair list` once.")

    if generate_pair_list:
        load_status = st.empty()
        load_status.info(f"Loading base list from {len(selected_exchanges)} exchange(s)...")

        frames: list[pd.DataFrame] = []
        generation_errors: list[str] = []
        exchange_capabilities: dict[str, dict[str, bool]] = {}
        progress = st.progress(0.0)

        for idx, exchange_id in enumerate(selected_exchanges, start=1):
            frame, caps, error_text = load_exchange_snapshot(
                exchange_id=exchange_id,
                market_mode=market_mode,
                pair_search=pair_search,
                max_pairs_per_exchange=max_pairs_per_exchange,
            )
            exchange_capabilities[exchange_id] = caps
            if error_text:
                generation_errors.append(error_text)
            if not frame.empty:
                frames.append(frame)
            progress.progress(idx / len(selected_exchanges))

        progress.empty()
        load_status.empty()

        if not frames:
            st.error("No market data could be loaded from selected exchanges.")
            if generation_errors:
                with st.expander("Load errors"):
                    for err in generation_errors[:25]:
                        st.code(err)
            return

        combined = pd.concat(frames, ignore_index=True)
        combined = prepare_numeric_columns(combined)

        filtered = apply_base_filters(
            combined,
            symbol_query=symbol_query,
            min_quote_volume=min_quote_volume,
            max_quote_volume=max_quote_volume,
            pct_change_min=pct_change_min,
            pct_change_max=pct_change_max,
            funding_direction=funding_direction,
        )
        filtered = filtered.sort_values("quote_volume_24h", ascending=False, na_position="last")
        filtered["custom_pct_change"] = pd.NA
        filtered["utc_slice_quote_volume"] = pd.NA

        advanced_active = enable_custom_move_filter or enable_utc_slice_filter
        if advanced_active and not filtered.empty:
            working = filtered.copy()
            if len(working) > analysis_pair_cap:
                st.info(
                    f"Advanced filters are applied to the top {analysis_pair_cap} pairs "
                    "by 24h quote volume to control API load."
                )
                working = working.head(analysis_pair_cap).copy()

            if enable_custom_move_filter:
                move_map: dict[tuple[str, str], float | None] = {}
                move_progress = st.progress(0.0)
                for idx, row in enumerate(working.itertuples(index=False), start=1):
                    move = compute_custom_pct_change(
                        exchange_id=row.exchange_id,
                        symbol=row.symbol,
                        timeframe=move_timeframe,
                        candles_back=move_candles_back,
                    )
                    move_map[(row.exchange_id, row.symbol)] = move
                    move_progress.progress(idx / len(working))
                move_progress.empty()

                working["custom_pct_change"] = [
                    move_map.get((ex, sym))
                    for ex, sym in zip(working["exchange_id"], working["symbol"])
                ]
                working["custom_pct_change"] = pd.to_numeric(
                    working["custom_pct_change"], errors="coerce"
                )
                working = working[
                    working["custom_pct_change"].between(move_min, move_max, inclusive="both")
                ]
                if move_direction == "Long":
                    working = working[working["custom_pct_change"] > 0]
                elif move_direction == "Short":
                    working = working[working["custom_pct_change"] < 0]

            if enable_utc_slice_filter and not working.empty:
                utc_map: dict[tuple[str, str], float | None] = {}
                utc_progress = st.progress(0.0)
                for idx, row in enumerate(working.itertuples(index=False), start=1):
                    utc_volume = compute_utc_slice_quote_volume(
                        exchange_id=row.exchange_id,
                        symbol=row.symbol,
                        start_hour_utc=utc_start_hour,
                        end_hour_utc=utc_end_hour,
                        lookback_days=utc_lookback_days,
                    )
                    utc_map[(row.exchange_id, row.symbol)] = utc_volume
                    utc_progress.progress(idx / len(working))
                utc_progress.empty()

                working["utc_slice_quote_volume"] = [
                    utc_map.get((ex, sym))
                    for ex, sym in zip(working["exchange_id"], working["symbol"])
                ]
                working["utc_slice_quote_volume"] = pd.to_numeric(
                    working["utc_slice_quote_volume"], errors="coerce"
                )
                working = working[working["utc_slice_quote_volume"].fillna(0) >= utc_volume_min]
                if utc_volume_max > 0:
                    working = working[working["utc_slice_quote_volume"].fillna(0) <= utc_volume_max]

            filtered = working

        st.session_state["generated_df"] = filtered.copy()
        st.session_state["generated_capabilities"] = exchange_capabilities
        st.session_state["generated_errors"] = generation_errors
        st.session_state["generated_live_errors"] = []
        st.session_state["generated_at_utc"] = datetime.now(tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        st.session_state["last_live_update_utc"] = None

    generated_df = st.session_state.get("generated_df", pd.DataFrame()).copy()
    exchange_capabilities = st.session_state.get("generated_capabilities", {})

    if generated_df.empty:
        st.info("No generated list available yet.")
        return

    if manual_live_update:
        updated_df, manual_live_errors = apply_live_updates_to_generated_list(generated_df)
        updated_df = apply_base_filters(
            updated_df,
            symbol_query=symbol_query,
            min_quote_volume=min_quote_volume,
            max_quote_volume=max_quote_volume,
            pct_change_min=pct_change_min,
            pct_change_max=pct_change_max,
            funding_direction=funding_direction,
        )
        updated_df = updated_df.sort_values("quote_volume_24h", ascending=False, na_position="last")
        generated_df = updated_df
        st.session_state["generated_df"] = updated_df.copy()
        st.session_state["generated_live_errors"] = manual_live_errors
        st.session_state["last_live_update_utc"] = datetime.now(tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    filtered = generated_df.copy()

    arbitrage_df = pd.DataFrame()
    if (arbitrage_enabled or (alerts_enabled and alert_include_arbitrage)) and not filtered.empty:
        arbitrage_df = build_arbitrage_table(
            data=filtered,
            min_spread_pct=arbitrage_min_spread_pct,
            trade_size_usd=arbitrage_trade_size,
        )

    tabs = st.tabs(["Screener", "Orderbooks", "Liquidations", "Arbitrage", "Alerts"])

    with tabs[0]:
        run_every = f"{refresh_interval_seconds}s" if live_refresh else None

        @st.fragment(run_every=run_every)
        def live_screener_fragment() -> None:
            current_df = st.session_state.get("generated_df", pd.DataFrame()).copy()
            if current_df.empty:
                st.info("No generated list available yet.")
                return

            live_errors_local: list[str] = st.session_state.get("generated_live_errors", [])
            if live_refresh:
                updated_df, live_errors_local = apply_live_updates_to_generated_list(current_df)
                updated_df = apply_base_filters(
                    updated_df,
                    symbol_query=symbol_query,
                    min_quote_volume=min_quote_volume,
                    max_quote_volume=max_quote_volume,
                    pct_change_min=pct_change_min,
                    pct_change_max=pct_change_max,
                    funding_direction=funding_direction,
                )
                updated_df = updated_df.sort_values(
                    "quote_volume_24h", ascending=False, na_position="last"
                )
                st.session_state["generated_df"] = updated_df.copy()
                st.session_state["generated_live_errors"] = live_errors_local
                st.session_state["last_live_update_utc"] = datetime.now(tz=timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                current_df = updated_df
            else:
                current_df = apply_base_filters(
                    current_df,
                    symbol_query=symbol_query,
                    min_quote_volume=min_quote_volume,
                    max_quote_volume=max_quote_volume,
                    pct_change_min=pct_change_min,
                    pct_change_max=pct_change_max,
                    funding_direction=funding_direction,
                ).sort_values("quote_volume_24h", ascending=False, na_position="last")

            generated_at_utc = st.session_state.get("generated_at_utc")
            last_live_update_utc = st.session_state.get("last_live_update_utc")
            st.caption(
                f"Generated list (UTC): {generated_at_utc or '-'} | "
                f"Last live ticker update (UTC): {last_live_update_utc or '-'} | "
                f"Live refresh: {'ON' if live_refresh else 'OFF'}"
            )
            st.caption(
                "Generated list stays on screen. Live cycles update only ticker fields "
                "and remove pairs that no longer match filters."
            )

            generation_errors_local: list[str] = st.session_state.get("generated_errors", [])
            if generation_errors_local or live_errors_local:
                with st.expander("Load warnings / errors"):
                    for item in generation_errors_local[:25]:
                        st.code(item)
                    for item in live_errors_local[:25]:
                        st.code(item)

            render_screener_view(current_df)

        live_screener_fragment()

    with tabs[1]:
        render_orderbook_tab(
            data=filtered,
            exchange_capabilities=exchange_capabilities,
            enabled=orderbook_enabled,
            top_pairs=orderbook_top_pairs,
            depth_levels=orderbook_depth_levels,
        )

    with tabs[2]:
        render_liquidation_tab(
            data=filtered,
            exchange_capabilities=exchange_capabilities,
            enabled=liquidation_enabled,
            top_pairs=liquidation_top_pairs,
            window_minutes=liquidation_window_minutes,
            limit_per_pair=liquidation_limit_per_pair,
        )

    with tabs[3]:
        render_arbitrage_tab(arbitrage_df=arbitrage_df, enabled=arbitrage_enabled)

    with tabs[4]:
        render_alert_tab(
            data=filtered,
            arbitrage_df=arbitrage_df,
            enabled=alerts_enabled,
            enable_move_alerts=alert_move_enabled,
            long_move_threshold=alert_long_move_threshold,
            short_move_threshold=alert_short_move_threshold,
            enable_funding_alerts=alert_funding_enabled,
            positive_funding_threshold=alert_positive_funding_threshold,
            negative_funding_threshold=alert_negative_funding_threshold,
            enable_utc_volume_alerts=alert_utc_volume_enabled,
            utc_volume_threshold=alert_utc_volume_threshold,
            include_arbitrage_alerts=alert_include_arbitrage,
            enable_toasts=alerts_toasts,
        )


if __name__ == "__main__":
    run()
