from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


MarketType = Literal["perpetual", "spot", "both"]
RsiMode = Literal["all", "lt30", "gt70", "between30_70"]
RsiBucket = Literal["lt30", "gt70", "between30_70"]
RsiTimeframe = Literal["1m", "5m", "15m", "1h", "4h", "1d"]
NatrBucket = Literal["compression", "normal", "high", "extreme"]
NatrTimeframe = Literal["1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d"]


class FilterConfig(BaseModel):
    min_quote_volume: float = 0.0
    max_quote_volume: float = 0.0
    min_pct_change: float = -100.0
    max_pct_change: float = 100.0
    symbol_contains: str = ""
    rsi_mode: RsiMode = "all"
    rsi_modes: list[RsiBucket] = Field(default_factory=list)
    rsi_timeframe: RsiTimeframe = "1d"
    rsi_period: int = Field(default=14, ge=2, le=200)
    natr_modes: list[NatrBucket] = Field(default_factory=list)
    natr_timeframe: NatrTimeframe = "1h"
    natr_period: int = Field(default=14, ge=2, le=200)
    natr_enabled: bool = False


class StartPayload(BaseModel):
    exchanges: list[str] = Field(default_factory=list)
    market_type: MarketType = "both"
    pair_search: str = ""
    max_pairs_per_exchange: int = Field(default=250, ge=1, le=5000)
    tick_interval_sec: float = Field(default=2.0, ge=0.2, le=60.0)
    universe_refresh_sec: float = Field(default=900.0, ge=30.0, le=86400.0)
    filters: FilterConfig = Field(default_factory=FilterConfig)


class UpdateFiltersPayload(BaseModel):
    filters: FilterConfig


class ClientEnvelope(BaseModel):
    type: str
    payload: dict = Field(default_factory=dict)
