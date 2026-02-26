import { useEffect, useMemo, useRef, useState } from "react";
import { CandlestickSeries, LineSeries, createChart } from "lightweight-charts";
import "./App.css";

const WS_URL = import.meta.env.VITE_WS_URL || "ws://localhost:8000/ws/live";
const EXCLUDED_EXCHANGE_TOKENS = ["kraken", "mexc"];
const DEFAULT_SELECTED_EXCHANGES = ["binanceusdm", "bybit"];
const FALLBACK_EXCHANGES = [...DEFAULT_SELECTED_EXCHANGES].sort((a, b) => a.localeCompare(b));
const CHART_CANDLE_MAX_POINTS = 500;
const CHART_TIMEFRAME_OPTIONS = ["1m", "5m", "15m", "1h", "4h", "1d"];
const CHART_TOTAL_HEIGHT = 500;
const CHART_PANE_STRETCH = {
  price: 7,
  rsi: 2.4,
  natr: 2.4,
};
const WS_RECONNECT_BASE_MS = 1500;
const WS_RECONNECT_MAX_MS = 15000;
const WS_STALE_TIMEOUT_MS = 60000;
const WS_STALE_CHECK_INTERVAL_MS = 10000;
const CHART_TIMEFRAME_MS = {
  "1m": 60_000,
  "3m": 180_000,
  "5m": 300_000,
  "15m": 900_000,
  "30m": 1_800_000,
  "1h": 3_600_000,
  "4h": 14_400_000,
  "1d": 86_400_000,
};

const API_BASE_URL = (() => {
  try {
    const url = new URL(WS_URL);
    const protocol = url.protocol === "wss:" ? "https:" : "http:";
    return `${protocol}//${url.host}`;
  } catch {
    return "http://localhost:8000";
  }
})();

function parseTickerTimeToMs(value) {
  if (typeof value !== "string" || !value.trim()) {
    return Date.now();
  }

  const raw = value.trim();
  const normalized = raw.includes("T") ? raw : raw.replace(" ", "T");
  const parsed = Date.parse(`${normalized}Z`);
  if (Number.isFinite(parsed)) {
    return parsed;
  }
  return Date.now();
}

function timeframeToMs(timeframe) {
  return CHART_TIMEFRAME_MS[timeframe] || CHART_TIMEFRAME_MS["1m"];
}

function normalizeCandle(item) {
  const time = Number(item?.time);
  const open = Number(item?.open);
  const high = Number(item?.high);
  const low = Number(item?.low);
  const close = Number(item?.close);
  if (
    !Number.isFinite(time) ||
    !Number.isFinite(open) ||
    !Number.isFinite(high) ||
    !Number.isFinite(low) ||
    !Number.isFinite(close)
  ) {
    return null;
  }

  return {
    time: Math.trunc(time),
    open,
    high,
    low,
    close,
  };
}

function mergeTickIntoCandles(candles, price, tickTimestampMs, bucketSizeMs) {
  if (!Number.isFinite(price) || !Number.isFinite(tickTimestampMs) || !Number.isFinite(bucketSizeMs) || bucketSizeMs <= 0) {
    return Array.isArray(candles) ? candles : [];
  }

  const next = Array.isArray(candles) ? [...candles] : [];
  const bucketStartMs = Math.floor(tickTimestampMs / bucketSizeMs) * bucketSizeMs;
  const bucketTime = Math.trunc(bucketStartMs / 1000);

  if (next.length === 0) {
    return [
      {
        time: bucketTime,
        open: price,
        high: price,
        low: price,
        close: price,
      },
    ];
  }

  const lastIndex = next.length - 1;
  const previous = next[lastIndex];
  const previousTime = Number(previous?.time);
  if (!Number.isFinite(previousTime)) {
    return next;
  }

  if (bucketTime < previousTime) {
    return next;
  }

  if (bucketTime === previousTime) {
    next[lastIndex] = {
      ...previous,
      high: Math.max(Number(previous.high), price),
      low: Math.min(Number(previous.low), price),
      close: price,
    };
    return next;
  }

  const open = Number(previous.close);
  next.push({
    time: bucketTime,
    open,
    high: Math.max(open, price),
    low: Math.min(open, price),
    close: price,
  });

  if (next.length > CHART_CANDLE_MAX_POINTS) {
    next.splice(0, next.length - CHART_CANDLE_MAX_POINTS);
  }

  return next;
}

function computeRsiSeriesWilder(candles, period = 14) {
  const safePeriod = Math.max(2, Math.trunc(Number(period) || 14));
  const normalized = (Array.isArray(candles) ? candles : [])
    .map((candle) => ({
      time: Number(candle?.time),
      close: Number(candle?.close),
    }))
    .filter((item) => Number.isFinite(item.time) && Number.isFinite(item.close));

  if (normalized.length < safePeriod + 1) {
    return [];
  }

  const closes = normalized.map((item) => item.close);
  let gains = 0;
  let losses = 0;
  for (let index = 1; index <= safePeriod; index += 1) {
    const change = closes[index] - closes[index - 1];
    gains += Math.max(change, 0);
    losses += Math.max(-change, 0);
  }

  let avgGain = gains / safePeriod;
  let avgLoss = losses / safePeriod;
  const result = [];

  const calcRsi = () => {
    if (avgLoss === 0) {
      if (avgGain === 0) {
        return 50;
      }
      return 100;
    }
    const rs = avgGain / avgLoss;
    return 100 - 100 / (1 + rs);
  };

  result.push({
    time: Math.trunc(normalized[safePeriod].time),
    value: calcRsi(),
  });

  for (let index = safePeriod + 1; index < normalized.length; index += 1) {
    const change = closes[index] - closes[index - 1];
    const gain = Math.max(change, 0);
    const loss = Math.max(-change, 0);
    avgGain = (avgGain * (safePeriod - 1) + gain) / safePeriod;
    avgLoss = (avgLoss * (safePeriod - 1) + loss) / safePeriod;
    result.push({
      time: Math.trunc(normalized[index].time),
      value: calcRsi(),
    });
  }

  return result;
}

function computeNatrSeriesWilder(candles, period = 14) {
  const safePeriod = Math.max(2, Math.trunc(Number(period) || 14));
  const normalized = (Array.isArray(candles) ? candles : [])
    .map((candle) => ({
      time: Number(candle?.time),
      high: Number(candle?.high),
      low: Number(candle?.low),
      close: Number(candle?.close),
    }))
    .filter(
      (item) =>
        Number.isFinite(item.time) &&
        Number.isFinite(item.high) &&
        Number.isFinite(item.low) &&
        Number.isFinite(item.close),
    );

  if (normalized.length < safePeriod + 1) {
    return [];
  }

  const trueRanges = [];
  for (let index = 1; index < normalized.length; index += 1) {
    const current = normalized[index];
    const previousClose = normalized[index - 1].close;
    const tr = Math.max(
      current.high - current.low,
      Math.abs(current.high - previousClose),
      Math.abs(current.low - previousClose),
    );
    trueRanges.push(tr);
  }

  if (trueRanges.length < safePeriod) {
    return [];
  }

  let atr = trueRanges.slice(0, safePeriod).reduce((sum, value) => sum + value, 0) / safePeriod;
  const result = [];

  const firstClose = normalized[safePeriod].close;
  if (firstClose > 0) {
    result.push({
      time: Math.trunc(normalized[safePeriod].time),
      value: (atr / firstClose) * 100,
    });
  }

  for (let index = safePeriod + 1; index < normalized.length; index += 1) {
    const tr = trueRanges[index - 1];
    atr = (atr * (safePeriod - 1) + tr) / safePeriod;
    const close = normalized[index].close;
    if (close <= 0) {
      continue;
    }
    result.push({
      time: Math.trunc(normalized[index].time),
      value: (atr / close) * 100,
    });
  }

  return result;
}

function formatNum(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "-";
  }
  return Number(value).toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function priceDisplayDigits(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return 2;
  }
  return numeric >= 0 && numeric < 1 ? 4 : 2;
}

function symbolTier(symbol) {
  const raw = String(symbol || "").trim().toUpperCase();
  if (!raw) {
    return "";
  }

  const base = raw.split("/")[0]?.split(":")[0]?.trim() || "";
  if (base === "BTC") {
    return "btc";
  }
  if (base === "ETH") {
    return "eth";
  }
  return "";
}

function toFiniteNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function normalizeText(value) {
  return String(value || "").trim().toLowerCase();
}

function isExchangeAllowed(exchangeId) {
  const normalized = normalizeText(exchangeId);
  if (!normalized) {
    return false;
  }
  return !EXCLUDED_EXCHANGE_TOKENS.some((token) => normalized.includes(token));
}

function isSubsequence(text, pattern) {
  if (!pattern) {
    return true;
  }

  let textIndex = 0;
  let patternIndex = 0;
  while (textIndex < text.length && patternIndex < pattern.length) {
    if (text[textIndex] === pattern[patternIndex]) {
      patternIndex += 1;
    }
    textIndex += 1;
  }

  return patternIndex === pattern.length;
}

function levenshteinDistance(source, target) {
  const sourceText = normalizeText(source);
  const targetText = normalizeText(target);

  if (sourceText === targetText) {
    return 0;
  }

  if (!sourceText.length) {
    return targetText.length;
  }

  if (!targetText.length) {
    return sourceText.length;
  }

  const previous = new Array(targetText.length + 1).fill(0);
  const current = new Array(targetText.length + 1).fill(0);

  for (let index = 0; index <= targetText.length; index += 1) {
    previous[index] = index;
  }

  for (let sourceIndex = 1; sourceIndex <= sourceText.length; sourceIndex += 1) {
    current[0] = sourceIndex;
    for (let targetIndex = 1; targetIndex <= targetText.length; targetIndex += 1) {
      const cost = sourceText[sourceIndex - 1] === targetText[targetIndex - 1] ? 0 : 1;
      current[targetIndex] = Math.min(
        current[targetIndex - 1] + 1,
        previous[targetIndex] + 1,
        previous[targetIndex - 1] + cost,
      );
    }
    for (let targetIndex = 0; targetIndex <= targetText.length; targetIndex += 1) {
      previous[targetIndex] = current[targetIndex];
    }
  }

  return previous[targetText.length];
}

function fuzzyScore(candidate, query) {
  const exchangeId = normalizeText(candidate);
  const search = normalizeText(query);
  if (!search) {
    return 0;
  }

  if (exchangeId === search) {
    return 10_000;
  }

  if (exchangeId.startsWith(search)) {
    return 9_000 - (exchangeId.length - search.length);
  }

  const containsIndex = exchangeId.indexOf(search);
  if (containsIndex >= 0) {
    return 8_000 - containsIndex;
  }

  if (isSubsequence(exchangeId, search)) {
    return 7_000 - (exchangeId.length - search.length);
  }

  const threshold = Math.max(1, Math.floor(search.length / 3));
  const distance = levenshteinDistance(exchangeId, search);
  if (distance <= threshold) {
    return 6_000 - distance * 100;
  }

  return -1;
}

function App() {
  const [connected, setConnected] = useState(false);
  const [engineState, setEngineState] = useState("idle");
  const [lastUpdate, setLastUpdate] = useState("-");
  const [stats, setStats] = useState(null);
  const [rowsById, setRowsById] = useState({});
  const [priceDirectionById, setPriceDirectionById] = useState({});
  const [selectedRowId, setSelectedRowId] = useState(null);
  const [chartTimeframe, setChartTimeframe] = useState("1m");
  const [chartLoading, setChartLoading] = useState(false);
  const [chartError, setChartError] = useState("");
  const [drawingMode, setDrawingMode] = useState(false);
  const [drawingAnchorSet, setDrawingAnchorSet] = useState(false);
  const [drawingCount, setDrawingCount] = useState(0);
  const [errors, setErrors] = useState([]);

  const [allExchanges, setAllExchanges] = useState(FALLBACK_EXCHANGES);
  const [selectedExchanges, setSelectedExchanges] = useState(
    [...DEFAULT_SELECTED_EXCHANGES].sort((a, b) => a.localeCompare(b)),
  );
  const [exchangeQuery, setExchangeQuery] = useState("");
  const [exchangeMetaLoading, setExchangeMetaLoading] = useState(false);
  const [exchangeMetaError, setExchangeMetaError] = useState("");
  const [marketType, setMarketType] = useState("both");
  const [pairSearch, setPairSearch] = useState("");
  const [maxPairs, setMaxPairs] = useState(250);
  const [tickInterval, setTickInterval] = useState(2);
  const [universeRefresh, setUniverseRefresh] = useState(900);

  const [minPct, setMinPct] = useState(-7);
  const [maxPct, setMaxPct] = useState(7);
  const [minVol, setMinVol] = useState(0);
  const [maxVol, setMaxVol] = useState(0);
  const [symbolContains, setSymbolContains] = useState("");
  const [rsiLt30Enabled, setRsiLt30Enabled] = useState(false);
  const [rsiGt70Enabled, setRsiGt70Enabled] = useState(false);
  const [rsiBetweenEnabled, setRsiBetweenEnabled] = useState(false);
  const [rsiTimeframe, setRsiTimeframe] = useState("1d");
  const [rsiPeriod, setRsiPeriod] = useState(14);
  const [showRsiInChart, setShowRsiInChart] = useState(false);
  const [natrCompressionEnabled, setNatrCompressionEnabled] = useState(false);
  const [natrNormalEnabled, setNatrNormalEnabled] = useState(false);
  const [natrHighEnabled, setNatrHighEnabled] = useState(false);
  const [natrExtremeEnabled, setNatrExtremeEnabled] = useState(false);
  const [natrTimeframe, setNatrTimeframe] = useState("1h");
  const [natrPeriod, setNatrPeriod] = useState(14);
  const [showNatrInChart, setShowNatrInChart] = useState(false);

  const wsRef = useRef(null);
  const pendingRef = useRef([]);
  const rowsByIdRef = useRef({});
  const priceDirectionByIdRef = useRef({});
  const universeConfigRef = useRef(null);
  const engineStateRef = useRef("idle");
  const reconnectTimerRef = useRef(null);
  const reconnectAttemptRef = useRef(0);
  const shouldResyncOnOpenRef = useRef(false);
  const lastServerEventMsRef = useRef(Date.now());
  const manualDisconnectRef = useRef(false);
  const selectedRowIdRef = useRef(null);
  const chartTimeframeRef = useRef("1m");
  const rsiPeriodRef = useRef(14);
  const showRsiInChartRef = useRef(false);
  const natrPeriodRef = useRef(14);
  const showNatrInChartRef = useRef(false);
  const drawingModeRef = useRef(false);
  const drawingStartRef = useRef(null);
  const chartContainerRef = useRef(null);
  const chartApiRef = useRef(null);
  const candleSeriesRef = useRef(null);
  const rsiSeriesRef = useRef(null);
  const natrSeriesRef = useRef(null);
  const chartPriceDigitsRef = useRef(2);
  const candlesRef = useRef([]);
  const drawingSeriesRef = useRef([]);
  const hasStartedRef = useRef(false);
  const lastUniverseKeyRef = useRef("");
  const lastFilterKeyRef = useRef("");
  const sendRef = useRef(null);
  const filterConfigRef = useRef(null);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();

    const loadExchanges = async () => {
      setExchangeMetaLoading(true);
      setExchangeMetaError("");

      try {
        const response = await fetch(`${API_BASE_URL}/meta/exchanges`, { signal: controller.signal });
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }

        const payload = await response.json();
        const values = Array.isArray(payload?.exchanges) ? payload.exchanges : [];
        const normalized = [...new Set(values.map((item) => String(item).trim()).filter(Boolean))];
        const allowedExchanges = normalized.filter((exchangeId) => isExchangeAllowed(exchangeId)).sort((a, b) =>
          a.localeCompare(b),
        );

        if (!active || allowedExchanges.length === 0) {
          return;
        }

        setAllExchanges(allowedExchanges);
        setSelectedExchanges((previous) => {
          const validPrevious = previous.filter(
            (exchangeId) => isExchangeAllowed(exchangeId) && allowedExchanges.includes(exchangeId),
          );
          if (validPrevious.length > 0) {
            return [...new Set(validPrevious)].sort((a, b) => a.localeCompare(b));
          }

          const defaults = DEFAULT_SELECTED_EXCHANGES.filter((exchangeId) => allowedExchanges.includes(exchangeId));
          if (defaults.length > 0) {
            return [...new Set(defaults)].sort((a, b) => a.localeCompare(b));
          }

          return allowedExchanges.slice(0, Math.min(4, allowedExchanges.length));
        });
      } catch (error) {
        const isAbort =
          typeof error === "object" &&
          error !== null &&
          "name" in error &&
          error.name === "AbortError";
        if (!active || isAbort) {
          return;
        }

        setExchangeMetaError(`Could not load exchanges from backend (${String(error)}). Using fallback list.`);
        setAllExchanges(FALLBACK_EXCHANGES);
        setSelectedExchanges((previous) => {
          const validPrevious = previous.filter((exchangeId) => isExchangeAllowed(exchangeId));
          if (validPrevious.length > 0) {
            return [...new Set(validPrevious)].sort((a, b) => a.localeCompare(b));
          }
          return [...DEFAULT_SELECTED_EXCHANGES].sort((a, b) => a.localeCompare(b));
        });
      } finally {
        if (active) {
          setExchangeMetaLoading(false);
        }
      }
    };

    loadExchanges();

    return () => {
      active = false;
      controller.abort();
    };
  }, []);

  const filteredExchangeOptions = useMemo(() => {
    const sorted = [...allExchanges].filter((exchangeId) => isExchangeAllowed(exchangeId)).sort((a, b) => a.localeCompare(b));
    const query = normalizeText(exchangeQuery);
    if (!query) {
      return sorted;
    }

    return sorted.filter((exchangeId) => fuzzyScore(exchangeId, query) >= 0);
  }, [allExchanges, exchangeQuery]);

  const universeConfig = useMemo(
    () => ({
      exchanges: [...new Set(selectedExchanges.filter((exchangeId) => isExchangeAllowed(exchangeId)))].sort((a, b) =>
        a.localeCompare(b),
      ),
      market_type: marketType,
      pair_search: pairSearch,
      max_pairs_per_exchange: Math.trunc(clamp(toFiniteNumber(maxPairs, 250), 1, 5000)),
      tick_interval_sec: clamp(toFiniteNumber(tickInterval, 2), 0.2, 60),
      universe_refresh_sec: clamp(toFiniteNumber(universeRefresh, 900), 30, 86400),
    }),
    [selectedExchanges, marketType, pairSearch, maxPairs, tickInterval, universeRefresh],
  );
  universeConfigRef.current = universeConfig;

  const filterConfig = useMemo(
    () => {
      const selectedRsiModes = [];
      if (rsiLt30Enabled) {
        selectedRsiModes.push("lt30");
      }
      if (rsiGt70Enabled) {
        selectedRsiModes.push("gt70");
      }
      if (rsiBetweenEnabled) {
        selectedRsiModes.push("between30_70");
      }

      const selectedNatrModes = [];
      if (natrCompressionEnabled) {
        selectedNatrModes.push("compression");
      }
      if (natrNormalEnabled) {
        selectedNatrModes.push("normal");
      }
      if (natrHighEnabled) {
        selectedNatrModes.push("high");
      }
      if (natrExtremeEnabled) {
        selectedNatrModes.push("extreme");
      }

      return {
        min_quote_volume: toFiniteNumber(minVol, 0),
        max_quote_volume: Math.max(0, toFiniteNumber(maxVol, 0)),
        min_pct_change: toFiniteNumber(minPct, -100),
        max_pct_change: toFiniteNumber(maxPct, 100),
        symbol_contains: symbolContains,
        rsi_mode: selectedRsiModes.length === 1 ? selectedRsiModes[0] : "all",
        rsi_modes: selectedRsiModes,
        rsi_timeframe: rsiTimeframe,
        rsi_period: Math.trunc(clamp(toFiniteNumber(rsiPeriod, 14), 2, 200)),
        natr_modes: selectedNatrModes,
        natr_timeframe: natrTimeframe,
        natr_period: Math.trunc(clamp(toFiniteNumber(natrPeriod, 14), 2, 200)),
        natr_enabled: showNatrInChart || selectedNatrModes.length > 0,
      };
    },
    [
      minVol,
      maxVol,
      minPct,
      maxPct,
      symbolContains,
      rsiLt30Enabled,
      rsiGt70Enabled,
      rsiBetweenEnabled,
      rsiTimeframe,
      rsiPeriod,
      natrCompressionEnabled,
      natrNormalEnabled,
      natrHighEnabled,
      natrExtremeEnabled,
      natrTimeframe,
      natrPeriod,
      showNatrInChart,
    ],
  );
  filterConfigRef.current = filterConfig;

  const sortedRows = useMemo(() => {
    const minQuoteVolume = Number(filterConfig.min_quote_volume || 0);
    const maxQuoteVolume = Number(filterConfig.max_quote_volume || 0);
    const minPctChange = Number(filterConfig.min_pct_change ?? -100);
    const maxPctChange = Number(filterConfig.max_pct_change ?? 100);
    const symbolNeedle = normalizeText(filterConfig.symbol_contains || "");
    const activeRsiModes = new Set(filterConfig.rsi_modes || []);
    const activeNatrModes = new Set(filterConfig.natr_modes || []);
    const rsiFilterActive = activeRsiModes.size > 0;
    const natrFilterActive = activeNatrModes.size > 0;

    const arr = Object.values(rowsById).filter((row) => {
      if (selectedExchanges.length === 0) {
        return false;
      }

      if (!selectedExchanges.includes(String(row?.exchange_id || ""))) {
        return false;
      }

      const rowMarketType = String(row?.market_type || "").toLowerCase();
      if (marketType === "perpetual" && rowMarketType !== "perpetual") {
        return false;
      }
      if (marketType === "spot" && rowMarketType !== "spot") {
        return false;
      }

      const symbolValue = normalizeText(row?.symbol || "");
      if (symbolNeedle && !symbolValue.includes(symbolNeedle)) {
        return false;
      }

      const quoteVolume = Number(row?.quote_volume_24h);
      const normalizedQuoteVolume = Number.isFinite(quoteVolume) ? quoteVolume : 0;
      if (normalizedQuoteVolume < minQuoteVolume) {
        return false;
      }
      if (maxQuoteVolume > 0 && normalizedQuoteVolume > maxQuoteVolume) {
        return false;
      }

      const pctValue = Number(row?.pct_change_24h);
      const normalizedPct = Number.isFinite(pctValue) ? pctValue : 0;
      if (normalizedPct < minPctChange || normalizedPct > maxPctChange) {
        return false;
      }

      if (rsiFilterActive) {
        const rsiValue = Number(row?.rsi);
        if (!Number.isFinite(rsiValue)) {
          return false;
        }

        const rsiMatch =
          (activeRsiModes.has("lt30") && rsiValue < 30) ||
          (activeRsiModes.has("gt70") && rsiValue > 70) ||
          (activeRsiModes.has("between30_70") && rsiValue >= 30 && rsiValue <= 70);
        if (!rsiMatch) {
          return false;
        }
      }

      if (natrFilterActive) {
        const natrValue = Number(row?.natr);
        if (!Number.isFinite(natrValue)) {
          return false;
        }

        const natrMatch =
          (activeNatrModes.has("compression") && natrValue < 0.8) ||
          (activeNatrModes.has("normal") && natrValue >= 0.8 && natrValue <= 1.8) ||
          (activeNatrModes.has("high") && natrValue > 1.8) ||
          (activeNatrModes.has("extreme") && natrValue > 3.5);
        if (!natrMatch) {
          return false;
        }
      }

      return true;
    });
    arr.sort((a, b) => {
      const exchangeCompare = String(a?.exchange_id || "").localeCompare(String(b?.exchange_id || ""), undefined, {
        sensitivity: "base",
        numeric: true,
      });
      if (exchangeCompare !== 0) {
        return exchangeCompare;
      }

      const symbolCompare = String(a?.symbol || "").localeCompare(String(b?.symbol || ""), undefined, {
        sensitivity: "base",
        numeric: true,
      });
      if (symbolCompare !== 0) {
        return symbolCompare;
      }

      return String(a?.market_type || "").localeCompare(String(b?.market_type || ""), undefined, {
        sensitivity: "base",
        numeric: true,
      });
    });
    return arr;
  }, [rowsById, selectedExchanges, filterConfig, marketType]);

  const selectedRow = selectedRowId ? rowsById[selectedRowId] : null;
  const selectedChartTrendClass = priceDirectionById[selectedRowId || ""] || "";
  const natrFilterUiActive = natrCompressionEnabled || natrNormalEnabled || natrHighEnabled || natrExtremeEnabled;
  const showNatrColumn = showNatrInChart || natrFilterUiActive;

  const pushError = (message) => {
    const normalized = String(message || "Unknown error");
    setErrors((prev) => [normalized, ...prev.filter((item) => item !== normalized)].slice(0, 8));
  };

  const toggleExchange = (exchangeId) => {
    if (!isExchangeAllowed(exchangeId)) {
      return;
    }
    setSelectedExchanges((previous) => {
      if (previous.includes(exchangeId)) {
        return previous.filter((value) => value !== exchangeId);
      }
      return [...previous, exchangeId].sort((a, b) => a.localeCompare(b));
    });
  };

  const selectAllExchanges = () => {
    setSelectedExchanges(
      [...new Set(allExchanges.filter((exchangeId) => isExchangeAllowed(exchangeId)))].sort((a, b) =>
        a.localeCompare(b),
      ),
    );
  };

  const clearSelectedExchanges = () => {
    setSelectedExchanges([]);
  };

  const clearDrawings = () => {
    const chart = chartApiRef.current;
    if (!chart) {
      drawingSeriesRef.current = [];
      drawingStartRef.current = null;
      setDrawingCount(0);
      setDrawingAnchorSet(false);
      return;
    }

    for (const series of drawingSeriesRef.current) {
      try {
        chart.removeSeries(series);
      } catch {
        // chart may already be disposed
      }
    }
    drawingSeriesRef.current = [];
    drawingStartRef.current = null;
    setDrawingCount(0);
    setDrawingAnchorSet(false);
  };

  const resetChartViewport = () => {
    const chart = chartApiRef.current;
    if (!chart) {
      return;
    }

    try {
      chart.priceScale("right").setAutoScale(true);
    } catch {
      // best effort
    }

    try {
      chart.timeScale().resetTimeScale();
    } catch {
      chart.timeScale().fitContent();
    }
  };

  const applyChartPriceFormatFromValue = (value) => {
    const candleSeries = candleSeriesRef.current;
    if (!candleSeries) {
      return;
    }

    const digits = priceDisplayDigits(value);
    if (chartPriceDigitsRef.current === digits) {
      return;
    }

    chartPriceDigitsRef.current = digits;
    candleSeries.applyOptions({
      priceFormat: {
        type: "price",
        precision: digits,
        minMove: digits === 4 ? 0.0001 : 0.01,
      },
    });
  };

  const applyPaneVisualFrames = () => {
    const chart = chartApiRef.current;
    if (!chart) {
      return;
    }

    const showRsi = showRsiInChartRef.current;
    const showNatr = showNatrInChartRef.current;
    const paneStyles = [
      {
        index: 0,
        border: "rgba(95, 131, 175, 0.5)",
        background: "linear-gradient(180deg, rgba(12, 21, 36, 0.88) 0%, rgba(10, 17, 29, 0.88) 100%)",
      },
      {
        index: 1,
        border: showRsi ? "rgba(79, 195, 255, 0.78)" : "rgba(79, 195, 255, 0.28)",
        background: showRsi
          ? "linear-gradient(180deg, rgba(19, 42, 66, 0.78) 0%, rgba(15, 33, 52, 0.78) 100%)"
          : "linear-gradient(180deg, rgba(15, 27, 42, 0.65) 0%, rgba(13, 24, 39, 0.65) 100%)",
      },
      {
        index: 2,
        border: showNatr ? "rgba(255, 176, 74, 0.78)" : "rgba(255, 176, 74, 0.28)",
        background: showNatr
          ? "linear-gradient(180deg, rgba(63, 42, 20, 0.78) 0%, rgba(46, 30, 14, 0.78) 100%)"
          : "linear-gradient(180deg, rgba(33, 25, 16, 0.65) 0%, rgba(26, 20, 13, 0.65) 100%)",
      },
    ];

    try {
      const panes = chart.panes();
      for (const paneStyle of paneStyles) {
        const paneElement = panes[paneStyle.index]?.getHTMLElement?.();
        if (!paneElement) {
          continue;
        }
        paneElement.style.boxSizing = "border-box";
        paneElement.style.border = `1px solid ${paneStyle.border}`;
        paneElement.style.borderRadius = "4px";
        paneElement.style.background = paneStyle.background;
      }
    } catch {
      // optional pane styling support
    }
  };

  const updateIndicatorScale = (rsiPoints = [], natrPoints = []) => {
    const chart = chartApiRef.current;
    if (!chart) {
      return;
    }

    const showRsi = showRsiInChartRef.current;
    const showNatr = showNatrInChartRef.current;

    try {
      const priceScale = chart.priceScale("right", 0);
      priceScale.applyOptions({
        visible: true,
        autoScale: true,
        scaleMargins: { top: 0.08, bottom: 0.08 },
        borderVisible: true,
        borderColor: "#5579a8",
        textColor: "#bed1f4",
        alignLabels: true,
        ticksVisible: true,
        entireTextOnly: false,
        minimumWidth: 72,
        ensureEdgeTickMarksVisible: true,
      });
    } catch {
      // optional pane/scale adjustment
    }

    try {
      const rsiScale = chart.priceScale("left", 1);
      rsiScale.applyOptions({
        visible: showRsi,
        autoScale: false,
        scaleMargins: { top: 0.12, bottom: 0.12 },
        borderVisible: true,
        borderColor: showRsi ? "#4fc3ff" : "#2b3c56",
        textColor: "#9fdcff",
        ticksVisible: true,
        alignLabels: true,
      });
      if (showRsi) {
        rsiScale.setVisibleRange({ from: 0, to: 100 });
      }
    } catch {
      // optional pane/scale adjustment
    }

    try {
      const natrScale = chart.priceScale("left", 2);
      const maxNatr = (Array.isArray(natrPoints) ? natrPoints : []).reduce((maxValue, point) => {
        const value = Number(point?.value);
        if (!Number.isFinite(value)) {
          return maxValue;
        }
        return Math.max(maxValue, value);
      }, 0);
      natrScale.applyOptions({
        visible: showNatr,
        autoScale: false,
        scaleMargins: { top: 0.12, bottom: 0.12 },
        borderVisible: true,
        borderColor: showNatr ? "#ffb04a" : "#3f3527",
        textColor: "#ffd6a0",
        ticksVisible: true,
        alignLabels: true,
      });
      if (showNatr) {
        natrScale.setVisibleRange({ from: 0, to: Math.max(4, maxNatr * 1.35) });
      }
    } catch {
      // optional pane/scale adjustment
    }

    try {
      const panes = chart.panes();
      if (Array.isArray(panes)) {
        panes[0]?.setStretchFactor(CHART_PANE_STRETCH.price);
        panes[1]?.setStretchFactor(CHART_PANE_STRETCH.rsi);
        panes[2]?.setStretchFactor(CHART_PANE_STRETCH.natr);
      }
    } catch {
      // optional pane sizing
    }

    applyPaneVisualFrames();
  };

  const refreshRsiChartSeries = () => {
    const rsiSeries = rsiSeriesRef.current;
    if (!rsiSeries) {
      return [];
    }

    if (!showRsiInChartRef.current) {
      rsiSeries.applyOptions({ visible: false });
      rsiSeries.setData([]);
      return [];
    }

    const period = Math.max(2, Math.trunc(Number(rsiPeriodRef.current) || 14));
    const rsiPoints = computeRsiSeriesWilder(candlesRef.current, period);
    rsiSeries.applyOptions({ visible: true });
    rsiSeries.setData(rsiPoints);
    return rsiPoints;
  };

  const refreshNatrChartSeries = () => {
    const natrSeries = natrSeriesRef.current;
    if (!natrSeries) {
      return [];
    }

    if (!showNatrInChartRef.current) {
      natrSeries.applyOptions({ visible: false });
      natrSeries.setData([]);
      return [];
    }

    const period = Math.max(2, Math.trunc(Number(natrPeriodRef.current) || 14));
    const natrPoints = computeNatrSeriesWilder(candlesRef.current, period);
    natrSeries.applyOptions({ visible: true });
    natrSeries.setData(natrPoints);
    return natrPoints;
  };

  const applyLiveTickToSelectedChart = (row) => {
    const selectedId = selectedRowIdRef.current;
    if (!selectedId || !row || row.id !== selectedId) {
      return;
    }

    const price = Number(row.last_price);
    if (!Number.isFinite(price)) {
      return;
    }

    const candleSeries = candleSeriesRef.current;
    if (!candleSeries) {
      return;
    }

    applyChartPriceFormatFromValue(price);

    const tickMs = parseTickerTimeToMs(row.ticker_timestamp_utc);
    const bucketMs = timeframeToMs(chartTimeframeRef.current);
    const nextCandles = mergeTickIntoCandles(candlesRef.current, price, tickMs, bucketMs);
    candlesRef.current = nextCandles;

    const lastCandle = nextCandles[nextCandles.length - 1];
    if (lastCandle) {
      candleSeries.update(lastCandle);
    }
    const rsiPoints = refreshRsiChartSeries();
    const natrPoints = refreshNatrChartSeries();
    updateIndicatorScale(rsiPoints, natrPoints);
  };

  const clearReconnectTimer = () => {
    if (reconnectTimerRef.current !== null) {
      window.clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
    }
  };

  const scheduleReconnect = (reason) => {
    if (manualDisconnectRef.current) {
      return;
    }
    if (reconnectTimerRef.current !== null) {
      return;
    }
    if (!hasStartedRef.current && pendingRef.current.length === 0) {
      return;
    }

    reconnectAttemptRef.current += 1;
    const delay = Math.min(
      WS_RECONNECT_BASE_MS * 2 ** Math.max(reconnectAttemptRef.current - 1, 0),
      WS_RECONNECT_MAX_MS,
    );
    const seconds = Math.max(1, Math.round(delay / 1000));
    if (reason) {
      pushError(`Live connection lost (${reason}). Reconnecting in ${seconds}s...`);
    } else {
      pushError(`Live connection lost. Reconnecting in ${seconds}s...`);
    }

    reconnectTimerRef.current = window.setTimeout(() => {
      reconnectTimerRef.current = null;
      shouldResyncOnOpenRef.current = hasStartedRef.current;
      connectSocket();
    }, delay);
  };

  const connectSocket = () => {
    const current = wsRef.current;
    if (current && (current.readyState === WebSocket.OPEN || current.readyState === WebSocket.CONNECTING)) {
      return;
    }

    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => {
      clearReconnectTimer();
      reconnectAttemptRef.current = 0;
      lastServerEventMsRef.current = Date.now();
      setConnected(true);

      if (shouldResyncOnOpenRef.current && hasStartedRef.current) {
        const latestUniverse = universeConfigRef.current;
        const latestFilters = filterConfigRef.current;
        if (latestUniverse && latestFilters && Array.isArray(latestUniverse.exchanges) && latestUniverse.exchanges.length > 0) {
          ws.send(
            JSON.stringify({
              type: "start",
              payload: {
                ...latestUniverse,
                filters: latestFilters,
              },
            }),
          );
        }
      }
      shouldResyncOnOpenRef.current = false;

      const pending = [...pendingRef.current];
      pendingRef.current = [];
      for (const message of pending) {
        ws.send(message);
      }
    };

    ws.onclose = (event) => {
      if (wsRef.current === ws) {
        wsRef.current = null;
      }
      setConnected(false);
      setEngineState("disconnected");
      engineStateRef.current = "disconnected";

      if (manualDisconnectRef.current) {
        manualDisconnectRef.current = false;
        return;
      }

      const reasonText =
        typeof event?.reason === "string" && event.reason.trim()
          ? event.reason.trim()
          : `code ${event?.code ?? "unknown"}`;
      scheduleReconnect(reasonText);
    };

    ws.onerror = () => {
      pushError("WebSocket error");
    };

    ws.onmessage = (event) => {
      lastServerEventMsRef.current = Date.now();
      try {
        const payload = JSON.parse(event.data);
        handleServerMessage(payload);
      } catch (err) {
        pushError(`Invalid server message: ${String(err)}`);
      }
    };
  };

  const send = (message) => {
    const raw = JSON.stringify(message);
    const ws = wsRef.current;
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(raw);
      return;
    }
    pendingRef.current.push(raw);
    connectSocket();
  };
  sendRef.current = send;

  const handleServerMessage = (payload) => {
    if (payload.type === "status") {
      const nextState = payload.state || "unknown";
      setEngineState(nextState);
      engineStateRef.current = nextState;
      return;
    }

    if (payload.type === "error") {
      pushError(payload.message || "Unknown backend error");
      return;
    }

    if (payload.type === "diff") {
      const added = payload.added || [];
      const updated = payload.updated || [];
      const removed = payload.removed || [];

      const prevRows = rowsByIdRef.current;
      const prevDirections = priceDirectionByIdRef.current;
      const nextRows = { ...prevRows };
      const nextDirections = { ...prevDirections };

      for (const row of added) {
        nextRows[row.id] = row;
        if (!nextDirections[row.id]) {
          nextDirections[row.id] = "flat";
        }
      }

      for (const row of updated) {
        const previousRow = prevRows[row.id];
        const previousPrice = Number(previousRow?.last_price);
        const nextPrice = Number(row?.last_price);
        if (Number.isFinite(previousPrice) && Number.isFinite(nextPrice)) {
          if (nextPrice > previousPrice) {
            nextDirections[row.id] = "up";
          } else if (nextPrice < previousPrice) {
            nextDirections[row.id] = "down";
          } else if (!nextDirections[row.id]) {
            nextDirections[row.id] = "flat";
          }
        } else if (!nextDirections[row.id]) {
          nextDirections[row.id] = "flat";
        }
        nextRows[row.id] = row;
      }

      for (const rowId of removed) {
        delete nextRows[rowId];
        delete nextDirections[rowId];
      }

      rowsByIdRef.current = nextRows;
      priceDirectionByIdRef.current = nextDirections;
      setRowsById(nextRows);
      setPriceDirectionById(nextDirections);

      const selectedId = selectedRowIdRef.current;
      if (selectedId) {
        const liveRow =
          updated.find((item) => item?.id === selectedId) || added.find((item) => item?.id === selectedId);
        if (liveRow) {
          applyLiveTickToSelectedChart(liveRow);
        }
      }

      setLastUpdate(payload.server_time_utc || "-");
      setStats(payload.stats || null);
      if (Array.isArray(payload.errors) && payload.errors.length > 0) {
        setErrors((prev) => {
          const unique = [];
          for (const message of [...payload.errors, ...prev]) {
            const normalized = String(message || "").trim();
            if (!normalized || unique.includes(normalized)) {
              continue;
            }
            unique.push(normalized);
            if (unique.length >= 8) {
              break;
            }
          }
          return unique;
        });
      }
    }
  };

  const startEngine = () => {
    if (universeConfig.exchanges.length === 0) {
      pushError("Please select at least one exchange");
      return;
    }

    const universeKey = JSON.stringify(universeConfig);
    const filterKey = JSON.stringify(filterConfig);
    lastUniverseKeyRef.current = universeKey;
    lastFilterKeyRef.current = filterKey;
    hasStartedRef.current = true;
    manualDisconnectRef.current = false;
    shouldResyncOnOpenRef.current = false;
    clearReconnectTimer();
    reconnectAttemptRef.current = 0;
    lastServerEventMsRef.current = Date.now();

    rowsByIdRef.current = {};
    priceDirectionByIdRef.current = {};
    setRowsById({});
    setPriceDirectionById({});
    setSelectedRowId(null);
    setDrawingMode(false);
    setChartLoading(false);
    candlesRef.current = [];
    candleSeriesRef.current?.setData([]);
    rsiSeriesRef.current?.setData([]);
    natrSeriesRef.current?.setData([]);
    updateIndicatorScale([], []);
    clearDrawings();
    setChartError("");
    setErrors([]);
    send({
      type: "start",
      payload: {
        ...universeConfig,
        filters: filterConfig,
      },
    });
  };

  const updateFilters = () => {
    const filterKey = JSON.stringify(filterConfig);
    lastFilterKeyRef.current = filterKey;
    send({
      type: "update_filters",
      payload: {
        filters: filterConfig,
      },
    });
  };

  const pause = () => send({ type: "pause", payload: {} });
  const resume = () => send({ type: "resume", payload: {} });
  const stop = () => {
    manualDisconnectRef.current = true;
    shouldResyncOnOpenRef.current = false;
    clearReconnectTimer();
    reconnectAttemptRef.current = 0;
    pendingRef.current = [];
    hasStartedRef.current = false;
    lastUniverseKeyRef.current = "";
    lastFilterKeyRef.current = "";
    rowsByIdRef.current = {};
    priceDirectionByIdRef.current = {};
    setRowsById({});
    setPriceDirectionById({});
    setSelectedRowId(null);
    setDrawingMode(false);
    setChartLoading(false);
    candlesRef.current = [];
    candleSeriesRef.current?.setData([]);
    rsiSeriesRef.current?.setData([]);
    natrSeriesRef.current?.setData([]);
    updateIndicatorScale([], []);
    clearDrawings();
    setChartError("");
    setStats(null);
    setLastUpdate("-");
    setErrors([]);
    const ws = wsRef.current;
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
      send({ type: "stop", payload: {} });
    }
  };

  useEffect(() => {
    if (selectedExchanges.length > 0) {
      return;
    }

    const hadData = Object.keys(rowsByIdRef.current).length > 0;
    const wasRunning = hasStartedRef.current;
    if (!hadData && !wasRunning) {
      return;
    }

    hasStartedRef.current = false;
    manualDisconnectRef.current = true;
    shouldResyncOnOpenRef.current = false;
    clearReconnectTimer();
    reconnectAttemptRef.current = 0;
    pendingRef.current = [];
    lastUniverseKeyRef.current = "";
    lastFilterKeyRef.current = "";
    rowsByIdRef.current = {};
    priceDirectionByIdRef.current = {};
    setRowsById({});
    setPriceDirectionById({});
    setSelectedRowId(null);
    setDrawingMode(false);
    setChartLoading(false);
    candlesRef.current = [];
    candleSeriesRef.current?.setData([]);
    rsiSeriesRef.current?.setData([]);
    natrSeriesRef.current?.setData([]);
    updateIndicatorScale([], []);
    clearDrawings();
    setChartError("");
    setStats(null);
    setLastUpdate("-");
    setErrors([]);
    const ws = wsRef.current;
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
      sendRef.current?.({ type: "stop", payload: {} });
    }
  }, [selectedExchanges]);

  useEffect(() => {
    if (universeConfig.exchanges.length === 0) {
      return;
    }

    const universeKey = JSON.stringify(universeConfig);
    const timeoutId = window.setTimeout(() => {
      const shouldRestart = !hasStartedRef.current || universeKey !== lastUniverseKeyRef.current;
      if (!shouldRestart) {
        return;
      }

      const currentFilters = filterConfigRef.current;
      if (!currentFilters) {
        return;
      }
      lastUniverseKeyRef.current = universeKey;
      lastFilterKeyRef.current = JSON.stringify(currentFilters);
      hasStartedRef.current = true;
      manualDisconnectRef.current = false;
      shouldResyncOnOpenRef.current = false;
      clearReconnectTimer();
      reconnectAttemptRef.current = 0;
      lastServerEventMsRef.current = Date.now();
      rowsByIdRef.current = {};
      priceDirectionByIdRef.current = {};
      setRowsById({});
      setPriceDirectionById({});
      setSelectedRowId(null);
      setDrawingMode(false);
      setChartLoading(false);
      candlesRef.current = [];
      candleSeriesRef.current?.setData([]);
      rsiSeriesRef.current?.setData([]);
      natrSeriesRef.current?.setData([]);
      updateIndicatorScale([], []);
      clearDrawings();
      setChartError("");
      setErrors([]);
      sendRef.current?.({
        type: "start",
        payload: {
          ...universeConfig,
          filters: currentFilters,
        },
      });
    }, 550);

    return () => {
      window.clearTimeout(timeoutId);
    };
  }, [universeConfig]);

  useEffect(() => {
    selectedRowIdRef.current = selectedRowId;
  }, [selectedRowId]);

  useEffect(() => {
    engineStateRef.current = engineState;
  }, [engineState]);

  useEffect(() => {
    chartTimeframeRef.current = chartTimeframe;
  }, [chartTimeframe]);

  useEffect(() => {
    rsiPeriodRef.current = Math.max(2, Math.trunc(toFiniteNumber(rsiPeriod, 14)));
  }, [rsiPeriod]);

  useEffect(() => {
    showRsiInChartRef.current = showRsiInChart;
  }, [showRsiInChart]);

  useEffect(() => {
    natrPeriodRef.current = Math.max(2, Math.trunc(toFiniteNumber(natrPeriod, 14)));
  }, [natrPeriod]);

  useEffect(() => {
    showNatrInChartRef.current = showNatrInChart;
  }, [showNatrInChart]);

  useEffect(() => {
    drawingModeRef.current = drawingMode;
    if (!drawingMode) {
      drawingStartRef.current = null;
      setDrawingAnchorSet(false);
    }
  }, [drawingMode]);

  useEffect(() => {
    const container = chartContainerRef.current;
    if (!container) {
      return;
    }

    const chart = createChart(container, {
      width: Math.max(container.clientWidth || 640, 320),
      height: CHART_TOTAL_HEIGHT,
      layout: {
        background: { color: "#0a111d" },
        textColor: "#bed1f4",
        panes: {
          enableResize: true,
          separatorColor: "rgba(88, 118, 154, 0.95)",
          separatorHoverColor: "rgba(132, 182, 247, 0.55)",
        },
      },
      grid: {
        vertLines: { color: "#16263f" },
        horzLines: { color: "#16263f" },
      },
      rightPriceScale: {
        visible: true,
        ticksVisible: true,
        borderColor: "#213656",
        textColor: "#bed1f4",
        entireTextOnly: false,
        minimumWidth: 72,
        ensureEdgeTickMarksVisible: true,
      },
      timeScale: {
        borderColor: "#213656",
        timeVisible: true,
        secondsVisible: false,
      },
      crosshair: {
        mode: 0,
      },
    });

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#2ed078",
      borderUpColor: "#2ed078",
      wickUpColor: "#2ed078",
      downColor: "#f36c74",
      borderDownColor: "#f36c74",
      wickDownColor: "#f36c74",
      priceScaleId: "right",
      priceFormat: {
        type: "price",
        precision: 2,
        minMove: 0.01,
      },
      priceLineVisible: true,
      lastValueVisible: true,
    }, 0);
    const rsiSeries = chart.addSeries(
      LineSeries,
      {
        color: "#4fc3ff",
        lineWidth: 2.5,
        priceScaleId: "left",
        priceLineVisible: false,
        lastValueVisible: true,
        crosshairMarkerVisible: true,
        visible: false,
      },
      1,
    );
    const natrSeries = chart.addSeries(
      LineSeries,
      {
        color: "#ffb04a",
        lineWidth: 2.5,
        priceScaleId: "left",
        priceLineVisible: false,
        lastValueVisible: true,
        crosshairMarkerVisible: true,
        visible: false,
      },
      2,
    );
    rsiSeries.createPriceLine({
      price: 70,
      color: "#f36c74",
      lineWidth: 1,
      lineStyle: 2,
      axisLabelVisible: true,
      title: "70",
    });
    rsiSeries.createPriceLine({
      price: 30,
      color: "#2ed078",
      lineWidth: 1,
      lineStyle: 2,
      axisLabelVisible: true,
      title: "30",
    });
    natrSeries.createPriceLine({
      price: 0.8,
      color: "#6ab4ff",
      lineWidth: 1,
      lineStyle: 2,
      axisLabelVisible: true,
      title: "0.8",
    });
    natrSeries.createPriceLine({
      price: 1.8,
      color: "#2ed078",
      lineWidth: 1,
      lineStyle: 2,
      axisLabelVisible: true,
      title: "1.8",
    });
    natrSeries.createPriceLine({
      price: 3.5,
      color: "#f36c74",
      lineWidth: 1,
      lineStyle: 2,
      axisLabelVisible: true,
      title: "3.5",
    });

    chartApiRef.current = chart;
    candleSeriesRef.current = candleSeries;
    rsiSeriesRef.current = rsiSeries;
    natrSeriesRef.current = natrSeries;
    chartPriceDigitsRef.current = 2;
    candlesRef.current = [];
    candleSeries.setData([]);
    rsiSeries.setData([]);
    natrSeries.setData([]);
    updateIndicatorScale([], []);

    const handleChartClick = (param) => {
      if (!drawingModeRef.current) {
        return;
      }
      if (!param?.point || param.time === undefined || param.time === null) {
        return;
      }

      const price = candleSeries.coordinateToPrice(param.point.y);
      const time = Number(param.time);
      if (!Number.isFinite(price) || !Number.isFinite(time)) {
        return;
      }

      const start = drawingStartRef.current;
      if (!start) {
        drawingStartRef.current = { time: Math.trunc(time), price };
        setDrawingAnchorSet(true);
        return;
      }

      const end = { time: Math.trunc(time), price };
      const lineSeries = chart.addSeries(LineSeries, {
        color: "#ffcf5b",
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: false,
      });
      const points =
        start.time <= end.time
          ? [
              { time: start.time, value: start.price },
              { time: end.time, value: end.price },
            ]
          : [
              { time: end.time, value: end.price },
              { time: start.time, value: start.price },
            ];
      lineSeries.setData(points);
      drawingSeriesRef.current = [...drawingSeriesRef.current, lineSeries];
      setDrawingCount(drawingSeriesRef.current.length);
      drawingStartRef.current = null;
      setDrawingAnchorSet(false);
    };

    chart.subscribeClick(handleChartClick);

    let cleanupResize = () => {};
    if (typeof ResizeObserver !== "undefined") {
      const resizeObserver = new ResizeObserver((entries) => {
        const rect = entries[0]?.contentRect;
        if (!rect) {
          return;
        }
        chart.applyOptions({
          width: Math.max(Math.floor(rect.width), 320),
          height: CHART_TOTAL_HEIGHT,
        });
      });
      resizeObserver.observe(container);
      cleanupResize = () => resizeObserver.disconnect();
    } else {
      const onResize = () => {
        chart.applyOptions({
          width: Math.max(Math.floor(container.clientWidth || 640), 320),
          height: CHART_TOTAL_HEIGHT,
        });
      };
      window.addEventListener("resize", onResize);
      cleanupResize = () => window.removeEventListener("resize", onResize);
    }

    return () => {
      chart.unsubscribeClick(handleChartClick);
      cleanupResize();
      for (const series of drawingSeriesRef.current) {
        try {
          chart.removeSeries(series);
        } catch {
          // ignore during teardown
        }
      }
      drawingSeriesRef.current = [];
      drawingStartRef.current = null;
      chart.remove();
      chartApiRef.current = null;
      candleSeriesRef.current = null;
      rsiSeriesRef.current = null;
      natrSeriesRef.current = null;
      candlesRef.current = [];
      setDrawingCount(0);
      setDrawingAnchorSet(false);
    };
  }, []);

  useEffect(() => {
    const row = selectedRowId ? rowsByIdRef.current[selectedRowId] : null;
    if (!row) {
      setChartLoading(false);
      setChartError("");
      setDrawingMode(false);
      candlesRef.current = [];
      candleSeriesRef.current?.setData([]);
      rsiSeriesRef.current?.setData([]);
      natrSeriesRef.current?.setData([]);
      rsiSeriesRef.current?.applyOptions({ visible: false });
      natrSeriesRef.current?.applyOptions({ visible: false });
      updateIndicatorScale([], []);
      clearDrawings();
      return;
    }

    let active = true;
    const controller = new AbortController();

    const loadCandles = async () => {
      setChartLoading(true);
      setChartError("");
      clearDrawings();
      candlesRef.current = [];
      candleSeriesRef.current?.setData([]);
      rsiSeriesRef.current?.setData([]);
      natrSeriesRef.current?.setData([]);
      applyChartPriceFormatFromValue(row.last_price);
      resetChartViewport();
      try {
        const params = new URLSearchParams({
          exchange_id: String(row.exchange_id || ""),
          symbol: String(row.symbol || ""),
          timeframe: chartTimeframe,
          limit: String(CHART_CANDLE_MAX_POINTS),
        });
        const response = await fetch(`${API_BASE_URL}/chart/ohlcv?${params.toString()}`, {
          signal: controller.signal,
        });
        if (!response.ok) {
          let detail = `HTTP ${response.status}`;
          try {
            const payload = await response.json();
            if (payload?.detail) {
              detail = String(payload.detail);
            }
          } catch {
            // best-effort error payload parsing
          }
          throw new Error(detail);
        }

        const payload = await response.json();
        const normalized = Array.isArray(payload?.candles)
          ? payload.candles.map((item) => normalizeCandle(item)).filter(Boolean)
          : [];

        if (!active) {
          return;
        }

        candlesRef.current = normalized.slice(-CHART_CANDLE_MAX_POINTS);
        candleSeriesRef.current?.setData(candlesRef.current);
        const latestClose =
          candlesRef.current.length > 0
            ? candlesRef.current[candlesRef.current.length - 1].close
            : row.last_price;
        applyChartPriceFormatFromValue(latestClose);
        resetChartViewport();
        window.requestAnimationFrame(() => {
          resetChartViewport();
        });
        applyLiveTickToSelectedChart(row);
      } catch (error) {
        const isAbort =
          typeof error === "object" &&
          error !== null &&
          "name" in error &&
          error.name === "AbortError";
        if (!active || isAbort) {
          return;
        }

        setChartError(`Chart load failed: ${String(error)}`);
        candlesRef.current = [];
        candleSeriesRef.current?.setData([]);
        rsiSeriesRef.current?.setData([]);
        natrSeriesRef.current?.setData([]);
        updateIndicatorScale([], []);
      } finally {
        if (active) {
          setChartLoading(false);
        }
      }
    };

    loadCandles();

    return () => {
      active = false;
      controller.abort();
    };
  }, [selectedRowId, chartTimeframe]);

  useEffect(() => {
    if (!selectedRowId) {
      return;
    }
    if (rowsById[selectedRowId]) {
      return;
    }
    setSelectedRowId(null);
  }, [rowsById, selectedRowId]);

  useEffect(() => {
    const rsiPoints = refreshRsiChartSeries();
    const natrPoints = refreshNatrChartSeries();
    updateIndicatorScale(rsiPoints, natrPoints);
  }, [showRsiInChart, rsiPeriod, showNatrInChart, natrPeriod, selectedRowId]);

  useEffect(() => {
    if (!hasStartedRef.current) {
      return;
    }

    const filterKey = JSON.stringify(filterConfig);
    if (filterKey === lastFilterKeyRef.current) {
      return;
    }

    const timeoutId = window.setTimeout(() => {
      const nextFilterKey = JSON.stringify(filterConfig);
      if (nextFilterKey === lastFilterKeyRef.current) {
        return;
      }
      lastFilterKeyRef.current = nextFilterKey;
      sendRef.current?.({
        type: "update_filters",
        payload: {
          filters: filterConfig,
        },
      });
    }, 250);

    return () => {
      window.clearTimeout(timeoutId);
    };
  }, [filterConfig, engineState]);

  useEffect(() => {
    const intervalId = window.setInterval(() => {
      if (!hasStartedRef.current) {
        return;
      }
      if (manualDisconnectRef.current) {
        return;
      }

      const state = engineStateRef.current;
      if (["paused", "stopped", "idle", "disconnected"].includes(state)) {
        return;
      }

      const ws = wsRef.current;
      if (!ws || ws.readyState !== WebSocket.OPEN) {
        scheduleReconnect("socket not open");
        return;
      }

      const staleMs = Date.now() - lastServerEventMsRef.current;
      if (staleMs < WS_STALE_TIMEOUT_MS) {
        return;
      }

      shouldResyncOnOpenRef.current = true;
      pushError(`No live updates for ${Math.round(staleMs / 1000)}s. Reconnecting...`);
      try {
        ws.close();
      } catch {
        // best effort close
      }
    }, WS_STALE_CHECK_INTERVAL_MS);

    return () => {
      window.clearInterval(intervalId);
    };
  }, []);

  useEffect(() => {
    return () => {
      manualDisconnectRef.current = true;
      shouldResyncOnOpenRef.current = false;
      clearReconnectTimer();
      pendingRef.current = [];
      const ws = wsRef.current;
      if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
        try {
          ws.close();
        } catch {
          // ignore close errors on unmount
        }
      }
      wsRef.current = null;
    };
  }, []);

  return (
    <div className="app-shell">
      <header className="topbar">
        <h1>CEX Live Screener - Phase 1</h1>
        <div className="status-block">
          <span className={`dot ${connected ? "ok" : "off"}`} />
          <span>{connected ? "Connected" : "Disconnected"}</span>
          <span className="state-pill">{engineState}</span>
          <span>Last update: {lastUpdate}</span>
        </div>
      </header>

      <section className="controls-grid">
        <div className="panel">
          <h2>Universe</h2>
          <div className="exchange-field">
            <div className="field-label">Exchanges</div>
            <input
              value={exchangeQuery}
              onChange={(e) => setExchangeQuery(e.target.value)}
              placeholder="Search exchange (case-insensitive, typo-tolerant)"
            />
            <div className="exchange-actions">
              <button type="button" onClick={selectAllExchanges} disabled={allExchanges.length === 0}>
                Select all
              </button>
              <button type="button" onClick={clearSelectedExchanges} disabled={selectedExchanges.length === 0}>
                Clear
              </button>
              <span>{selectedExchanges.length} selected</span>
            </div>
            <div className="selected-exchanges">
              {selectedExchanges.length === 0 && <span className="empty-note">No exchanges selected</span>}
              {selectedExchanges.map((exchangeId) => (
                <button
                  key={exchangeId}
                  type="button"
                  className="chip"
                  onClick={() => toggleExchange(exchangeId)}
                  title={`Remove ${exchangeId}`}
                >
                  {exchangeId} x
                </button>
              ))}
            </div>
            <div className="exchange-dropdown">
              {filteredExchangeOptions.length === 0 && (
                <div className="exchange-empty">No matching exchanges.</div>
              )}
              {filteredExchangeOptions.slice(0, 200).map((exchangeId) => {
                const selected = selectedExchanges.includes(exchangeId);
                return (
                  <button
                    key={exchangeId}
                    type="button"
                    className={`exchange-option ${selected ? "selected" : ""}`}
                    onClick={() => toggleExchange(exchangeId)}
                  >
                    <span>{exchangeId}</span>
                    {selected && <span className="selected-tag">Selected</span>}
                  </button>
                );
              })}
            </div>
            {filteredExchangeOptions.length > 200 && (
              <div className="meta-note">Showing first 200 matches.</div>
            )}
            {exchangeMetaLoading && <div className="meta-note">Loading exchange list from backend...</div>}
            {exchangeMetaError && <div className="meta-error">{exchangeMetaError}</div>}
          </div>
          <label>
            Market type
            <select value={marketType} onChange={(e) => setMarketType(e.target.value)}>
              <option value="both">both</option>
              <option value="perpetual">perpetual</option>
              <option value="spot">spot</option>
            </select>
          </label>
          <label>
            Pair search
            <input value={pairSearch} onChange={(e) => setPairSearch(e.target.value)} />
          </label>
          <div className="row-2">
            <label>
              Max pairs / exchange
              <input type="number" value={maxPairs} onChange={(e) => setMaxPairs(e.target.value)} />
            </label>
            <label>
              Tick interval sec
              <input type="number" step="0.2" value={tickInterval} onChange={(e) => setTickInterval(e.target.value)} />
            </label>
          </div>
          <label>
            Universe refresh sec
            <input type="number" value={universeRefresh} onChange={(e) => setUniverseRefresh(e.target.value)} />
          </label>
        </div>

        <div className="panel">
          <h2>Filters</h2>
          <div className="row-2">
            <label>
              Min 24h %
              <input type="number" step="0.1" value={minPct} onChange={(e) => setMinPct(e.target.value)} />
            </label>
            <label>
              Max 24h %
              <input type="number" step="0.1" value={maxPct} onChange={(e) => setMaxPct(e.target.value)} />
            </label>
          </div>
          <div className="row-2">
            <label>
              Min quote vol
              <input type="number" value={minVol} onChange={(e) => setMinVol(e.target.value)} />
            </label>
            <label>
              Max quote vol (0=no cap)
              <input type="number" value={maxVol} onChange={(e) => setMaxVol(e.target.value)} />
            </label>
          </div>
          <label>
            Symbol contains
            <input value={symbolContains} onChange={(e) => setSymbolContains(e.target.value)} />
          </label>
          <div className="rsi-section">
            <div className="field-label">RSI filter (multi-select)</div>
            <div className="rsi-mode-grid">
              <button
                type="button"
                className={`rsi-mode-btn ${rsiLt30Enabled ? "active" : ""}`}
                onClick={() => setRsiLt30Enabled((prev) => !prev)}
              >
                RSI &lt; 30
              </button>
              <button
                type="button"
                className={`rsi-mode-btn ${rsiGt70Enabled ? "active" : ""}`}
                onClick={() => setRsiGt70Enabled((prev) => !prev)}
              >
                RSI &gt; 70
              </button>
              <button
                type="button"
                className={`rsi-mode-btn ${rsiBetweenEnabled ? "active" : ""}`}
                onClick={() => setRsiBetweenEnabled((prev) => !prev)}
              >
                RSI 30 - 70
              </button>
            </div>
            <div className="row-2">
              <label>
                RSI timeframe
                <select value={rsiTimeframe} onChange={(e) => setRsiTimeframe(e.target.value)}>
                  <option value="1m">1m</option>
                  <option value="5m">5m</option>
                  <option value="15m">15m</option>
                  <option value="1h">1h</option>
                  <option value="4h">4h</option>
                  <option value="1d">1d</option>
                </select>
              </label>
              <label>
                RSI length
                <input
                  type="number"
                  min="2"
                  max="200"
                  value={rsiPeriod}
                  onChange={(e) => setRsiPeriod(e.target.value)}
                />
              </label>
            </div>
          </div>
          <div className="natr-section">
            <div className="field-label">NATR filter (multi-select)</div>
            <div className="natr-hint">
              Scalper: 1m-5m | Day trader: 15m-1h | Swing: 4h | Long-term: 1d
            </div>
            <div className="natr-mode-grid">
              <button
                type="button"
                className={`natr-mode-btn ${natrCompressionEnabled ? "active" : ""}`}
                onClick={() => setNatrCompressionEnabled((prev) => !prev)}
              >
                &lt; 0.8 Compression
              </button>
              <button
                type="button"
                className={`natr-mode-btn ${natrNormalEnabled ? "active" : ""}`}
                onClick={() => setNatrNormalEnabled((prev) => !prev)}
              >
                0.8 - 1.8 Normal
              </button>
              <button
                type="button"
                className={`natr-mode-btn ${natrHighEnabled ? "active" : ""}`}
                onClick={() => setNatrHighEnabled((prev) => !prev)}
              >
                &gt; 1.8 High Vol
              </button>
              <button
                type="button"
                className={`natr-mode-btn ${natrExtremeEnabled ? "active" : ""}`}
                onClick={() => setNatrExtremeEnabled((prev) => !prev)}
              >
                &gt; 3.5 Extreme
              </button>
            </div>
            <div className="row-2">
              <label>
                NATR timeframe
                <select value={natrTimeframe} onChange={(e) => setNatrTimeframe(e.target.value)}>
                  <option value="1m">1m</option>
                  <option value="3m">3m</option>
                  <option value="5m">5m</option>
                  <option value="15m">15m</option>
                  <option value="30m">30m</option>
                  <option value="1h">1h</option>
                  <option value="4h">4h</option>
                  <option value="1d">1d</option>
                </select>
              </label>
              <label>
                NATR length
                <input
                  type="number"
                  min="2"
                  max="200"
                  value={natrPeriod}
                  onChange={(e) => setNatrPeriod(e.target.value)}
                />
              </label>
            </div>
          </div>
          <div className="btn-row">
            <button onClick={startEngine}>Restart Now</button>
            <button onClick={updateFilters}>Apply Filters Now</button>
            <button onClick={pause}>Pause</button>
            <button onClick={resume}>Resume</button>
            <button onClick={stop}>Stop</button>
          </div>
        </div>

        <div className="panel stats">
          <h2>Runtime Stats</h2>
          <div>Visible rows: {stats?.visible_count ?? sortedRows.length}</div>
          <div>Tracked rows: {stats?.tracked_rows ?? "-"}</div>
          <div>Added: {stats?.added_count ?? "-"}</div>
          <div>Updated: {stats?.updated_count ?? "-"}</div>
          <div>Removed: {stats?.removed_count ?? "-"}</div>
          <div>Paused: {String(stats?.paused ?? false)}</div>
        </div>
      </section>

      {errors.length > 0 && (
        <section className="errors">
          <h3>Latest Errors</h3>
          {errors.map((err, idx) => (
            <div key={`${idx}-${err}`} className="error-item">
              {err}
            </div>
          ))}
        </section>
      )}

      <section className="chart-panel">
        <div className="chart-topbar">
          <h2>Pair Chart</h2>
          <div className="chart-actions">
            {selectedRow && (
              <button type="button" className="chart-close-btn" onClick={() => setSelectedRowId(null)}>
                Close
              </button>
            )}
          </div>
        </div>
        {selectedRow && (
          <div className="chart-meta">
            <div>
              {selectedRow.exchange_id} / {selectedRow.symbol}
            </div>
            <div>
              Last:{" "}
              <span className={selectedChartTrendClass}>
                {formatNum(selectedRow.last_price, priceDisplayDigits(selectedRow.last_price))}
              </span>
            </div>
            <div>
              24h%:{" "}
              <span className={Number(selectedRow.pct_change_24h || 0) >= 0 ? "up" : "down"}>
                {formatNum(selectedRow.pct_change_24h, 3)}
              </span>
            </div>
            <div>TF: {chartTimeframe}</div>
            <div>
              RSI chart: {Math.max(2, Math.trunc(toFiniteNumber(rsiPeriod, 14)))} / {chartTimeframe}
            </div>
            <div>
              NATR chart: {Math.max(2, Math.trunc(toFiniteNumber(natrPeriod, 14)))} / {chartTimeframe}
            </div>
          </div>
        )}

        {selectedRow && (
          <>
            <div className="chart-toolbar">
              <div className="timeframe-switch">
                {CHART_TIMEFRAME_OPTIONS.map((option) => (
                  <button
                    key={option}
                    type="button"
                    className={`tf-btn ${chartTimeframe === option ? "active" : ""}`}
                    onClick={() => setChartTimeframe(option)}
                  >
                    {option}
                  </button>
                ))}
              </div>
              <div className="draw-tools">
                <button
                  type="button"
                  className={`draw-btn ${showRsiInChart ? "active" : ""}`}
                  onClick={() => setShowRsiInChart((prev) => !prev)}
                >
                  {showRsiInChart ? "Hide RSI" : "Show RSI"}
                </button>
                <button
                  type="button"
                  className={`draw-btn ${showNatrInChart ? "active" : ""}`}
                  onClick={() => setShowNatrInChart((prev) => !prev)}
                >
                  {showNatrInChart ? "Hide NATR" : "Show NATR"}
                </button>
                <button
                  type="button"
                  className={`draw-btn ${drawingMode ? "active" : ""}`}
                  onClick={() => setDrawingMode((previous) => !previous)}
                >
                  {drawingMode ? "Drawing On" : "Draw Trendline"}
                </button>
                <button type="button" className="draw-btn" onClick={clearDrawings} disabled={drawingCount === 0}>
                  Clear Drawings ({drawingCount})
                </button>
              </div>
            </div>
            <div className="chart-hint">
              {drawingMode
                ? drawingAnchorSet
                  ? "Click the second point to finish the trendline."
                  : "Click the first point to start a trendline."
                : "Enable Draw Trendline to place two-click drawing lines."}
            </div>
            <div className="chart-pane-legend">
              <div className="pane-chip pane-chip-price">
                <span className="pane-chip-title">Price Pane</span>
                <span>Candles</span>
              </div>
              <div className={`pane-chip pane-chip-rsi ${showRsiInChart ? "active" : ""}`}>
                <span className="pane-chip-title">RSI Pane</span>
                <span>{showRsiInChart ? "Visible" : "Hidden"}</span>
              </div>
              <div className={`pane-chip pane-chip-natr ${showNatrInChart ? "active" : ""}`}>
                <span className="pane-chip-title">NATR Pane</span>
                <span>{showNatrInChart ? "Visible" : "Hidden"}</span>
              </div>
            </div>
            {chartError && <div className="meta-error">{chartError}</div>}
          </>
        )}
        {!selectedRow && <div className="chart-empty">Click a pair row below to open its live chart.</div>}
        <div className="chart-canvas">
          <div ref={chartContainerRef} className="chart-root" />
          {!selectedRow && <div className="chart-overlay">Waiting for pair selection</div>}
          {selectedRow && chartLoading && <div className="chart-overlay">Loading candles...</div>}
        </div>
      </section>

      <section className="table-panel">
        <table>
          <thead>
            <tr>
              <th>Exchange</th>
              <th>Symbol</th>
              <th>Type</th>
              <th>Last Price</th>
              <th>24h %</th>
              <th>Quote Vol</th>
              <th>RSI ({Math.max(2, Math.trunc(toFiniteNumber(rsiPeriod, 14)))}/{rsiTimeframe})</th>
              {showNatrColumn && (
                <th>NATR ({Math.max(2, Math.trunc(toFiniteNumber(natrPeriod, 14)))}/{natrTimeframe})</th>
              )}
              <th>Bid</th>
              <th>Ask</th>
              <th>Ticker TS (UTC)</th>
            </tr>
          </thead>
          <tbody>
            {sortedRows.map((row) => {
              const tier = symbolTier(row.symbol);
              return (
                <tr
                  key={row.id}
                  className={`pair-row ${selectedRowId === row.id ? "selected-row" : ""} ${tier ? `tier-${tier}` : ""}`}
                  onClick={() => setSelectedRowId(row.id)}
                >
                  <td>{row.exchange_id}</td>
                  <td>{row.symbol}</td>
                  <td>{row.market_type}</td>
                  <td
                    className={priceDirectionById[row.id] === "up" ? "up" : priceDirectionById[row.id] === "down" ? "down" : ""}
                  >
                    {formatNum(row.last_price, priceDisplayDigits(row.last_price))}
                  </td>
                  <td className={Number(row.pct_change_24h || 0) >= 0 ? "up" : "down"}>
                    {formatNum(row.pct_change_24h, 3)}
                  </td>
                  <td>{formatNum(row.quote_volume_24h, 2)}</td>
                  <td>{formatNum(row.rsi, 2)}</td>
                  {showNatrColumn && <td>{formatNum(row.natr, 2)}</td>}
                  <td>{formatNum(row.bid, 8)}</td>
                  <td>{formatNum(row.ask, 8)}</td>
                  <td>{row.ticker_timestamp_utc || "-"}</td>
                </tr>
              );
            })}
            {sortedRows.length === 0 && (
              <tr>
                <td colSpan={showNatrColumn ? 11 : 10} className="empty-cell">
                  No rows yet. Engine auto-starts after you choose exchanges.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </section>
    </div>
  );
}

export default App;
