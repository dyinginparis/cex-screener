# Phase 1 Backend

FastAPI websocket backend for live CEX screener updates with diff events.

## Run

```powershell
cd backend
..\.venv\Scripts\python.exe -m pip install -r requirements.txt
..\.venv\Scripts\python.exe -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## WebSocket

Endpoint: `ws://localhost:8000/ws/live`

Client messages:
- `start` with `StartPayload`
- `pause`
- `resume`
- `stop`
- `update_filters` with `UpdateFiltersPayload`

Server messages:
- `status`
- `diff` (`added`, `updated`, `removed`)
- `error`

## Heatmap Endpoints

- `GET /meta/heatmap`
- `GET /heatmap/symbols?search=BTC&limit=120&exchange_id=bybit&market_type=perpetual`
- `GET /heatmap/orderbook?symbol_id=bybit|BTC/USDT:USDT&levels=32&range_bps=300`

Provider modes:
- `HEATMAP_PROVIDER=auto` (default): `coinapi` if key exists, else `ccxt`
- `HEATMAP_PROVIDER=ccxt`: direct exchange snapshot orderbook
- `HEATMAP_PROVIDER=coinapi`: licensed provider path

CoinAPI optional env:

```powershell
$env:COINAPI_API_KEY="your_coinapi_key"
$env:COINAPI_REST_BASE="https://rest.coinapi.io"
$env:HEATMAP_CCXT_DEFAULT_EXCHANGE="bybit"
```
