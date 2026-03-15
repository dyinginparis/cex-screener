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

## REST Endpoints

- `GET /meta/exchanges`
- `GET /symbols?exchange_id=bybit&market_type=perpetual&search=btc`
- `GET /chart/ohlcv?exchange_id=bybit&symbol=BTC/USDT:USDT&timeframe=1m&limit=300`
