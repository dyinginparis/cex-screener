# CEX Multi-Market Screener

This repository runs as a two-part live stack:
- `backend/`: FastAPI + CCXT live engine (WebSocket diff events)
- `frontend/`: React UI (stable row updates without full reload)

Streamlit is no longer part of the active workflow.

## Run

1. Install backend deps:
```powershell
.\.venv\Scripts\python.exe -m pip install -r backend\requirements.txt
```

2. Start backend:
```powershell
.\.venv\Scripts\python.exe -m uvicorn backend.app.main:app --reload --host 0.0.0.0 --port 8000
```

3. Install frontend deps:
```powershell
cd frontend
npm install
```

4. Start frontend:
```powershell
npm run dev
```

5. Open:
`http://localhost:5173`

## Notes
- WebSocket endpoint: `ws://localhost:8000/ws/live`
- Exchange metadata endpoint: `http://localhost:8000/meta/exchanges`
- Heatmap provider metadata: `http://localhost:8000/meta/heatmap`
- Heatmap symbols: `http://localhost:8000/heatmap/symbols`
- Heatmap snapshot: `http://localhost:8000/heatmap/orderbook`

## Licensed Heatmap Setup

The new **Orderbook Heatmap** tab uses a licensed provider path (CoinAPI) for commercial-ready data workflows.

Set environment variable before starting backend:

```powershell
$env:COINAPI_API_KEY="your_coinapi_key"
```

Optional:

```powershell
$env:HEATMAP_PROVIDER="coinapi"
$env:COINAPI_REST_BASE="https://rest.coinapi.io"
```
