# Phase 1 Frontend (React)

Minimal React client for the FastAPI websocket backend.

## Run

```powershell
npm install
npm run dev
```

Default websocket endpoint:
- `ws://localhost:8000/ws/live`

Override with env:

```powershell
$env:VITE_WS_URL=\"ws://localhost:8000/ws/live\"
npm run dev
```

