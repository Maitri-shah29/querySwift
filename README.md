# Approximate Query Engine

Local-first desktop analytics app for approximate query processing.

## Layout

- `backend/`: Go HTTP API using DuckDB as the analytical store and Postgres/CSV as sources
- `frontend/`: React + Vite desktop UI
- `desktop/`: Tauri wrapper that launches the backend and hosts the frontend

## Running

### Backend

```powershell
cd D:\WomenTechies26\backend
go run .\cmd\server
```

### Frontend

```powershell
cd D:\WomenTechies26\frontend
npm install
npm run dev
```

### Desktop

```powershell
cd D:\WomenTechies26\desktop\src-tauri
cargo tauri dev
```
