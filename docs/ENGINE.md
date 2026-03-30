# Approximate Query Engine

This document explains how the current engine works end to end.

## 1. What This Project Is

This project is a local-first desktop analytics application built from three layers:

- a Go backend that owns all data ingestion, storage, query execution, approximation, and benchmarking
- a React frontend that acts only as an API client
- a Tauri desktop shell that launches the backend and hosts the frontend

The product goal is approximate query processing for analytical SQL workloads. The app is not a fuzzy-search engine and it is not a general SQL database frontend. It is specifically aimed at:

- faster aggregate queries over large datasets
- exact vs approximate result comparison
- tunable speed vs accuracy trade-offs
- local desktop workflows with CSV files and live Postgres sources

## 2. Core Idea

The engine keeps two versions of each dataset inside DuckDB:

- a raw full table
- a sampled approximation table

When a user runs a query:

- `exact` mode runs on the raw full table
- `approx` mode runs on the sample table
- `compare` mode runs both and reports latency plus result differences

The current approximation strategy is weighted sampling:

- each sampled row carries a synthetic `__aqe_weight`
- approximate aggregates use weighted formulas
- the backend estimates error from sample size and also computes actual error in compare mode

## 3. High-Level Architecture

## 3.1 Backend

The backend lives in `backend/` and is the real engine.

Main responsibilities:

- register data sources
- import CSV data into DuckDB
- mirror Postgres data into DuckDB
- maintain sample tables
- parse and validate the supported SQL subset
- run exact and approximate queries
- benchmark exact vs approximate execution
- expose all of this over a local HTTP API

Main code files:

- `backend/cmd/server/main.go`
- `backend/internal/app/server.go`
- `backend/internal/app/sqlparse.go`
- `backend/internal/app/types.go`

## 3.2 Frontend

The frontend lives in `frontend/` and does not talk directly to DuckDB or Postgres.

It calls backend endpoints for:

- source registration and sync
- query execution
- benchmark execution
- stream control

## 3.3 Desktop Shell

The Tauri shell lives in `desktop/src-tauri/`.

It is intentionally thin. Its job is:

- start the backend executable as a child process
- point it at a local DuckDB file in app data
- host the frontend window
- stop the backend process when the app closes

## 4. Data Model

The engine stores metadata about sources and benchmarks in DuckDB itself.

## 4.1 Source Metadata

The backend creates an internal table:

- `aqe_sources`

This table stores:

- source id
- source kind (`csv` or `postgres`)
- logical table name used by the query engine
- physical raw and sample DuckDB table names
- source-specific connection/file metadata
- sample rate
- stratification columns
- watermark state for Postgres sync
- row counts and last sync time

## 4.2 Benchmark Metadata

The backend creates:

- `aqe_benchmarks`

Each benchmark report is stored as JSON so the frontend can display past runs easily.

## 4.3 Per-Dataset Tables

For each registered dataset, the backend creates:

- `<table>_raw`
- `<table>_sample`

Example:

- logical table name: `sales`
- raw table: `sales_raw`
- sample table: `sales_sample`

The logical name is what the user writes in SQL:

```sql
SELECT region, SUM(revenue) FROM sales GROUP BY region
```

The backend maps that logical name to the physical raw/sample tables internally.

## 5. Source Ingestion

## 5.1 CSV Import

CSV import uses DuckDB's built-in file reader:

```sql
CREATE OR REPLACE TABLE <raw_table> AS
SELECT * FROM read_csv_auto('<path>', HEADER=TRUE)
```

This means:

- schema inference is delegated to DuckDB
- the imported table becomes immediately queryable
- once the raw table is created, the engine builds a sample table from it

After import, the backend:

1. creates or replaces the raw table
2. creates or replaces the sample table
3. computes row counts
4. stores source metadata

## 5.2 Postgres Registration

Postgres sources are not queried directly at runtime for analytical results.

Instead, the backend:

1. connects to Postgres
2. fetches rows from the source table
3. copies them into DuckDB
4. builds a sample table inside DuckDB
5. runs exact and approximate analytics against DuckDB only

This makes compare mode consistent because both exact and approximate paths read from the same analytical store.

## 5.3 Postgres Full Sync

Initial registration performs a full sync:

```sql
SELECT * FROM <schema>.<table>
```

The backend inspects column metadata from Postgres and maps it into a small DuckDB type set:

- integer-like types -> `BIGINT`
- float/numeric types -> `DOUBLE`
- boolean -> `BOOLEAN`
- date -> `DATE`
- timestamp types -> `TIMESTAMP`
- everything else -> `TEXT`

Rows are then inserted into the DuckDB raw table.

## 5.4 Postgres Incremental Sync

The current live-data prototype is polling-based, not CDC-based.

If the source defines a watermark column, incremental sync uses:

```sql
SELECT * FROM <schema>.<table>
WHERE <watermark_column> > $1
ORDER BY <watermark_column>
```

How it works:

- the source stores `last_watermark`
- on each sync, only newer rows are fetched
- if a primary key is configured, rows are staged and merged into the DuckDB raw table
- if no watermark is available, the engine falls back to full sync behavior

This is a practical v1 streaming prototype, not a production replication system.

## 6. Sampling Strategy

The current engine uses sample tables plus row weights.

## 6.1 Uniform Sampling Path

If no stratification columns are configured, the engine uses random row sampling:

```sql
SELECT *, (1.0 / sample_rate) AS __aqe_weight
FROM raw_table
WHERE random() < sample_rate
```

Meaning:

- roughly `sample_rate` fraction of rows are retained
- each row represents `1 / sample_rate` rows from the original dataset

Example:

- sample rate = `0.10`
- each kept row gets weight `10`

## 6.2 Stratified Sampling Path

If stratification columns are configured, the engine samples independently inside each stratum.

It does this with window functions:

- `row_number() over (partition by strata order by random())`
- `count(*) over (partition by strata)`

Then it keeps approximately `sample_rate * strata_size` rows from each stratum, with at least one row per non-empty stratum.

Why this helps:

- small groups are less likely to disappear entirely
- skewed categorical distributions are handled more gracefully
- group-by queries tend to behave better than naive global random sampling

## 6.3 Weight Calculation

For the stratified path, weight is derived from:

- actual rows in the stratum
- sampled rows retained from the stratum

Formula:

- weight = `strata_count / sampled_in_strata`

This lets approximate aggregates scale the sample back toward the original data size.

## 7. Query Support

The engine intentionally supports a narrow analytical SQL subset.

Supported:

- `COUNT(*)`
- `SUM(column)`
- `AVG(column)`
- `GROUP BY`
- selecting grouped dimension columns

Not currently supported:

- joins
- filters
- window functions in user queries
- `COUNT DISTINCT`
- arbitrary expressions
- subqueries
- multiple tables

## 7.1 Parsing

The parser in `sqlparse.go` is regex- and rule-based, not a full SQL parser.

It validates that the query looks like:

```sql
SELECT <select-list>
FROM <table>
[GROUP BY <columns>]
```

It then classifies select expressions into:

- group columns
- `count`
- `sum`
- `avg`

It also rejects invalid shapes, for example:

- selecting a non-aggregated column that is not in `GROUP BY`
- queries with no aggregate at all

## 7.2 Logical Table Resolution

The parser returns the logical table name from the SQL.

The backend then looks up the registered source whose `TableName` matches that logical name and rewrites execution onto:

- raw DuckDB table for exact mode
- sample DuckDB table for approximate mode

## 8. Exact Execution

Exact execution is straightforward:

- parse supported SQL
- build backend-owned SQL over the raw table
- execute against DuckDB
- return rows, schema, and execution time

Example transformation:

User query:

```sql
SELECT region, SUM(revenue) AS total_revenue, COUNT(*) AS total_rows
FROM sales
GROUP BY region
```

Exact execution SQL:

```sql
SELECT "region" AS "region",
       SUM("revenue") AS "total_revenue",
       COUNT(*) AS "total_rows"
FROM "sales_raw"
GROUP BY "region"
```

## 9. Approximate Execution

Approximate execution uses the sample table and weighted formulas.

## 9.1 COUNT

Approximate `COUNT(*)` becomes:

```sql
SUM(__aqe_weight)
```

## 9.2 SUM

Approximate `SUM(x)` becomes:

```sql
SUM(x * __aqe_weight)
```

## 9.3 AVG

Approximate `AVG(x)` becomes:

```sql
SUM(x * __aqe_weight) / NULLIF(SUM(__aqe_weight), 0)
```

## 9.4 GROUP BY

Grouping columns are preserved exactly. The approximation affects only the aggregate computations.

Example transformed approximate SQL:

```sql
SELECT "region" AS "region",
       SUM("revenue" * __aqe_weight) AS "total_revenue",
       SUM(__aqe_weight) AS "total_rows"
FROM "sales_sample"
GROUP BY "region"
```

## 10. Compare Mode

Compare mode runs both paths:

1. exact query on raw table
2. approximate query on sample table

Then it reports:

- exact result rows
- approximate result rows
- exact latency
- approximate latency
- speedup
- estimated error
- actual error

## 10.1 Speedup

Speedup is computed as:

- `exact_millis / approx_millis`

## 10.2 Actual Error

The engine compares numeric fields between exact and approximate result rows.

Current behavior:

- rows are aligned by their non-numeric group-key fields
- numeric values are compared as relative percentage error
- the final reported value is the average percentage error across comparable numeric cells

This is a simple but useful v1 metric.

## 10.3 Estimated Error

The current estimate is based on sample rate and total row count:

- `sqrt((1 - p) / (p * n)) * 100`

Where:

- `p` = sample rate
- `n` = number of rows in the raw table

This is a coarse estimate, not a rigorous confidence interval for every query shape.

## 10.4 Confidence

The UI exposes an accuracy target, but in the current backend it is used only as a light reporting control:

- approximate confidence is set to at least `0.5`
- otherwise it reflects the provided target

In the current implementation, the accuracy target does not yet change sample construction dynamically. It is best understood as a reported target, not a full adaptive query planner.

## 11. Benchmarks

Benchmark mode automates repeated compare runs.

Input:

- benchmark name
- list of queries
- iteration count
- accuracy target

For each query:

1. run compare mode repeatedly
2. average exact time
3. average approximate time
4. average actual error
5. record estimated error and confidence

Output per query:

- exact latency
- approximate latency
- speedup
- estimated error
- actual error
- confidence

The whole report is stored in `aqe_benchmarks` and exposed to the frontend.

## 12. HTTP API

The frontend and desktop shell interact with the backend over localhost HTTP.

## 12.1 Health

- `GET /health`

Returns basic backend status and source count.

## 12.2 Sources

- `GET /sources`
- `POST /sources/csv/import`
- `POST /sources/postgres/register`
- `POST /sources/{id}/sync`
- `POST /sources/{id}/stream/start`
- `POST /sources/{id}/stream/stop`

## 12.3 Queries

- `POST /queries/run`

Request shape:

- `sql`
- `mode`
- `accuracy_target`

## 12.4 Benchmarks

- `GET /benchmarks`
- `POST /benchmarks/run`

## 13. Frontend Flow

The frontend organizes the engine into four user-facing workflows.

## 13.1 Data Sources

Users can:

- register a CSV dataset
- register a Postgres dataset
- set sample rate
- define stratification columns
- define watermark and polling settings for Postgres

## 13.2 Query Studio

Users can:

- choose exact, approximate, or compare mode
- provide the SQL subset supported by the backend
- set a target accuracy slider
- inspect exact and approximate output tables

## 13.3 Benchmarks

Users can:

- define named benchmark runs
- provide one or more queries
- run repeated exact-vs-approx comparisons
- inspect historical results

## 13.4 Streaming

Users can:

- start or stop polling for Postgres sources
- trigger manual sync
- inspect watermark and freshness state

## 14. Desktop Runtime Flow

When the Tauri app starts:

1. it resolves an app data directory
2. it starts the backend executable as a child process
3. it passes:
   - `AQE_PORT=8088`
   - `AQE_DUCKDB_PATH=<app_data>/aqe.duckdb`
4. it opens the frontend window

When the app closes:

- it kills the managed backend child process

## 15. Current Limitations

These are important because they explain where the current engine is intentionally simple.

- SQL support is narrow and parser-based, not full SQL
- there is no cost-based optimizer
- the accuracy slider is not yet tied to adaptive sample generation
- sample tables are rebuilt wholesale after refresh
- Postgres sync is polling-based, not CDC
- there is no query predicate pushdown planner
- error estimation is approximate and generic
- the compare metric is cell-based average relative error, not a statistical confidence report

## 16. Why DuckDB Fits Here

DuckDB is a strong fit for this design because:

- it is embedded and local-first
- it handles CSV ingestion well
- it supports analytical SQL efficiently
- it works well as a single-node desktop analytical engine
- it keeps exact and approximate execution in the same environment

## 17. Why Go Fits Here

Go is a good fit for the current backend because:

- it is easy to expose a clean local HTTP API
- it has straightforward concurrency for polling and sync jobs
- it works well for a desktop-side local service
- it keeps the frontend and backend strongly separated

## 18. Summary

The engine works by importing or mirroring data into DuckDB, maintaining a raw table and a weighted sample table per dataset, limiting query execution to a safe analytical subset, and then running exact and approximate aggregate queries side by side.

Today it is best described as:

- a local analytical engine
- with weighted sampled approximations
- narrow SQL support
- exact-vs-approx comparison
- benchmark reporting
- a simple live Postgres polling prototype

That gives a solid foundation for future work such as:

- predicate support
- dynamic sample selection
- sketch-based summaries
- more rigorous confidence intervals
- richer SQL support
- true streaming ingestion
