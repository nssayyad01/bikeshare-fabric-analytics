# Bike-Share Analytics on Microsoft Fabric

End-to-end data engineering and analytics project on NYC Citi Bike data, built on Microsoft Fabric.

## Business problem
Bike stations often run empty or full. This project helps operations managers decide where to move bikes (rebalancing) and shows how weather affects demand, through a Power BI dashboard.

## Data sources
- Structured: historical Citi Bike trip CSVs (2013 and 2024 schemas), PostgreSQL operational tables (stations, maintenance tickets, rebalancing tasks; simulated with Python + Faker)
- Semi-structured: GBFS live station status (JSON, every 15 min), Open-Meteo weather (JSON, hourly)
- Unstructured (planned): technician notes, rider feedback, city reports

## Architecture (medallion)
Sources -> Data Factory pipelines -> Bronze (Lakehouse, raw) -> Silver (PySpark, cleaned) -> Gold (Warehouse, star schema) -> Semantic model (Direct Lake) -> Power BI

## Tech stack
Microsoft Fabric (Data Factory, OneLake, Lakehouse, Warehouse, Notebooks, Power BI), PostgreSQL (Neon), Python, SQL, Git

## Status
- [x] 1: Postgres setup, idempotent station seed script, Faker simulator
- [x] 2: Bronze ingestion (Postgres incremental merge, GBFS, weather, historical trips)
- [x] 3: Silver layer (schema drift merge for trips, GBFS flatten + incremental load, weather with forecast handling, station mapping, Postgres tables via MERGE)
- [x] 4: Gold star schema (SCD2 dim_station, 5 fact tables), text classification for technician notes and rider feedback (rule-based, 198/200 on simulated feedback). City PDF reports still pending.
- [x] 5: Semantic model and Power BI (complete, with known items)
- Direct Lake semantic model `sm_bikeshare` (8 tables, 15 relationships) with measures: Total Trips, Empty/Full Rate Now (Active), Empty/Full Active Stations Now, Open Tickets, Pending Tasks, Rain Drop %, Negative Share %.
- Report `rpt_bikeshare`, 5 pages: Overview, Operations, Demand, Weather Impact, Rider Voice & Impact.
- Added `start_hour` to `gold.fact_trips` (see gold_build.sql section 16) for the hour-of-day chart.

**Known limitations**
- Live GBFS/weather ingestion stopped on 16-17 Oct 2026 (schedule end date passed), so "Now" metrics refer to the 17 Oct 10:54 UTC snapshot, and the Operations line chart shows a flat gap. It fills once schedules are re-enabled (Week 6).
- Weather impact uses Dec 2013 and Jan 2024 only (2024 is ~17 days). Rain/Snow may include snow. Compare within a year; pooled numbers are misleading. Correlation, not causation.
- Tickets and rider feedback are simulated; classifier accuracy is not real-world.
- `start_hour` assumes `started_at` is NYC local time (inferred from the hourly pattern, not confirmed against source docs).
- The map visual shows a "being retired soon" warning.
- Independent portfolio project using public Citi Bike data. Not affiliated with Citi Bike or Lyft.
- [ ] 6: Orchestration, data quality, CI/CD, documentation

## Repo contents
- `seed_stations.py`: loads real station list from the GBFS feed into Postgres (idempotent upsert)
- `simulate_ops.py`: generates simulated maintenance tickets and rebalancing tasks
- `test_connection.py`: checks the database connection

## Setup
Create a `.env` file with `DATABASE_URL` (never commit it), then `pip install faker psycopg2-binary python-dotenv`.
