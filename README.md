# Bike-Share Analytics on Microsoft Fabric

End-to-end data engineering and analytics project on NYC Citi Bike data, built on Microsoft Fabric.

## Business problem
Bike stations often run empty or full. This project helps operations managers decide where to move bikes (rebalancing) and shows how weather affects demand, through a Power BI dashboard.

## Data sources
- Structured: historical Citi Bike trip CSVs (2013 and 2024 schemas), PostgreSQL operational tables (stations, maintenance tickets, rebalancing tasks; simulated with Python + Faker)
- Semi-structured: GBFS live station status (JSON, every 15 min), Open-Meteo weather (JSON, hourly)
- Unstructured: technician notes and rider feedback (simulated text, rule-based classifier). City PDF reports are optional and not yet built.

## Architecture (medallion)
Neon Postgres -> Copy job (CopyJob_1) -> Bronze (lh_bronze) -> Silver (lh_silver, PySpark notebooks) -> Gold (wh_gold, star schema) -> Semantic model (sm_bikeshare, Direct Lake) -> Power BI report (rpt_bikeshare)

GBFS and weather are ingested by their own pipelines (pl_ingest_gbfs every 15 min, pl_ingest_weather hourly).

## Pipelines
- **pl_refresh_master** (hourly, scheduled): `nb_refresh_gbfs` -> `nb_refresh_weather` -> Gold daily refresh (dim_date, day_sort, dim_weather, dim_station SCD2 close and open, fact_station_status) -> Gold audit checks.
- **pl_sync_operations** (manual, run only after `simulate_ops.py` or `simulate_feedback.py`): `copy_neon_to_bronze` -> `nb_refresh_station_mapping` -> Gold ops refresh (tickets, tasks, feedback: UPDATE then INSERT) -> ops audit.

Both pipelines share the trial capacity, so do not run them together, and do not run notebooks manually while `pl_refresh_master` is running. Capacity errors (HTTP 430) are handled by retries.

## Data quality and audit
- Table `gold.audit_runs` logs each check per run: unknown station rows, orphan date keys, duplicate fact keys, freshness (Gold vs Silver snapshot), and Gold vs Silver counts for tickets, tasks and feedback.
- Checks currently log PASS/WARN/FAIL only; they do not stop the pipeline. A fail-the-pipeline rule is planned once thresholds are settled.

## Maintenance
- Delta OPTIMIZE and VACUUM (retain 7 days) run on the large Silver tables (`gbfs_station_status`, `weather_hourly`, `trips`) from the notebook `nb_maintenance_delta`. Run it manually about once a week, not inside a pipeline.

## Status
- [x] 1: Postgres setup, idempotent station seed script, Faker simulator
- [x] 2: Bronze ingestion (Postgres incremental merge, GBFS, weather, historical trips)
- [x] 3: Silver layer (schema drift merge for trips, GBFS flatten + incremental load, weather with forecast handling, station mapping, Postgres tables via MERGE)
- [x] 4: Gold star schema (SCD2 dim_station, 5 fact tables), text classification for technician notes and rider feedback (rule-based, 198/200 on simulated feedback). City PDF reports still pending.
- [x] 5: Semantic model and Power BI (complete, with known items)
- Direct Lake semantic model `sm_bikeshare` (8 tables, 15 relationships) with measures: Total Trips, Empty/Full Rate Now (Active), Empty/Full Active Stations Now, Open Tickets, Pending Tasks, Rain Drop %, Negative Share %.
- Report `rpt_bikeshare`, 5 pages: Overview, Operations, Demand, Weather Impact, Rider Voice & Impact.
- [~] 6: Git integration done (GitHub, `fabric/` folder). Data dictionary and architecture diagram done. Optional: deployment pipelines (test/prod).

## Known limitations
- Live station data covers 14 Oct 09:28 to 16 Oct 09:22 UTC and from 23 Oct 18:51 UTC onward, with one manual snapshot on 17 Oct in between. The ~6 day gap will not be recovered, because GBFS only provides current data.
- One GBFS station (`2278012504182672958`) is not in the Neon stations table. Its snapshots map to the Unknown station key (-1), so the Unknown count keeps growing by about 4 per hour.
- Station `valid_from` (SCD2) is the time our Silver load picked up a change (Neon `updated_at`), not the actual time the capacity changed in the real world.
- Weather impact uses Dec 2013 and Jan 2024 only (2024 is ~17 days). Rain/Snow may include snow. Compare within a year; pooled numbers are misleading. Correlation, not causation.
- Tickets and rider feedback are simulated; classifier accuracy is not real-world.
- `start_hour` assumes `started_at` is NYC local time (inferred from the hourly pattern, not confirmed against source docs).
- Feedback UPDATE path tested on one row (feedback_59) that was edited and then restored to its original text.
- The map visual shows a "being retired soon" warning.
- Gold refresh is driven by pipelines now, but the Gold SQL lives in `sql/gold/` and in `gold_build.sql` section 19.
- Independent portfolio project using public Citi Bike data. Not affiliated with Citi Bike or Lyft.

## Repo contents
- `fabric/`: Fabric item definitions (notebooks, pipelines, copy jobs, lakehouses, warehouse, semantic model, report), synced by Fabric Git integration from the fabric-sync branch and merged into main. Git stores definitions only, no data.
- `seed_stations.py`: loads real station list from the GBFS feed into Postgres (idempotent upsert)
- `simulate_ops.py`: generates simulated maintenance tickets and rebalancing tasks (new and updated rows)
- `simulate_feedback.py`: generates simulated rider feedback (runs once, guarded)
- `test_connection.py`: checks the database connection
- `gold_build.sql`: Gold layer record. Sections 01-16 are the one-time build, 17-18 are applied fixes, 19 is the refresh SQL used by the pipelines. Do not run the file top to bottom.

## Setup
Create a `.env` file with `DATABASE_URL` (never commit it), then `pip install faker psycopg2-binary python-dotenv`.