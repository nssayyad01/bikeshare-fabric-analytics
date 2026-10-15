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
- [ ] 3: Silver layer (flattening, dedup, schema drift handling)
- [ ] 4: Unstructured processing, Gold star schema
- [ ] 5: Semantic model, DAX, Power BI dashboards
- [ ] 6: Orchestration, data quality, CI/CD, documentation

## Repo contents
- `seed_stations.py`: loads real station list from the GBFS feed into Postgres (idempotent upsert)
- `simulate_ops.py`: generates simulated maintenance tickets and rebalancing tasks
- `test_connection.py`: checks the database connection

## Setup
Create a `.env` file with `DATABASE_URL` (never commit it), then `pip install faker psycopg2-binary python-dotenv`.
