-- =============================================================================
-- Gold layer build script (Fabric Warehouse: wh_gold)
--
-- DO NOT RUN THIS FILE TOP TO BOTTOM.
-- Only run the labelled section you need.
--
-- Sections:
--   BUILD    (01-16): run once, history only. Sections 02-07, 12-14 are
--                     CREATE TABLE AS and will overwrite existing tables.
--   FIXES    (17-18): already applied in wh_gold. Do not re-run.
--   REFRESH  (19):    daily and ops refresh. Run each sub-part as its own
--                     Script activity in the pipeline, in the order shown.
--
-- Notes:
--   * Fabric Warehouse has no nvarchar, so text is cast to varchar.
--   * Sections 04 and 06 are rebuild versions written after SCD2 was added.
--   * Section 08 is a one-time fix from the original build, not needed on rebuild.
-- =============================================================================


-- =============================================================================
-- BUILD (run once, history only)
-- =============================================================================

-- 01 schema
CREATE SCHEMA gold;

-- 02 dim_date (only dates present in the data)  [CREATE TABLE AS]
CREATE TABLE gold.dim_date AS
SELECT
    CAST(YEAR(dt) * 10000 + MONTH(dt) * 100 + DAY(dt) AS int) AS date_key,
    dt AS full_date,
    YEAR(dt) AS year_num,
    MONTH(dt) AS month_num,
    CAST(DATENAME(month, dt) AS varchar(20)) AS month_name,
    DAY(dt) AS day_num,
    CAST(DATENAME(weekday, dt) AS varchar(20)) AS day_name,
    CASE WHEN DATENAME(weekday, dt) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END AS is_weekend
FROM (
    SELECT CAST(started_at AS date) AS dt FROM lh_silver.dbo.trips
    UNION SELECT CAST(ended_at AS date) FROM lh_silver.dbo.trips
    UNION SELECT CAST(snapshot_ts AS date) FROM lh_silver.dbo.gbfs_station_status
    UNION SELECT CAST(hour_ts_utc AS date) FROM lh_silver.dbo.weather_hourly
    UNION SELECT CAST(hour_ts_utc AS date) FROM lh_silver.dbo.weather_hourly_history
) d;

-- 03 dim_weather (live non-forecast + historical; UTC key, local time kept)  [CREATE TABLE AS]
CREATE TABLE gold.dim_weather AS
SELECT
    CAST(YEAR(hour_ts_utc) * 1000000 + MONTH(hour_ts_utc) * 10000
         + DAY(hour_ts_utc) * 100 + DATEPART(hour, hour_ts_utc) AS bigint) AS weather_key,
    hour_ts_utc,
    CAST(hour_ts_utc AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time' AS datetime2(0)) AS hour_ts_local,
    temperature_c, precipitation_mm, wind_speed_kmh,
    CASE WHEN precipitation_mm > 0 THEN 1 ELSE 0 END AS is_raining,
    CAST(src AS varchar(10)) AS source
FROM (
    SELECT hour_ts_utc, temperature_c, precipitation_mm, wind_speed_kmh, 'live' AS src
    FROM lh_silver.dbo.weather_hourly WHERE is_forecast = 0
    UNION ALL
    SELECT hour_ts_utc, temperature_c, precipitation_mm, wind_speed_kmh, 'history'
    FROM lh_silver.dbo.weather_hourly_history
) w;

-- 04 dim_station initial load (unknown row + current + historical)  [CREATE TABLE AS]
CREATE TABLE gold.dim_station AS
SELECT CAST(-1 AS int) AS station_key, CAST('UNKNOWN' AS varchar(100)) AS station_id,
       CAST('Unknown station' AS varchar(200)) AS station_name, CAST(NULL AS varchar(50)) AS region,
       CAST(NULL AS int) AS capacity, CAST(NULL AS float) AS lat, CAST(NULL AS float) AS lon,
       CAST('unknown' AS varchar(20)) AS station_type,
       CAST('1900-01-01' AS datetime2(0)) AS valid_from, CAST('9999-12-31' AS datetime2(0)) AS valid_to,
       CAST(1 AS int) AS is_current
UNION ALL
SELECT CAST(ROW_NUMBER() OVER (ORDER BY station_id) AS int),
       station_id, station_name, region, capacity, lat, lon, station_type,
       CAST('1900-01-01' AS datetime2(0)), CAST('9999-12-31' AS datetime2(0)), CAST(1 AS int)
FROM (
    SELECT CAST(station_id AS varchar(100)) AS station_id, CAST(station_name AS varchar(200)) AS station_name,
           CAST(region AS varchar(50)) AS region, CAST(capacity AS int) AS capacity,
           CAST(lat AS float) AS lat, CAST(lon AS float) AS lon, CAST('current' AS varchar(20)) AS station_type
    FROM lh_silver.dbo.stations
    UNION ALL
    SELECT CAST(CONCAT('hist_', source_schema, '_', trip_station_id) AS varchar(100)),
           CAST(trip_station_name AS varchar(200)), CAST(NULL AS varchar(50)), CAST(NULL AS int),
           CAST(lat AS float), CAST(lng AS float), CAST('historical' AS varchar(20))
    FROM lh_silver.dbo.station_map WHERE match_status = 'historical_only'
) s;

-- 05 helper: trip station -> station_key (is_current = 1 avoids double rows after SCD2)  [CREATE TABLE AS]
CREATE TABLE gold.stg_station_lookup AS
SELECT CAST(m.source_schema AS varchar(10)) AS source_schema,
       CAST(m.trip_station_id AS varchar(50)) AS trip_station_id,
       CAST(m.trip_station_name AS varchar(200)) AS trip_station_name,
       d.station_key
FROM lh_silver.dbo.station_map m
JOIN gold.dim_station d
  ON d.is_current = 1
 AND d.station_id = CAST(CASE m.match_status
        WHEN 'matched' THEN m.station_id
        WHEN 'historical_only' THEN CONCAT('hist_', m.source_schema, '_', m.trip_station_id)
        ELSE 'UNKNOWN' END AS varchar(100));

-- 06 fact_trips  [CREATE TABLE AS]
CREATE TABLE gold.fact_trips AS
SELECT CAST(t.trip_id AS varchar(64)) AS trip_id,
       CAST(YEAR(t.started_at) * 10000 + MONTH(t.started_at) * 100 + DAY(t.started_at) AS int) AS date_key,
       COALESCE(ss.station_key, -1) AS start_station_key,
       COALESCE(es.station_key, -1) AS end_station_key,
       w.weather_key,
       t.started_at, t.ended_at,
       CAST(t.duration_sec AS int) AS duration_sec,
       CAST(t.user_type AS varchar(10)) AS user_type,
       CAST(t.rideable_type AS varchar(30)) AS rideable_type,
       CAST(t.source_schema AS varchar(10)) AS source_schema
FROM lh_silver.dbo.trips t
LEFT JOIN gold.stg_station_lookup ss
  ON ss.source_schema = t.source_schema AND ss.trip_station_id = t.start_station_id
 AND ss.trip_station_name = t.start_station_name
LEFT JOIN gold.stg_station_lookup es
  ON es.source_schema = t.source_schema AND es.trip_station_id = t.end_station_id
 AND es.trip_station_name = t.end_station_name
LEFT JOIN (
    SELECT weather_key,
           CAST(YEAR(hour_ts_local) * 1000000 + MONTH(hour_ts_local) * 10000
                + DAY(hour_ts_local) * 100 + DATEPART(hour, hour_ts_local) AS bigint) AS local_key
    FROM gold.dim_weather WHERE source = 'history'
) w
  ON w.local_key = CAST(YEAR(t.started_at) * 1000000 + MONTH(t.started_at) * 10000
                        + DAY(t.started_at) * 100 + DATEPART(hour, t.started_at) AS bigint);

-- 07 fact_station_status (station version chosen by snapshot time)  [CREATE TABLE AS]
CREATE TABLE gold.fact_station_status AS
SELECT COALESCE(ds.station_key, -1) AS station_key,
       CAST(YEAR(s.snapshot_ts_local) * 10000 + MONTH(s.snapshot_ts_local) * 100
            + DAY(s.snapshot_ts_local) AS int) AS date_key,
       w.weather_key,
       s.snapshot_ts, s.snapshot_ts_local,
       s.num_bikes_available, s.num_ebikes_available, s.num_bikes_disabled,
       s.num_docks_available, s.num_docks_disabled,
       s.is_installed, s.is_renting, s.is_returning,
       CASE WHEN s.num_bikes_available = 0 THEN 1 ELSE 0 END AS is_empty,
       CASE WHEN s.num_docks_available = 0 THEN 1 ELSE 0 END AS is_full
FROM (
    SELECT CAST(station_id AS varchar(100)) AS station_id, snapshot_ts,
           CAST(snapshot_ts AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time' AS datetime2(0)) AS snapshot_ts_local,
           CAST(YEAR(snapshot_ts) * 1000000 + MONTH(snapshot_ts) * 10000
                + DAY(snapshot_ts) * 100 + DATEPART(hour, snapshot_ts) AS bigint) AS weather_key_utc,
           CAST(num_bikes_available AS int) AS num_bikes_available,
           CAST(num_ebikes_available AS int) AS num_ebikes_available,
           CAST(num_bikes_disabled AS int) AS num_bikes_disabled,
           CAST(num_docks_available AS int) AS num_docks_available,
           CAST(num_docks_disabled AS int) AS num_docks_disabled,
           CAST(is_installed AS int) AS is_installed,
           CAST(is_renting AS int) AS is_renting,
           CAST(is_returning AS int) AS is_returning
    FROM lh_silver.dbo.gbfs_station_status
) s
LEFT JOIN gold.dim_station ds
  ON ds.station_id = s.station_id AND ds.station_type = 'current'
 AND s.snapshot_ts >= ds.valid_from AND s.snapshot_ts < ds.valid_to
LEFT JOIN gold.dim_weather w
  ON w.weather_key = s.weather_key_utc AND w.source = 'live';

-- 08 one-time fix (original build only): snapshot of a station added after the first dim_station load
UPDATE f SET f.station_key = d.station_key
FROM gold.fact_station_status f
CROSS JOIN gold.dim_station d
WHERE f.station_key = -1 AND f.snapshot_ts = '2026-10-17 10:54:05'
  AND d.station_id = '2275685788423988534' AND d.is_current = 1;

-- 09 SCD2 step 1 (original build reference; daily version is 19d): close changed versions
UPDATE d
SET d.valid_to = CAST(s.updated_at AS datetime2(0)), d.is_current = 0
FROM gold.dim_station d
JOIN lh_silver.dbo.stations s ON s.station_id = d.station_id
WHERE d.station_type = 'current' AND d.is_current = 1
  AND (d.capacity <> s.capacity OR d.station_name <> s.station_name
       OR ISNULL(d.region, 'x') <> ISNULL(s.region, 'x'));

-- 10 SCD2 step 2 (original build reference; daily version is 19e): open new versions
INSERT INTO gold.dim_station
    (station_key, station_id, station_name, region, capacity, lat, lon,
     station_type, valid_from, valid_to, is_current)
SELECT mk.max_key + ROW_NUMBER() OVER (ORDER BY s.station_id),
       CAST(s.station_id AS varchar(100)), CAST(s.station_name AS varchar(200)),
       CAST(s.region AS varchar(50)), CAST(s.capacity AS int),
       CAST(s.lat AS float), CAST(s.lon AS float), CAST('current' AS varchar(20)),
       CASE WHEN prev.station_id IS NULL THEN CAST('1900-01-01' AS datetime2(0))
            ELSE CAST(s.updated_at AS datetime2(0)) END,
       CAST('9999-12-31' AS datetime2(0)), CAST(1 AS int)
FROM lh_silver.dbo.stations s
CROSS JOIN (SELECT MAX(station_key) AS max_key FROM gold.dim_station) mk
LEFT JOIN (SELECT DISTINCT station_id FROM gold.dim_station WHERE station_type = 'current') prev
       ON prev.station_id = s.station_id
LEFT JOIN gold.dim_station cur
       ON cur.station_id = s.station_id AND cur.station_type = 'current' AND cur.is_current = 1
WHERE cur.station_id IS NULL;

-- 11 extra dates (tickets, tasks, text) not in the first dim_date build (original build reference; daily version is 19g-dates)  [INSERT]
INSERT INTO gold.dim_date (date_key, full_date, year_num, month_num, month_name, day_num, day_name, is_weekend)
SELECT CAST(YEAR(x.dt) * 10000 + MONTH(x.dt) * 100 + DAY(x.dt) AS int), x.dt,
       YEAR(x.dt), MONTH(x.dt), CAST(DATENAME(month, x.dt) AS varchar(20)), DAY(x.dt),
       CAST(DATENAME(weekday, x.dt) AS varchar(20)),
       CASE WHEN DATENAME(weekday, x.dt) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END
FROM (
    SELECT CAST(created_at AS date) AS dt FROM lh_silver.dbo.maintenance_tickets
    UNION SELECT CAST(updated_at AS date) FROM lh_silver.dbo.maintenance_tickets
    UNION SELECT CAST(assigned_at AS date) FROM lh_silver.dbo.rebalancing_tasks
    UNION SELECT CAST(completed_at AS date) FROM lh_silver.dbo.rebalancing_tasks
    UNION SELECT CAST(updated_at AS date) FROM lh_silver.dbo.rebalancing_tasks
    UNION SELECT CAST(created_at AS date) FROM lh_silver.dbo.text_docs
) x
LEFT JOIN gold.dim_date d ON d.full_date = x.dt
WHERE x.dt IS NOT NULL AND d.full_date IS NULL;

-- 12 fact_maintenance_tickets  [CREATE TABLE AS]
CREATE TABLE gold.fact_maintenance_tickets AS
SELECT CAST(t.ticket_id AS int) AS ticket_id,
       COALESCE(ds.station_key, -1) AS station_key,
       CAST(YEAR(t.created_at) * 10000 + MONTH(t.created_at) * 100 + DAY(t.created_at) AS int) AS created_date_key,
       t.created_at, t.updated_at,
       CAST(t.issue_category AS varchar(20)) AS issue_category,
       CAST(tx.predicted_category AS varchar(20)) AS predicted_category,
       CAST(t.status AS varchar(10)) AS status,
       CAST(t.is_open AS int) AS is_open,
       CAST(t.technician_note AS varchar(500)) AS technician_note
FROM lh_silver.dbo.maintenance_tickets t
LEFT JOIN lh_silver.dbo.text_docs tx ON tx.doc_id = t.doc_id
LEFT JOIN gold.dim_station ds
       ON ds.station_id = t.station_id AND ds.station_type = 'current'
      AND t.created_at >= ds.valid_from AND t.created_at < ds.valid_to;

-- 13 fact_rebalancing_tasks  [CREATE TABLE AS]
CREATE TABLE gold.fact_rebalancing_tasks AS
SELECT CAST(t.task_id AS int) AS task_id,
       COALESCE(fs.station_key, -1) AS from_station_key,
       COALESCE(ts.station_key, -1) AS to_station_key,
       CAST(YEAR(t.assigned_at) * 10000 + MONTH(t.assigned_at) * 100 + DAY(t.assigned_at) AS int) AS assigned_date_key,
       CASE WHEN t.completed_at IS NULL THEN NULL
            ELSE CAST(YEAR(t.completed_at) * 10000 + MONTH(t.completed_at) * 100 + DAY(t.completed_at) AS int) END AS completed_date_key,
       t.assigned_at, t.completed_at, t.updated_at,
       CAST(t.bikes_count AS int) AS bikes_count,
       CAST(t.status AS varchar(10)) AS status,
       CAST(t.is_completed AS int) AS is_completed,
       CAST(t.duration_minutes AS float) AS duration_minutes
FROM lh_silver.dbo.rebalancing_tasks t
LEFT JOIN gold.dim_station fs
       ON fs.station_id = t.from_station_id AND fs.station_type = 'current'
      AND t.assigned_at >= fs.valid_from AND t.assigned_at < fs.valid_to
LEFT JOIN gold.dim_station ts
       ON ts.station_id = t.to_station_id AND ts.station_type = 'current'
      AND t.assigned_at >= ts.valid_from AND t.assigned_at < ts.valid_to;

-- 14 fact_rider_feedback (true_category is the simulator's answer key, kept only to measure accuracy)  [CREATE TABLE AS]
CREATE TABLE gold.fact_rider_feedback AS
SELECT CAST(x.doc_id AS varchar(30)) AS feedback_doc_id,
       COALESCE(ds.station_key, -1) AS station_key,
       CAST(YEAR(x.created_at) * 10000 + MONTH(x.created_at) * 100 + DAY(x.created_at) AS int) AS created_date_key,
       x.created_at, x.updated_at,
       CAST(x.label_original AS varchar(30)) AS true_category,
       CAST(x.predicted_category AS varchar(30)) AS predicted_category,
       CAST(x.predicted_sentiment AS varchar(10)) AS predicted_sentiment,
       CAST(x.is_match AS int) AS category_correct,
       CAST(x.text AS varchar(500)) AS feedback_text
FROM lh_silver.dbo.text_docs x
LEFT JOIN gold.dim_station ds
       ON ds.station_id = x.station_id AND ds.station_type = 'current'
      AND x.created_at >= ds.valid_from AND x.created_at < ds.valid_to
WHERE x.source = 'rider_feedback';

-- 15 readable weather label (run after section 03)
ALTER TABLE gold.dim_weather ADD weather_label varchar(12);
UPDATE gold.dim_weather
SET weather_label = CASE WHEN is_raining = 1 THEN 'Rain/Snow' ELSE 'Dry' END;

-- 16 start_hour on fact_trips (needed for "Trips by Hour of Day" chart)
-- Assumption: started_at is NYC local time (hourly pattern looked local, not verified against source docs).
ALTER TABLE gold.fact_trips ADD start_hour smallint;
UPDATE gold.fact_trips
SET start_hour = DATEPART(hour, started_at);


-- =============================================================================
-- FIXES (already applied in wh_gold. Do not re-run.)
-- =============================================================================

-- 17 day_sort on dim_date (Demand chart Mon-Sun order)
ALTER TABLE gold.dim_date ADD day_sort INT;
UPDATE gold.dim_date
SET day_sort = CASE day_name
    WHEN 'Monday' THEN 1 WHEN 'Tuesday' THEN 2 WHEN 'Wednesday' THEN 3
    WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5 WHEN 'Saturday' THEN 6
    WHEN 'Sunday' THEN 7
END
WHERE day_sort IS NULL;

-- 18 audit table (applied once)
CREATE TABLE gold.audit_runs (
    run_ts DATETIME2(0) NOT NULL,
    check_name VARCHAR(100) NOT NULL,
    result_value BIGINT NULL,
    expected_value BIGINT NULL,
    status VARCHAR(10) NOT NULL
);


-- =============================================================================
-- REFRESH (pipeline activities, run in this order)
-- Each sub-part below = one Script activity (NonQuery, connection wh_gold).
-- Pipeline: pl_refresh_master (hourly) for 19a-19f; pl_sync_operations (manual) for 19g-19h.
-- =============================================================================

-- ---- 19a  pipeline: pl_refresh_master / gold_dim_date_insert
INSERT INTO gold.dim_date (date_key, full_date, year_num, month_num, month_name, day_num, day_name, is_weekend)
SELECT
    CAST(YEAR(x.dt) * 10000 + MONTH(x.dt) * 100 + DAY(x.dt) AS int),
    x.dt,
    YEAR(x.dt),
    MONTH(x.dt),
    CAST(DATENAME(month, x.dt) AS varchar(20)),
    DAY(x.dt),
    CAST(DATENAME(weekday, x.dt) AS varchar(20)),
    CASE WHEN DATENAME(weekday, x.dt) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END
FROM (
    SELECT DISTINCT CAST(CAST(snapshot_ts AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time' AS datetime2(0)) AS date) AS dt
    FROM lh_silver.dbo.gbfs_station_status
) x
LEFT JOIN gold.dim_date d ON d.full_date = x.dt
WHERE d.full_date IS NULL;

-- ---- 19b  pipeline: pl_refresh_master / gold_dim_date_daysort
UPDATE gold.dim_date
SET day_sort = CASE day_name
    WHEN 'Monday' THEN 1 WHEN 'Tuesday' THEN 2 WHEN 'Wednesday' THEN 3
    WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5 WHEN 'Saturday' THEN 6
    WHEN 'Sunday' THEN 7
END
WHERE day_sort IS NULL;

-- ---- 19c  pipeline: pl_refresh_master / gold_dim_weather_insert
INSERT INTO gold.dim_weather
    (weather_key, hour_ts_utc, hour_ts_local, temperature_c, precipitation_mm,
     wind_speed_kmh, is_raining, source, weather_label)
SELECT
    CAST(YEAR(w.hour_ts_utc) * 1000000 + MONTH(w.hour_ts_utc) * 10000
         + DAY(w.hour_ts_utc) * 100 + DATEPART(hour, w.hour_ts_utc) AS bigint),
    w.hour_ts_utc,
    CAST(w.hour_ts_utc AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time' AS datetime2(0)),
    w.temperature_c,
    w.precipitation_mm,
    w.wind_speed_kmh,
    CASE WHEN w.precipitation_mm > 0 THEN 1 ELSE 0 END,
    CAST('live' AS varchar(10)),
    CAST(CASE WHEN w.precipitation_mm > 0 THEN 'Rain/Snow' ELSE 'Dry' END AS varchar(12))
FROM lh_silver.dbo.weather_hourly w
LEFT JOIN gold.dim_weather d
       ON d.hour_ts_utc = w.hour_ts_utc AND d.source = 'live'
WHERE w.is_forecast = 0
  AND d.hour_ts_utc IS NULL;

-- ---- 19d  pipeline: pl_refresh_master / gold_dim_station_update (SCD2 close, must run right before 19e)
UPDATE d
SET d.valid_to = CAST(s.updated_at AS datetime2(0)), d.is_current = 0
FROM gold.dim_station d
JOIN lh_silver.dbo.stations s ON s.station_id = d.station_id
WHERE d.station_type = 'current' AND d.is_current = 1
  AND (d.capacity <> s.capacity OR d.station_name <> s.station_name
       OR ISNULL(d.region, 'x') <> ISNULL(s.region, 'x'));

-- ---- 19e  pipeline: pl_refresh_master / gold_dim_station_insert (SCD2 open, right after 19d)
INSERT INTO gold.dim_station
    (station_key, station_id, station_name, region, capacity, lat, lon,
     station_type, valid_from, valid_to, is_current)
SELECT mk.max_key + ROW_NUMBER() OVER (ORDER BY s.station_id),
       CAST(s.station_id AS varchar(100)), CAST(s.station_name AS varchar(200)),
       CAST(s.region AS varchar(50)), CAST(s.capacity AS int),
       CAST(s.lat AS float), CAST(s.lon AS float), CAST('current' AS varchar(20)),
       CASE WHEN prev.station_id IS NULL THEN CAST('1900-01-01' AS datetime2(0))
            ELSE CAST(s.updated_at AS datetime2(0)) END,
       CAST('9999-12-31' AS datetime2(0)), CAST(1 AS int)
FROM lh_silver.dbo.stations s
CROSS JOIN (SELECT MAX(station_key) AS max_key FROM gold.dim_station) mk
LEFT JOIN (SELECT DISTINCT station_id FROM gold.dim_station WHERE station_type = 'current') prev
       ON prev.station_id = s.station_id
LEFT JOIN gold.dim_station cur
       ON cur.station_id = s.station_id AND cur.station_type = 'current' AND cur.is_current = 1
WHERE cur.station_id IS NULL;

-- ---- 19f  pipeline: pl_refresh_master / gold_fact_station_status_insert
INSERT INTO gold.fact_station_status
    (station_key, date_key, weather_key, snapshot_ts, snapshot_ts_local,
     num_bikes_available, num_ebikes_available, num_bikes_disabled,
     num_docks_available, num_docks_disabled,
     is_installed, is_renting, is_returning, is_empty, is_full)
SELECT
    COALESCE(ds.station_key, -1),
    CAST(YEAR(s.snapshot_ts_local) * 10000 + MONTH(s.snapshot_ts_local) * 100
         + DAY(s.snapshot_ts_local) AS int),
    w.weather_key,
    s.snapshot_ts,
    s.snapshot_ts_local,
    s.num_bikes_available, s.num_ebikes_available, s.num_bikes_disabled,
    s.num_docks_available, s.num_docks_disabled,
    s.is_installed, s.is_renting, s.is_returning,
    CASE WHEN s.num_bikes_available = 0 THEN 1 ELSE 0 END,
    CASE WHEN s.num_docks_available = 0 THEN 1 ELSE 0 END
FROM (
    SELECT CAST(station_id AS varchar(100)) AS station_id,
           snapshot_ts,
           CAST(snapshot_ts AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time' AS datetime2(0)) AS snapshot_ts_local,
           CAST(YEAR(snapshot_ts) * 1000000 + MONTH(snapshot_ts) * 10000
                + DAY(snapshot_ts) * 100 + DATEPART(hour, snapshot_ts) AS bigint) AS weather_key_utc,
           CAST(num_bikes_available AS int)  AS num_bikes_available,
           CAST(num_ebikes_available AS int) AS num_ebikes_available,
           CAST(num_bikes_disabled AS int)   AS num_bikes_disabled,
           CAST(num_docks_available AS int)  AS num_docks_available,
           CAST(num_docks_disabled AS int)   AS num_docks_disabled,
           CAST(is_installed AS int)         AS is_installed,
           CAST(is_renting AS int)           AS is_renting,
           CAST(is_returning AS int)         AS is_returning
    FROM lh_silver.dbo.gbfs_station_status
) s
LEFT JOIN (SELECT DISTINCT snapshot_ts FROM gold.fact_station_status) f
       ON f.snapshot_ts = s.snapshot_ts
LEFT JOIN gold.dim_station ds
       ON ds.station_id = s.station_id AND ds.station_type = 'current'
      AND s.snapshot_ts >= ds.valid_from AND s.snapshot_ts < ds.valid_to
LEFT JOIN gold.dim_weather w
       ON w.weather_key = s.weather_key_utc AND w.source = 'live'
WHERE f.snapshot_ts IS NULL;

-- ---- 19g  pipeline: pl_sync_operations / gold_fact_tickets_update, gold_fact_tickets_insert,
--           gold_fact_tasks_update, gold_fact_tasks_insert, gold_fact_feedback_update, gold_fact_feedback_insert
--           (ops tables. Run only after simulate_ops.py or simulate_feedback.py.)

-- 19g-1 tickets UPDATE
UPDATE f
SET f.status = CAST(t.status AS varchar(10)),
    f.is_open = CAST(t.is_open AS int),
    f.updated_at = t.updated_at,
    f.predicted_category = CAST(tx.predicted_category AS varchar(20))
FROM gold.fact_maintenance_tickets f
JOIN lh_silver.dbo.maintenance_tickets t ON t.ticket_id = f.ticket_id
LEFT JOIN lh_silver.dbo.text_docs tx ON tx.doc_id = t.doc_id
WHERE t.updated_at > f.updated_at;

-- 19g-2 tickets INSERT
INSERT INTO gold.fact_maintenance_tickets
    (ticket_id, station_key, created_date_key, created_at, updated_at,
     issue_category, predicted_category, status, is_open, technician_note)
SELECT
    CAST(t.ticket_id AS int),
    COALESCE(ds.station_key, -1),
    CAST(YEAR(t.created_at) * 10000 + MONTH(t.created_at) * 100 + DAY(t.created_at) AS int),
    t.created_at,
    t.updated_at,
    CAST(t.issue_category AS varchar(20)),
    CAST(tx.predicted_category AS varchar(20)),
    CAST(t.status AS varchar(10)),
    CAST(t.is_open AS int),
    CAST(t.technician_note AS varchar(500))
FROM lh_silver.dbo.maintenance_tickets t
LEFT JOIN gold.fact_maintenance_tickets f ON f.ticket_id = t.ticket_id
LEFT JOIN lh_silver.dbo.text_docs tx ON tx.doc_id = t.doc_id
LEFT JOIN gold.dim_station ds
       ON ds.station_id = t.station_id AND ds.station_type = 'current'
      AND t.created_at >= ds.valid_from AND t.created_at < ds.valid_to
WHERE f.ticket_id IS NULL;

-- 19g-3 tasks UPDATE
UPDATE f
SET f.status = CAST(t.status AS varchar(10)),
    f.is_completed = CAST(t.is_completed AS int),
    f.completed_at = t.completed_at,
    f.completed_date_key = CASE WHEN t.completed_at IS NULL THEN NULL
        ELSE CAST(YEAR(t.completed_at) * 10000 + MONTH(t.completed_at) * 100
                  + DAY(t.completed_at) AS int) END,
    f.duration_minutes = CAST(t.duration_minutes AS float),
    f.updated_at = t.updated_at
FROM gold.fact_rebalancing_tasks f
JOIN lh_silver.dbo.rebalancing_tasks t ON t.task_id = f.task_id
WHERE t.updated_at > f.updated_at;

-- 19g-4 tasks INSERT
INSERT INTO gold.fact_rebalancing_tasks
    (task_id, from_station_key, to_station_key, assigned_date_key, completed_date_key,
     assigned_at, completed_at, updated_at, bikes_count, status, is_completed, duration_minutes)
SELECT
    CAST(t.task_id AS int),
    COALESCE(fs.station_key, -1),
    COALESCE(ts.station_key, -1),
    CAST(YEAR(t.assigned_at) * 10000 + MONTH(t.assigned_at) * 100 + DAY(t.assigned_at) AS int),
    CASE WHEN t.completed_at IS NULL THEN NULL
         ELSE CAST(YEAR(t.completed_at) * 10000 + MONTH(t.completed_at) * 100
                   + DAY(t.completed_at) AS int) END,
    t.assigned_at, t.completed_at, t.updated_at,
    CAST(t.bikes_count AS int),
    CAST(t.status AS varchar(10)),
    CAST(t.is_completed AS int),
    CAST(t.duration_minutes AS float)
FROM lh_silver.dbo.rebalancing_tasks t
LEFT JOIN gold.fact_rebalancing_tasks f ON f.task_id = t.task_id
LEFT JOIN gold.dim_station fs
       ON fs.station_id = t.from_station_id AND fs.station_type = 'current'
      AND t.assigned_at >= fs.valid_from AND t.assigned_at < fs.valid_to
LEFT JOIN gold.dim_station ts
       ON ts.station_id = t.to_station_id AND ts.station_type = 'current'
      AND t.assigned_at >= ts.valid_from AND t.assigned_at < ts.valid_to
WHERE f.task_id IS NULL;

-- 19g-5 feedback UPDATE (feedback_text included, tested on feedback_59)
UPDATE f
SET f.feedback_text = CAST(x.text AS varchar(500)),
    f.predicted_category = CAST(x.predicted_category AS varchar(30)),
    f.predicted_sentiment = CAST(x.predicted_sentiment AS varchar(10)),
    f.category_correct = CAST(x.is_match AS int),
    f.updated_at = x.updated_at
FROM gold.fact_rider_feedback f
JOIN lh_silver.dbo.text_docs x ON x.doc_id = f.feedback_doc_id AND x.source = 'rider_feedback'
WHERE x.updated_at > f.updated_at;

-- 19g-6 feedback INSERT
INSERT INTO gold.fact_rider_feedback
    (feedback_doc_id, station_key, created_date_key, created_at, updated_at,
     true_category, predicted_category, predicted_sentiment, category_correct, feedback_text)
SELECT
    CAST(x.doc_id AS varchar(30)),
    COALESCE(ds.station_key, -1),
    CAST(YEAR(x.created_at) * 10000 + MONTH(x.created_at) * 100 + DAY(x.created_at) AS int),
    x.created_at, x.updated_at,
    CAST(x.label_original AS varchar(30)),
    CAST(x.predicted_category AS varchar(30)),
    CAST(x.predicted_sentiment AS varchar(10)),
    CAST(x.is_match AS int),
    CAST(x.text AS varchar(500))
FROM lh_silver.dbo.text_docs x
LEFT JOIN gold.fact_rider_feedback f ON f.feedback_doc_id = x.doc_id
LEFT JOIN gold.dim_station ds
       ON ds.station_id = x.station_id AND ds.station_type = 'current'
      AND x.created_at >= ds.valid_from AND x.created_at < ds.valid_to
WHERE x.source = 'rider_feedback'
  AND f.feedback_doc_id IS NULL;

-- ---- 19h  pipeline: pl_sync_operations / gold_audit_ops (also gold_audit_checks in pl_refresh_master)
INSERT INTO gold.audit_runs (run_ts, check_name, result_value, expected_value, status)
SELECT SYSDATETIME(), 'ops_tickets_gold_vs_silver',
       (SELECT COUNT(*) FROM gold.fact_maintenance_tickets),
       (SELECT COUNT(*) FROM lh_silver.dbo.maintenance_tickets),
       CASE WHEN (SELECT COUNT(*) FROM gold.fact_maintenance_tickets)
               = (SELECT COUNT(*) FROM lh_silver.dbo.maintenance_tickets) THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT SYSDATETIME(), 'ops_tasks_gold_vs_silver',
       (SELECT COUNT(*) FROM gold.fact_rebalancing_tasks),
       (SELECT COUNT(*) FROM lh_silver.dbo.rebalancing_tasks),
       CASE WHEN (SELECT COUNT(*) FROM gold.fact_rebalancing_tasks)
               = (SELECT COUNT(*) FROM lh_silver.dbo.rebalancing_tasks) THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT SYSDATETIME(), 'ops_feedback_gold_vs_silver',
       (SELECT COUNT(*) FROM gold.fact_rider_feedback),
       (SELECT COUNT(*) FROM lh_silver.dbo.text_docs WHERE source = 'rider_feedback'),
       CASE WHEN (SELECT COUNT(*) FROM gold.fact_rider_feedback)
               = (SELECT COUNT(*) FROM lh_silver.dbo.text_docs WHERE source = 'rider_feedback') THEN 'PASS' ELSE 'FAIL' END;
-- ---- 19h-1  pipeline: pl_refresh_master / gold_audit_checks (runs after 19f, hourly)
INSERT INTO gold.audit_runs (run_ts, check_name, result_value, expected_value, status)
SELECT SYSDATETIME(), 'unknown_station_rows',
       (SELECT COUNT(*) FROM gold.fact_station_status WHERE station_key = -1), NULL,
       CASE WHEN (SELECT COUNT(*) FROM gold.fact_station_status WHERE station_key = -1) > 1000
            THEN 'WARN' ELSE 'PASS' END
UNION ALL
SELECT SYSDATETIME(), 'orphan_dates',
       (SELECT COUNT(*) FROM gold.fact_station_status f
          LEFT JOIN gold.dim_date d ON d.date_key = f.date_key
         WHERE d.date_key IS NULL), 0,
       CASE WHEN (SELECT COUNT(*) FROM gold.fact_station_status f
                    LEFT JOIN gold.dim_date d ON d.date_key = f.date_key
                   WHERE d.date_key IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT SYSDATETIME(), 'duplicate_keys',
       (SELECT COUNT(*) FROM (SELECT station_key, snapshot_ts FROM gold.fact_station_status
          GROUP BY station_key, snapshot_ts HAVING COUNT(*) > 1) x), 0,
       CASE WHEN (SELECT COUNT(*) FROM (SELECT station_key, snapshot_ts FROM gold.fact_station_status
          GROUP BY station_key, snapshot_ts HAVING COUNT(*) > 1) x) = 0 THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT SYSDATETIME(), 'freshness_minutes_behind_silver',
       DATEDIFF(MINUTE, (SELECT MAX(snapshot_ts) FROM gold.fact_station_status),
                        (SELECT MAX(snapshot_ts) FROM lh_silver.dbo.gbfs_station_status)), 30,
       CASE WHEN DATEDIFF(MINUTE, (SELECT MAX(snapshot_ts) FROM gold.fact_station_status),
                        (SELECT MAX(snapshot_ts) FROM lh_silver.dbo.gbfs_station_status)) <= 30
            THEN 'PASS' ELSE 'WARN' END;