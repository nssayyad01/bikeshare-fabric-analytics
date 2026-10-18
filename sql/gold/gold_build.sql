-- Gold layer build script (Fabric Warehouse: wh_gold)
-- Run order: top to bottom. Notes:
--   * Fabric Warehouse has no nvarchar, so text is cast to varchar.
--   * Sections 04, 06: rebuild versions, written after SCD2 was added. Not yet tested.
--   * Section 08 is a one-time fix from the original build, not needed on rebuild.

-- 01 schema
CREATE SCHEMA gold;

-- 02 dim_date (only dates present in the data)
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

-- 03 dim_weather (live non-forecast + historical; UTC key, local time kept)
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

-- 04 dim_station initial load (unknown row + current + historical)
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

-- 05 helper: trip station -> station_key (is_current = 1 avoids double rows after SCD2)
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

-- 06 fact_trips
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

-- 07 fact_station_status (station version chosen by snapshot time; not yet tested)
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

-- 09 SCD2 step 1: close versions whose name / region / capacity changed
UPDATE d
SET d.valid_to = CAST(s.updated_at AS datetime2(0)), d.is_current = 0
FROM gold.dim_station d
JOIN lh_silver.dbo.stations s ON s.station_id = d.station_id
WHERE d.station_type = 'current' AND d.is_current = 1
  AND (d.capacity <> s.capacity OR d.station_name <> s.station_name
       OR ISNULL(d.region, 'x') <> ISNULL(s.region, 'x'));

-- 10 SCD2 step 2: open new versions (changed stations + brand new stations)
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