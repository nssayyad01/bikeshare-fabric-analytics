# Data Dictionary

Project: CitiBike Rebalancing Command Center (Microsoft Fabric).
Layers: Silver = `lh_silver.dbo` (Lakehouse, PySpark-written Delta tables). Gold = `wh_gold.gold` (Warehouse, star schema). Keys ending in `_key` are Gold surrogate keys. `date_key` format is YYYYMMDD (int). `weather_key` format is YYYYMMDDHH (bigint, UTC hour).

All timestamps in Silver and Gold are UTC unless the column name ends in `_local`.

---

## Gold (wh_gold.gold)

### Dimensions

**dim_date** (one row per calendar date present in the data)
| Column | Type | Description |
|---|---|---|
| date_key | int | PK. YYYYMMDD |
| full_date | date | Calendar date |
| year_num, month_num, day_num | int | Date parts |
| month_name, day_name | varchar | e.g. "October", "Monday" |
| is_weekend | int | 1 for Saturday or Sunday |
| day_sort | int | 1 (Monday) to 7 (Sunday). Used to sort day names in the Demand chart |

**dim_station** (SCD Type 2, one row per station version)
| Column | Type | Description |
|---|---|---|
| station_key | int | PK, surrogate. -1 is the Unknown station |
| station_id | varchar | Citi Bike / GBFS station ID (historical stations use `hist_<schema>_<trip id>`) |
| station_name, region | varchar | Station name; region from GBFS |
| capacity | int | Dock capacity in this version |
| lat, lon | float | Coordinates |
| station_type | varchar | `current` (in Neon/GBFS), `historical` (trip data only), `unknown` |
| valid_from, valid_to | datetime2 | Version range. valid_from is when our Silver load saw the change, not the real-world change time |
| is_current | int | 1 for the active version |

**dim_weather** (one row per UTC hour)
| Column | Type | Description |
|---|---|---|
| weather_key | bigint | PK, YYYYMMDDHH (UTC) |
| hour_ts_utc, hour_ts_local | datetime2 | UTC hour and NYC local time |
| temperature_c, precipitation_mm, wind_speed_kmh | float | Open-Meteo values |
| is_raining | int | 1 if precipitation > 0 |
| source | varchar | `live` (Open-Meteo forecast API, actual hours) or `history` (Dec 2013, Jan 2024) |
| weather_label | varchar | `Dry` or `Rain/Snow` (note: snow is not separated from rain) |

### Facts

**fact_station_status** (one row per station per GBFS snapshot)
| Column | Type | Description |
|---|---|---|
| station_key | int | FK to dim_station (-1 if unknown) |
| date_key | int | FK to dim_date (local NYC date) |
| weather_key | bigint | FK to dim_weather (live, UTC hour) |
| snapshot_ts, snapshot_ts_local | datetime2 | Snapshot time, UTC and local |
| num_bikes_available, num_ebikes_available, num_bikes_disabled | int | Bike counts |
| num_docks_available, num_docks_disabled | int | Dock counts |
| is_installed, is_renting, is_returning | int | GBFS flags |
| is_empty | int | 1 if num_bikes_available = 0 |
| is_full | int | 1 if num_docks_available = 0 |

**fact_trips** (one row per trip, 2013 and 2024 combined)
| Column | Type | Description |
|---|---|---|
| trip_id | varchar | 2013 rows: hash of bike id, start time, start station. 2024: ride_id |
| date_key | int | FK to dim_date (start date) |
| start_station_key, end_station_key | int | FK to dim_station (-1 if unknown) |
| weather_key | bigint | FK to dim_weather (history, matched by local start hour) |
| started_at, ended_at | datetime2 | Trip start and end |
| duration_sec | int | Trip length in seconds |
| user_type | varchar | `member` or `casual` |
| rideable_type | varchar | 2024 only; `unknown` for 2013 |
| source_schema | varchar | `v2013` or `v2024` |
| start_hour | smallint | Hour of started_at (assumes NYC local time, not verified) |

**fact_maintenance_tickets** (simulated, one row per ticket)
| Column | Type | Description |
|---|---|---|
| ticket_id | int | PK |
| station_key | int | Station version at ticket creation |
| created_date_key | int | FK to dim_date |
| created_at, updated_at | datetime2 | |
| issue_category | varchar | Simulator's category (brake, dock, tire, screen, battery) |
| predicted_category | varchar | Rule-based classifier output |
| status | varchar | `open` or `closed` |
| is_open | int | 1 if open |
| technician_note | varchar | Free text (simulated) |

**fact_rebalancing_tasks** (simulated, one row per task)
| Column | Type | Description |
|---|---|---|
| task_id | int | PK |
| from_station_key, to_station_key | int | Station versions at assignment time |
| assigned_date_key, completed_date_key | int | FK to dim_date; completed is NULL while pending |
| assigned_at, completed_at, updated_at | datetime2 | |
| bikes_count | int | Bikes to move |
| status | varchar | `pending` or `completed` |
| is_completed | int | 1 if completed |
| duration_minutes | float | completed_at minus assigned_at |

**fact_rider_feedback** (simulated, one row per feedback)
| Column | Type | Description |
|---|---|---|
| feedback_doc_id | varchar | `feedback_<id>` |
| station_key | int | Station version at feedback time |
| created_date_key | int | FK to dim_date |
| created_at, updated_at | datetime2 | |
| true_category | varchar | Simulator answer key. Used only to measure classifier accuracy |
| predicted_category | varchar | Classifier output |
| predicted_sentiment | varchar | positive, negative, neutral |
| category_correct | int | 1 if predicted = true |
| feedback_text | varchar | Free text |

**stg_station_lookup** (helper, used to build fact_trips station keys)
| Column | Type | Description |
|---|---|---|
| source_schema, trip_station_id, trip_station_name | varchar | Trip-side station identifiers |
| station_key | int | Resolved current station key |

### Audit

**audit_runs** (one row per check per pipeline run)
| Column | Type | Description |
|---|---|---|
| run_ts | datetime2 | UTC time the check ran |
| check_name | varchar | e.g. unknown_station_rows, orphan_dates, duplicate_keys, freshness_minutes_behind_silver, ops_tickets_gold_vs_silver |
| result_value | bigint | Measured value |
| expected_value | bigint | Target (NULL if none) |
| status | varchar | PASS, WARN or FAIL |

---

## Silver (lh_silver.dbo)

**gbfs_station_status** (one row per station per GBFS snapshot, ~450 snapshots so far)
| Column | Type | Description |
|---|---|---|
| station_id | varchar | GBFS station ID |
| snapshot_ts, last_reported_ts | datetime2 | Snapshot time (from last_updated) and station's last report |
| num_bikes_available ... is_returning | int | GBFS fields (see Gold fact_station_status) |
| source_file_name | varchar | JSON file the row came from (used for incremental loads) |
| _loaded_at | datetime2 | Silver load time |

**stations** (Neon stations, current list)
| Column | Type | Description |
|---|---|---|
| station_id, station_name, region | varchar | |
| capacity, lat, lon | int, float | |
| status | varchar | From Neon |
| updated_at | datetime2 | Neon change time (drives SCD2) |
| capacity_valid | bit | 0 when capacity is 0 (out of service) |
| station_name_norm | varchar | lower(trim(name)) |

**maintenance_tickets**, **rebalancing_tasks**: Neon operational tables, merged by key (ticket_id, task_id). `is_open` and `is_completed` are bit flags. `doc_id` links to `text_docs`. `duration_minutes` is computed from assigned_at and completed_at.

**text_docs** (technician notes and rider feedback, one table)
| Column | Type | Description |
|---|---|---|
| doc_id | varchar | `ticket_<id>` or `feedback_<id>` |
| source | varchar | `maintenance_ticket` or `rider_feedback` |
| station_id, created_at, updated_at | | |
| text | varchar | Free text |
| label_original | varchar | Answer key (simulated) |
| predicted_category, is_match | varchar, bit | Classifier output and whether it matched |
| predicted_sentiment | varchar | Added after the first build. NULL for maintenance tickets |
| _loaded_at | datetime2 | |

**station_map** (trip-side station name/ID to GBFS station)
| Column | Type | Description |
|---|---|---|
| source_schema, trip_station_id, trip_station_name | varchar | Trip-side identity |
| lat, lng | float | Average trip coordinates |
| trip_mentions | bigint | Number of trip rows using this station |
| station_id | varchar | Matched GBFS/Neon station (NULL if ambiguous or not found) |
| match_status | varchar | `matched`, `historical_only`, `ambiguous_name` |

**trips** (2013 and 2024, common schema)
| Column | Type | Description |
|---|---|---|
| trip_id ... source_schema | | Same as Gold fact_trips |
| start_lat, start_lng, end_lat, end_lng | float | Trip coordinates |
| has_missing_station | bit | 1 if start or end station ID is null |

**weather_hourly** (live Open-Meteo, latest fetch per hour)
| Column | Type | Description |
|---|---|---|
| hour_ts_utc | datetime2 | PK |
| temperature_c, precipitation_mm, wind_speed_kmh | float | |
| fetched_at | datetime2 | When the winning JSON was fetched |
| source_file_name | varchar | JSON file |
| is_forecast | bit | 1 if the hour was still a forecast at fetch time |

**weather_hourly_history**: Dec 2013 and Jan 2024 archive hours. Same value columns, no fetch metadata.

**processed_files** (load log)
| Column | Type | Description |
|---|---|---|
| file_name | varchar | Weather JSON file already merged |
| source | varchar | `weather` (GBFS uses gbfs_station_status.source_file_name instead) |
| processed_at | datetime2 | |

---
---

## Verified assumptions

- **start_hour (fact_trips):** Treated as NYC local time. The hourly pattern matches NYC commute peaks (08:00 and 17:00) and the overnight trough (03:00 to 04:00), so this is an inference from the data. Not confirmed against Citi Bike's documentation. Checked on the 2024 data only.
- **trip_id (2013 trips):** SHA-256 hash of `bikeid|starttime|start station id`. The 2013 files have no unique trip ID, so two trips with the same bike, start time and start station would collapse into one row. This is a known limitation.
- **Historical stations:** 174,780 trips (about 13% of fact_trips) start at a station with `station_type = 'historical'`. These are included in trip counts. Their coordinates come from the trip data (averaged), not from GBFS.
