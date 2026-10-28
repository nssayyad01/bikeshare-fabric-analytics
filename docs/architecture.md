# Architecture

```mermaid
flowchart LR
    subgraph Sources
        NEON[(Neon Postgres<br/>stations, tickets, tasks, feedback)]
        GBFS[GBFS station status<br/>JSON every 15 min]
        OMETEO[Open-Meteo weather<br/>JSON hourly]
        CSV[Citi Bike trip CSVs<br/>2013, 2024]
    end

    subgraph Bronze [lh_bronze]
        CJ[CopyJob_1<br/>incremental by updated_at]
        RAWG[GBFS JSON files]
        RAWW[Weather JSON files]
    end

    subgraph Silver [lh_silver]
        SG[gbfs_station_status]
        SW[weather_hourly + history]
        SO[stations, tickets, tasks, text_docs, station_map, trips]
        LOG[(processed_files)]
    end

    subgraph Gold [wh_gold]
        DIM[dim_date, dim_weather, dim_station SCD2]
        FACT[fact_station_status, fact_trips,<br/>fact_maintenance_tickets, fact_rebalancing_tasks, fact_rider_feedback]
        AUDIT[(audit_runs)]
    end

    SM[sm_bikeshare<br/>Direct Lake]
    RPT[rpt_bikeshare<br/>Power BI report]

    NEON --> CJ --> SO
    GBFS --> RAWG --> SG
    OMETEO --> RAWW --> SW
    CSV --> SO
    SW --> LOG
    SG --> DIM
    SW --> DIM
    SO --> DIM
    DIM --> FACT
    FACT --> AUDIT
    FACT --> SM --> RPT
```

## Pipelines
- **pl_ingest_gbfs** (15 min), **pl_ingest_weather** (hourly): Bronze
- **pl_refresh_master** (hourly): nb_refresh_gbfs, nb_refresh_weather, Gold daily refresh, audit checks
- **pl_sync_operations** (manual, after simulators): CopyJob_1, nb_refresh_station_mapping, Gold ops refresh, ops audit
- **nb_maintenance_delta** (weekly, manual): OPTIMIZE and VACUUM