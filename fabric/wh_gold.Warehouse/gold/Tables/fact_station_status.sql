CREATE TABLE [gold].[fact_station_status] (
    [station_key]          INT           NULL,
    [date_key]             INT           NULL,
    [weather_key]          BIGINT        NULL,
    [snapshot_ts]          DATETIME2 (6) NULL,
    [snapshot_ts_local]    DATETIME2 (0) NULL,
    [num_bikes_available]  INT           NULL,
    [num_ebikes_available] INT           NULL,
    [num_bikes_disabled]   INT           NULL,
    [num_docks_available]  INT           NULL,
    [num_docks_disabled]   INT           NULL,
    [is_installed]         INT           NULL,
    [is_renting]           INT           NULL,
    [is_returning]         INT           NULL,
    [is_empty]             INT           NOT NULL,
    [is_full]              INT           NOT NULL
);


GO