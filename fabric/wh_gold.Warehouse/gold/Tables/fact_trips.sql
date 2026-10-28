CREATE TABLE [gold].[fact_trips] (
    [trip_id]           VARCHAR (64)  NULL,
    [date_key]          INT           NULL,
    [start_station_key] INT           NULL,
    [end_station_key]   INT           NULL,
    [weather_key]       BIGINT        NULL,
    [started_at]        DATETIME2 (6) NULL,
    [ended_at]          DATETIME2 (6) NULL,
    [duration_sec]      INT           NULL,
    [user_type]         VARCHAR (10)  NULL,
    [rideable_type]     VARCHAR (30)  NULL,
    [source_schema]     VARCHAR (10)  NULL,
    [start_hour]        SMALLINT      NULL
);


GO