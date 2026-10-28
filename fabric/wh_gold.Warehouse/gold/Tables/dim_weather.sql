CREATE TABLE [gold].[dim_weather] (
    [weather_key]      BIGINT        NULL,
    [hour_ts_utc]      DATETIME2 (6) NULL,
    [hour_ts_local]    DATETIME2 (0) NULL,
    [temperature_c]    FLOAT (53)    NULL,
    [precipitation_mm] FLOAT (53)    NULL,
    [wind_speed_kmh]   FLOAT (53)    NULL,
    [is_raining]       INT           NOT NULL,
    [source]           VARCHAR (10)  NULL,
    [weather_label]    VARCHAR (12)  NULL
);


GO