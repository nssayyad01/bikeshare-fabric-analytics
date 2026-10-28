CREATE TABLE [gold].[dim_station] (
    [station_key]  INT           NULL,
    [station_id]   VARCHAR (100) NULL,
    [station_name] VARCHAR (200) NULL,
    [region]       VARCHAR (50)  NULL,
    [capacity]     INT           NULL,
    [lat]          FLOAT (53)    NULL,
    [lon]          FLOAT (53)    NULL,
    [station_type] VARCHAR (20)  NULL,
    [valid_from]   DATETIME2 (0) NULL,
    [valid_to]     DATETIME2 (0) NULL,
    [is_current]   INT           NULL
);


GO