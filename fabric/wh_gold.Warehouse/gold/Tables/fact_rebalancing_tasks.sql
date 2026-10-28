CREATE TABLE [gold].[fact_rebalancing_tasks] (
    [task_id]            INT           NULL,
    [from_station_key]   INT           NULL,
    [to_station_key]     INT           NULL,
    [assigned_date_key]  INT           NULL,
    [completed_date_key] INT           NULL,
    [assigned_at]        DATETIME2 (6) NULL,
    [completed_at]       DATETIME2 (6) NULL,
    [updated_at]         DATETIME2 (6) NULL,
    [bikes_count]        INT           NULL,
    [status]             VARCHAR (10)  NULL,
    [is_completed]       INT           NULL,
    [duration_minutes]   FLOAT (53)    NULL
);


GO