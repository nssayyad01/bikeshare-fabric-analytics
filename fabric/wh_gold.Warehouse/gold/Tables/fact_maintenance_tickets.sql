CREATE TABLE [gold].[fact_maintenance_tickets] (
    [ticket_id]          INT           NULL,
    [station_key]        INT           NULL,
    [created_date_key]   INT           NULL,
    [created_at]         DATETIME2 (6) NULL,
    [updated_at]         DATETIME2 (6) NULL,
    [issue_category]     VARCHAR (20)  NULL,
    [predicted_category] VARCHAR (20)  NULL,
    [status]             VARCHAR (10)  NULL,
    [is_open]            INT           NULL,
    [technician_note]    VARCHAR (500) NULL
);


GO