CREATE TABLE [gold].[dim_date] (
    [date_key]   INT          NULL,
    [full_date]  DATE         NULL,
    [year_num]   INT          NULL,
    [month_num]  INT          NULL,
    [month_name] VARCHAR (20) NULL,
    [day_num]    INT          NULL,
    [day_name]   VARCHAR (20) NULL,
    [is_weekend] INT          NOT NULL,
    [day_sort]   INT          NULL
);


GO