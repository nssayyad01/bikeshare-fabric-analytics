CREATE TABLE [gold].[audit_runs] (
    [run_ts]         DATETIME2 (0) NOT NULL,
    [check_name]     VARCHAR (100) NOT NULL,
    [result_value]   BIGINT        NULL,
    [expected_value] BIGINT        NULL,
    [status]         VARCHAR (10)  NOT NULL
);


GO