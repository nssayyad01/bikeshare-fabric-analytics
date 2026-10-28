CREATE TABLE [gold].[fact_rider_feedback] (
    [feedback_doc_id]     VARCHAR (30)  NULL,
    [station_key]         INT           NULL,
    [created_date_key]    INT           NULL,
    [created_at]          DATETIME2 (6) NULL,
    [updated_at]          DATETIME2 (6) NULL,
    [true_category]       VARCHAR (30)  NULL,
    [predicted_category]  VARCHAR (30)  NULL,
    [predicted_sentiment] VARCHAR (10)  NULL,
    [category_correct]    INT           NULL,
    [feedback_text]       VARCHAR (500) NULL
);


GO