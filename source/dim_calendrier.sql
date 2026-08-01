CREATE TABLE IF NOT EXISTS job_search_project_catalog.job_search_project_schema.dim_calendrier (
    date_key              INT ,
    date_value            DATE
    day_number            INT ,
    day_name              STRING ,
    day_of_week           INT ,
    day_of_year           INT ,
    week_number           INT ,
    week_year             INT ,
    month_number          INT ,
    month_name            STRING,
    quarter_number        INT Quarter number,
    quarter_name          STRING,
    year_number           INT,
    is_weekend            BOOLEAN ,
    is_month_start        BOOLEAN ,
    is_month_end          BOOLEAN ,

)