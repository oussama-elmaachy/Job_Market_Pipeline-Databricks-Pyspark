CREATE TABLE IF NOT EXISTS job_search_project_catalog.job_search_project_schema.gold_table_job_search(
    job_title_id string,
    job_title STRING,
    job_top_skills string,
    job_type string,
    job_location_id string ,
    employer_name_id string, 
    job_description STRING,
    job_is_remote BOOLEAN,
    job_posted_at_time string, 
    job_posted_at_date date, 
    job_publisher_id string,
    job_apply_link STRING,
    ingestion_timestamp TIMESTAMP,
    ingestion_source STRING
)
