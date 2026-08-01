from pyspark.sql.types import (         StructType,
                                        StructField,
                                        StringType,
                                        IntegerType,
                                        TimestampType,
                                        ArrayType,
                                        BooleanType,
                                        MapType,
                                        LongType,
                                        DoubleType
                                        )

api_job_search_project_schema = StructType([
    StructField("job_id", StringType(), False),
    StructField("job_title", StringType(), True),
    StructField("employer_name", StringType(), True),
    StructField("employer_logo", StringType(), True),
    StructField("employer_website", StringType(), True),
    StructField("job_publisher", StringType(), True),
    StructField("job_employment_type", StringType(), True),
    StructField("job_employment_types", ArrayType(StringType()), True),
    StructField("job_apply_link", StringType(), True),
    StructField("job_apply_is_direct", BooleanType(), True),
    StructField("apply_options",ArrayType(MapType(StringType(), StringType())),True,),
    StructField("job_description", StringType(), True),
    StructField("job_is_remote", BooleanType(), True),
    StructField("job_posted_at", StringType(), True),
    StructField("job_posted_at_timestamp", LongType(), True),
    StructField("job_posted_at_datetime_utc", StringType(), True),
    StructField("job_location", StringType(), True),
    StructField("job_city", StringType(), True),
    StructField("job_state", StringType(), True),
    StructField("job_country", StringType(), True),
    StructField("job_latitude", DoubleType(), True),
    StructField("job_longitude", DoubleType(), True),
    StructField("job_benefits", ArrayType(StringType()), True),
    StructField("job_benefits_strings", ArrayType(StringType()), True),
    StructField("job_google_link", StringType(), True),
    StructField("job_salary", StringType(), True),
    StructField("job_salary_string", StringType(), True),
    StructField("job_min_salary", DoubleType(), True),
    StructField("job_max_salary", DoubleType(), True),
    StructField("job_salary_period", StringType(), True),
    StructField("job_highlights",MapType(StringType(), ArrayType(StringType())),True,),
    StructField("job_onet_soc", StringType(), True),
    StructField("job_onet_job_zone", StringType(), True),
    StructField("employer_reviews",StringType(),True),
    StructField("job_uid", StringType(), True)
])

table_silver_schema_job_search = StructType([
    StructField("job_id", StringType(), False),
    StructField("job_title", StringType(), True),
    StructField("employer_name", StringType(), True),
    StructField("employer_logo", StringType(), True),
    StructField("employer_website", StringType(), True),
    StructField("job_publisher", StringType(), True),
    StructField("job_employment_type", StringType(), True),
    StructField("job_employment_types", ArrayType(StringType()), True),
    StructField("job_apply_link", StringType(), True),
    StructField("job_apply_is_direct", BooleanType(), True),
    StructField("apply_options",ArrayType(MapType(StringType(), StringType())),True,),
    StructField("job_description", StringType(), True),
    StructField("job_is_remote", BooleanType(), True),
    StructField("job_posted_at", StringType(), True),
    StructField("job_posted_at_timestamp", LongType(), True),
    StructField("job_posted_at_datetime_utc", StringType(), True),
    StructField("job_location", StringType(), True),
    StructField("job_city", StringType(), True),
    StructField("job_state", StringType(), True),
    StructField("job_country", StringType(), True),
    StructField("job_latitude", DoubleType(), True),
    StructField("job_longitude", DoubleType(), True),
    StructField("job_benefits", ArrayType(StringType()), True),
    StructField("job_benefits_strings", ArrayType(StringType()), True),
    StructField("job_google_link", StringType(), True),
    StructField("job_salary", StringType(), True),
    StructField("job_salary_string", StringType(), True),
    StructField("job_min_salary", DoubleType(), True),
    StructField("job_max_salary", DoubleType(), True),
    StructField("job_salary_period", StringType(), True),
    StructField("job_highlights",MapType(StringType(), ArrayType(StringType())),True,),
    StructField("job_onet_soc", StringType(), True),
    StructField("job_onet_job_zone", StringType(), True),
    StructField("employer_reviews",StringType(),True),
    StructField("job_uid", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("job_uid", StringType(), True),
    StructField("job_uid", StringType(), True),
    StructField("job_uid", StringType(), True),
    StructField("job_uid", StringType(), True)
])















scope_api_name = "scope_api_job_search_project"
catalog_name   = "job_search_project_catalog"
schema_name    = "job_search_project_schema"
bronze_table   = "table_bronze_job_search"
silver_table   = "table_silver_job_search"
gold_table     = "table_gold_job_search"
checkpoint_table_silver="/Volumes/job_search_project_catalog/job_search_project_schema/job_search_project/checkpoints/silver/table_silver_job_search/"
