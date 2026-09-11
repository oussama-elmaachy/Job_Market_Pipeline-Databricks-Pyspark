from datetime import datetime
from zoneinfo import ZoneInfo
from src.utils.config import api_job_search_project_schema 
from delta.tables import DeltaTable
from pyspark.sql.functions import  lit,current_timestamp,col,current_timestamp
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


def create_bronze_df(spark,data,schema_bronze=api_job_search_project_schema):
    try:
        df=spark.createDataFrame(data,schema_bronze)
        df=(
            df.withColumn('ingestion_timestamp',current_timestamp())
            .withColumn("ingestion_source",lit("api_source")) 
             )
        
        return df
    
    except Exception as e:
        empty_df = spark.createDataFrame([], schema_bronze)
        return (
            empty_df.withColumn('ingestion_timestamp',current_timestamp())
                .withColumn("ingestion_source",lit("api_source")) 
        )


def merge_bronze_df_target(spark,df_source,df_target):
    target=DeltaTable.forName(spark,df_target)
    source=df_source

    try:
        ( target.alias("t").merge(source.alias("s"),"t.job_id = s.job_id")
        .whenMatchedUpdate(
            condition=(
                (col("s.job_location").isNotNull() & (col("t.job_location") != col("s.job_location"))) |
                (col("s.job_title").isNotNull() & (col("t.job_title") != col("s.job_title"))) |
                (col("s.job_description").isNotNull() & (col("t.job_description") != col("s.job_description"))) |
                (col("s.job_salary").isNotNull() & (col("t.job_salary") != col("s.job_salary"))) |
                (col("s.job_salary_string").isNotNull() & (col("t.job_salary_string") != col("s.job_salary_string")))
            ),
            set={
                "job_location": "coalesce(s.job_location, t.job_location)",
                "job_title": "coalesce(s.job_title, t.job_title)",
                "job_description": "coalesce(s.job_description, t.job_description)",
                "job_salary": "coalesce(s.job_salary, t.job_salary)",
                "job_salary_string": "coalesce(s.job_salary_string, t.job_salary_string)"
            }
            )
            .whenNotMatchedInsertAll() \
            .execute()
            )    
        
        merged_success=True
        metrics=target.history(1).select("operationMetrics").collect()[0][0]
        inserted = int(metrics.get("numTargetRowsInserted", 0))
        updated = int(metrics.get("numTargetRowsUpdated", 0))
        deleted = int(metrics.get("numTargetRowsDeleted", 0))

        return merged_success,inserted,updated,deleted

    except Exception as e:
        merged_success=False

        return merged_success,0,0,0
    
# the merge fucntion in SQL 
    def merge_df_target_sql(spark,df_source,df_target):

        spark.sql(f"""
            MERGE INTO {df_target} t
            USING {df_source} s
            ON t.id=s.job_id
            WHEN MATCHED THEN update set *
            WHEN NOT MATCHED THEN insert *
        """)
