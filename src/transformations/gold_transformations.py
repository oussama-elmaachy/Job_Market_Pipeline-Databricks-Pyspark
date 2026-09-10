from src.utils.spark_utils import get_spark
from src.utils.config import catalog_name,schema_name,bronze_table,silver_table,gold_table,checkpoint_silver_table
from delta.tables import DeltaTable
from pyspark.sql.functions import max



#Create gold table

def create_gold_table(df):
    # this is the gold table
    #the main function that process the dataframe from silver table to gold table

    try:    

        df= (
            df.groupBy('job_title_id','job_type','job_location_id','employer_name_id','job_publisher_id')
                .agg(
                        max("job_title").alias("job_title"),
                        max("job_top_skills").alias("job_top_skills"),
                        max("job_posted_at_date").alias("job_posted_at_date"),
                        max("job_posted_at_time").alias("job_posted_at_time"),
                        max("job_description").alias("job_description"),
                        max("job_is_remote").alias("job_is_remote"),
                        max("job_apply_link").alias("job_apply_link"),
                        max("ingestion_timestamp").alias("ingestion_timestamp"),
                        max("ingestion_source").alias("ingestion_source")
                     )
        )

        (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{catalog_name}.{schema_name}.{gold_table}")
        )   

    except Exception as e:

        return None


