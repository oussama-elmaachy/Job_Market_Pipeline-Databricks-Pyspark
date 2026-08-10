from utils.spark_utils import get_spark
from utils.config import catalog_name,schema_name,bronze_table,silver_table,gold_table,checkpoint_silver_table
from datetime import datetime
from zoneinfo import ZoneInfo
from delta.tables import DeltaTable
from pyspark.sql.functions import  (
        lit,current_timestamp,col,current_timestamp,lower,when,to_timestamp,from_utc_timestamp,date_format,concat_ws,format_number,trim,regexp_replace,translate,initcap,max
        )



#Create silver table
def create_gold_table(df):
    # this is the silver table
    #the main function that process the dataframe from silver table to gold table
    #the function is composed of several functions
    try:    
        """
        df=df.select('job_id','job_title','job_type','location_id','employer_name_id','job_description','job_is_remote','job_posted_at_time','job_posted_at_date','job_publisher_id','job_apply_link','ingestion_timestamp','ingestion_source')
        #print("gold table created")
        """
        print('gold creation is started :  .........')
        df=df.groupBy('job_title_id','job_type','job_location_id','employer_name_id','job_publisher_id').agg(
        max("job_title").alias("job_title"),
          max("job_posted_at_date").alias("job_posted_at_date"),
          max("job_posted_at_time").alias("job_posted_at_time"),
          max("job_description").alias("job_description"),
          max("job_is_remote").alias("job_is_remote"),
          max("job_apply_link").alias("job_apply_link"),
          max("ingestion_timestamp").alias("ingestion_timestamp"),
          max("ingestion_source").alias("ingestion_source")
      )
        (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{catalog_name}.{schema_name}.{gold_table}")
        )   
        print("gold table created" )
    except Exception as e:
        print(e)
        return None


