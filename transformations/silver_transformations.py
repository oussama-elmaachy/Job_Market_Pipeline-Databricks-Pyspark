from utils.spark_utils import get_spark
from utils.geo_location import get_geo_location,schema_location
from datetime import datetime
from utils.config import (catalog_name,schema_name,bronze_table,silver_table,checkpoint_silver_table,publisher_table,employer_table,location_table)
from zoneinfo import ZoneInfo
from delta.tables import DeltaTable
from pyspark.sql.functions import  (
        lit,current_timestamp,col,current_timestamp,lower,when,to_timestamp,from_utc_timestamp,date_format,concat_ws,format_number,trim,regexp_replace,translate,initcap,max
        )
#function to use for the silver table

def filter_silver_df(df):
    df=df.selectExpr("job_id",
                     "job_title",
                     "employer_name",
                     "job_publisher",
                    "job_employment_type",
                    "job_apply_link",
                    "job_apply_link_direct"
                     "job_description",
                     "job_is_remote",
                     "job_posted_at_datetime_utc",
                     "job_latitude",
                     "job_longitude",
                    "ingestion_timestamp",
                    "ingestion_source"
                    )


    return (
                    df.filter(col('job_posted_at_datetime_utc').isNotNull())
        )

def add_job_type(df):

    df=df.withColumn('job_employment_type',lower(col('job_employment_type')))

    freelance_condition = (
        col('job_employment_type').contains('freelance')|col('job_employment_type').contains('free-lance')|col('job_employment_type').contains('free lance'))
    cdi_condition = (
        col('job_employment_type').contains('stage') | col('job_employment_type').contains('alternan') | col('job_employment_type').contains('plein temps') | col('job_employment_type').contains('temps partiel')
        )

    return df.withColumn('job_type',
        when(col('job_employment_type').contains('stage'),lit('Stage'))
        .when(col('job_employment_type').contains('alternan'),lit('Alternance'))
        .when(freelance_condition,lit('Freelance'))
        .when(cdi_condition,lit('CDI'))                       
        .otherwise(lit('non definie'))
            )
    
def add_date_time_job_posted(df):
    return (
    df
    .withColumn('job_posted_at_datetime_paris',
            to_timestamp(col("job_posted_at_datetime_utc"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))
    .withColumn('job_posted_at_datetime_paris',
                from_utc_timestamp(col("job_posted_at_datetime_utc"),"Europe/Paris"))
    .withColumn('job_posted_at_time',date_format(col("job_posted_at_datetime_paris"),
                "HH:mm:ss")
            )
    .withColumn('job_posted_at_date',col('job_posted_at_datetime_paris').cast('date'))
    )


#function to use in dim_location

def create_location_id(df):
    return (
        df

        .withColumn('job_location_id',concat_ws('-',format_number(col('job_latitude'),8),format_number(col('job_longitude'),8)))

    )
#function to create ID from a column : used in dim_publisher,dim_employer and silver table

def create_id_column(df,column_name):
    normalized_text=regexp_replace(
                                trim(
                                    regexp_replace(
                                                    lower(
                                                            translate(col(column_name),"àáâäãåçèéêëìíîïñòóôöõùúûüÿ",
                                                                                            "aaaaaaceeeeiiiinooooouuuuy"
                                                        )
                                                    )
                            ,'[^a-z0-9]',' '
                                            )
                ),r"\s+","_" 
          )
                            
    
    return (
            df
            .withColumn(column_name+'_id',normalized_text)
            .withColumn(column_name,
                        initcap(
                            regexp_replace(col(column_name+'_id'),"_"," ")
                            )
                        )
        )

#Create silver table
def create_silver_table(df):
    # this is the silver table
    #the main function that process the dataframe from bronze table to silver table
    #the function is composed of several functions
    try:
        df=filter_df(df))
        df=add_job_type(df)
        df=add_date_time_job_posted(df)
        df=create_location_id(df)
        df=create_id_column(df,"employer_name")
        df=create_id_column(df,"job_publisher")
        df=create_id_column(df,"job_title")

        return df
    except Exception as e:
        print(e)
        return None
    
def merge_silver_table(batch_df,batch_id):
    print("========== BATCH START ==========")
    print("batch_id:", batch_id)
    print("rows:", batch_df.count())
    spark = get_spark()
    batch_df = create_silver_table(batch_df)

    print("Rows after transformation:", batch_df.count())
    target = DeltaTable.forName(spark, f"{catalog_name}.{schema_name}.{silver_table}")
    source = batch_df
    
    ( 
     target.alias("t").merge(source.alias("s"),"t.job_id = s.job_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
            )




def create_dim_publisher(df):
    df=df.select('job_publisher').distinct()
    df=create_id_column(df,'job_publisher')
    return df

def create_dim_employer(df):
    df = df.select("employer_name","employer_logo","employer_website").distinct()
    df = create_id_column(df, "employer_name")
    df = df.dropDuplicates(["employer_name_id"])
    return df

def create_dim_location(df):
    df=df.select('job_latitude','job_longitude').distinct()
    df=create_location_id(df)
    return df
    
""" 
    except Exception as e:
        print("merge failed because:", e)
"""


def merge_dim_publisher(batch_df,batch_id):
    print("batch:", batch_id)
    print("Rows before transform:", batch_df.count())    
    batch_df = batch_df.select("job_publisher")
    batch_df = create_id_column(batch_df, "job_publisher")
    batch_df = batch_df.dropDuplicates(["job_publisher_id"])

    print("Rows after transform:", batch_df.count())
    spark = get_spark()
    target = DeltaTable.forName(spark,f"{catalog_name}.{schema_name}.{publisher_table}")
    source=batch_df
    (
        target.alias("t")
        .merge(source.alias("s"),"t.job_publisher_id = s.job_publisher_id")
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("merge finished")


def merge_dim_employer(batch_df,batch_id):
    batch_df = batch_df.select("employer_name","employer_logo",
                               "employer_website").distinct()
    batch_df= create_id_column(batch_df, "employer_name")
    batch_df= (
        batch_df.groupBy("employer_name_id")
        .agg(max("employer_name").alias("employer_name"),
            max("employer_logo").alias("employer_logo"),
            max("employer_website").alias("employer_website"))
        )
    spark = get_spark()
    target = DeltaTable.forName(spark,f"{catalog_name}.{schema_name}.{employer_table}")
    source=batch_df
    (
        target.alias("t").merge(source.alias("s"),"t.employer_name_id = s.employer_name_id")
        .whenMatchedUpdate(set={
                            "employer_logo": 
                                "coalesce(s.employer_logo, t.employer_logo)",
                             "employer_website": 
                                 "coalesce(s.employer_website, t.employer_website)"})
        .whenNotMatchedInsertAll()
        .execute()
    )


def merge_dim_location(batch_df,batch_id):
    batch_df=batch_df.select('job_latitude','job_longitude').distinct()
    batch_df=create_location_id(batch_df)
    batch_df=batch_df.mapInPandas(get_geo_location, schema=schema_location)
    spark = get_spark()
    target = DeltaTable.forName(spark,f"{catalog_name}.{schema_name}.{location_table}")
    source=batch_df
    (
        target.alias("t").merge(source.alias("s"),"t.job_location_id = s.job_location_id")
        .whenNotMatchedInsertAll()
        .execute()
        )
