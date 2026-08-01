from utils.spark_utils import get_spark
from datetime import datetime
from zoneinfo import ZoneInfo
from delta.tables import DeltaTable
from pyspark.sql.functions import  (
        lit,current_timestamp,col,current_timestamp,lower,when,to_timestamp,from_utc_timestamp,date_format,concat_ws,format_number,trim,regexp_replace,translate,initcap
        )
#function to use for the silver table

def filter_df(df):
    return (
        df.filter(col('job_posted_at_datetime_utc').isNotNull())
        )

def add_job_type(df):
    dff=df.withColumn('job_employment_type',lower(col('job_employment_type')))
    freelance_condition = (
        col('job_employment_type').contains('freelance')|col('job_employment_type').contains('free-lance')|col('job_employment_type').contains('free lance'))
    cdi_condition = (
        col('job_employment_type').contains('stage') | col('job_employment_type').contains('alternan') | col('job_employment_type').contains('plein temps') | col('job_employment_type').contains('temps partiel')
        )


    return dff.withColumn('job_type',
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
        .withColumn('location_id',
                concat_ws('_',
                          format_number(col('job_latitude'),9),
                          format_number(col('job_longitude'),9))
                    )
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
        df=filter_df(df)
        df=add_job_type(df)
        df=add_date_time_job_posted(df)
        df=create_location_id(df)
        df=create_id_column(df,"employer_name")
        df=create_id_column(df,"job_publisher")
        #print("silver table created")
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

    target = DeltaTable.forName(spark, "job_search_project_catalog.job_search_project_schema.table_silver_job_search")
    source = batch_df
    try:
        ( target.alias("t").merge(source.alias("s"),"t.job_id = s.job_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll() 
            .execute()
            )
        metrics=target.history(1).select("operationMetrics").collect()[0][0]
        inserted = int(metrics.get("numTargetRowsInserted", 0))
        updated = int(metrics.get("numTargetRowsUpdated", 0))
        deleted = int(metrics.get("numTargetRowsDeleted", 0))
        merged_success=True
        print("merge success") 
        print(f"inserted: {inserted}, updated: {updated}, deleted: {deleted}")
        
         
    except Exception as e:
        print("merge failed because:", e)



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
