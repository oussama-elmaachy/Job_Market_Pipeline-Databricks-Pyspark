import reverse_geocoder as rg
import pandas as pd
from pyspark.sql.types import  StructType, StructField,StringType,DoubleType

# this file is used to get the location of the job from the latitude and longitude
# the function is used in the job_location notebook
# the function uses the API reverse_geocoder to get the location of the job
# the library  is mentionned in the requirements.txt file

schema_location = StructType([

    StructField("job_latitude", DoubleType(), True),
    StructField("job_longitude", DoubleType(), True),
    StructField("job_location_id", StringType(), True),
    StructField("job_city", StringType(), True),
    StructField("job_state", StringType(), True),
    StructField("job_country", StringType(), True)
])

def get_geo_location(iterator):
    for pd_df in iterator:
        
        city=  []
        state = []
        country = []

        for lat, lon in zip(pd_df['job_latitude'], pd_df['job_longitude']):

            try:
                results = rg.search([(lat, lon)])
                city.append(results[0]['name'])
                state.append(results[0]['admin1'])
                country.append(results[0]['cc'])
            except:            
                city.append("non definé")
                state.append("non definé")
                country.append("non definé")
            
        pd_df['job_city'] = city
        pd_df['job_state'] = state
        pd_df['job_country'] = country
        
        yield pd_df

















