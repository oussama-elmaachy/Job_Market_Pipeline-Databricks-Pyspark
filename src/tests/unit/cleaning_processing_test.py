import pytest
from src.transformations.silver_transformations import (add_job_type,
                                                        add_date_time_job_posted,
                                                        create_id_column,
                                                        create_location_id
                                                        )
from src.utils.spark_utils import get_spark
from datetime import datetime, date

@pytest.mark.parametrize(
    "job_employment_type,job_description, expected",
    [
        ("FREE lance ......", "free-lance", ("Freelance",)),
        ("cdi", "temps plein", ("CDI",)),
        ("contrat à durée déterminée", "cdd", ("CDD",)),
        ("stagiaire.", "stage", ("Stage",)),
        ("alternance", "alternance", ("Alternance",)),
        ("autre", "autre", ("Non précisé",)),
    ],
)
def test_add_job_type(job_employment_type,job_description, expected):
    spark=get_spark()
    df = spark.createDataFrame(
        [(job_employment_type, job_description)],
        ["job_employment_type", "job_description"],
    )

    result = add_job_type(df)

    actual = result.select("job_type").first()

    assert actual == expected  

@pytest.mark.parametrize(
    "job_posted_at_datetime_utc,expected",
    [
        ("2026-10-01T05:38:00.000Z",(datetime(2026,10,1,7,38,0),"07:38:00",date(2026,10,1))),
            ("2026-11-01T05:38:00.000Z",(datetime(2026,11,1,6,38,0),"06:38:00",date(2026,11,1)))
    ]
)


def test_add_date_time_job_posted(job_posted_at_datetime_utc,expected):
    spark=get_spark()
    df = spark.createDataFrame(
        [job_posted_at_datetime_utc],
        ["job_posted_at_datetime_utc"] )

    result = add_date_time_job_posted(df)
    actual_values = result.select("job_posted_at_datetime_paris","job_posted_at_time","job_posted_at_date").first()
    assert actual_values == expected
    
@pytest.mark.parametrize(
    "job_title,expected",
    [
        ("Ingenieur Data Azure/Fabric",("ingenieur_data_azure_fabric",)),
           ("Data Analyste A Paris",("data_analyste_a_paris",)),
           ("ML ingénieur @ Lyon 69",("ml_ingenieur_lyon_69",))
    ]
)
def test_create_id_column(job_title,expected):
    spark=get_spark()
    df = spark.createDataFrame(
        [job_title],
        ["job_title"] 
        )
    result = create_id_column(df,"job_title")
    actual_values = result.select("job_title_id").first()
    assert actual_values == expected

@pytest.mark.parametrize(
    "job_latitude,job_longitude,expected",
    [
        (8.5654,-45.654,("8.56540000--45.65400000",)),
           (1.5654,15.654,("1.56540000-15.65400000",))
    ]   
)
def test_create_location_id(job_latitude,job_longitude,expected):
    spark=get_spark()
    df = spark.createDataFrame(
        [(job_latitude,job_longitude)],
        ["job_latitude","job_longitude"] )

    result = create_location_id(df)
    actual_values = result.select("job_location_id").first()

    assert actual_values == expected
    
   



