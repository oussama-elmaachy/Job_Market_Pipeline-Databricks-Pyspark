CREATE OR REPLACE TABLE job_search_project_catalog.job_search_project_schema.dim_calendrier as 
WITH sequence_date AS (
    select explode(
        sequence(
            date('2026-01-01'),current_date() , interval 1 day)
            ) as date_
)
select 
    date_,
    year(date_) as year,
    month(date_) as month,
    day(date_) as day,
    case 
        when dayofweek(date_)= 1 then 7
        else dayofweek(date_)-1
    end 
            as day_of_week,
    case 
        when dayofweek(date_) in (1,7) then "weekend"
        else "weekday "
        end as day_of_week_type,        
    dayofmonth(date_) as day_of_month,
    dayofyear(date_) as day_of_year,
    weekofyear(date_) as week_of_year,
    date_format(date_,'E') as day_of_weekname,
    date_format(date_,'MMMM') as month_name,
    date_format(date_,'MMM') as month_name_short
   

from sequence_date
