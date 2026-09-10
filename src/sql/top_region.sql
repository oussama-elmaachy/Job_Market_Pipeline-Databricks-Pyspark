use catalog job_search_project_catalog;
use schema job_search_project_schema;

with tbl_gold_loc_join as (
-- this table joins the gold table with the location table to get the location city and state of the job offer   
    select 
        l.job_state as region,
        l.job_city as city,
        count(*) as total_offres
    from gold_table_job_search as g
    inner join dim_job_location as l
    on g.job_location_id=l.job_location_id 
    group by l.job_state,l.job_city
), tbl_top_region as (
-- this table is to keep only top 5 regions per offres    
select 
    region,
    city,
    total_offres,
    sum(total_offres) over(partition by region) as total_offres_region
from tbl_gold_loc_join
qualify dense_rank() over(order by sum(total_offres) over(partition by region) desc) <= 5
order by total_offres_region desc,total_offres desc,region asc,city asc
)

select *
from tbl_top_region

