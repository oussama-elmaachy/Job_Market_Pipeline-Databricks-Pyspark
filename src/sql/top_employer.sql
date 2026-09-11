use catalog job_search_project_catalog;
use schema job_search_project_schema;

with tbl_top_company as (
-- this is the top 5 companies with the most job offers
select 
    employer_name_id,
    count(*) as total_offres
from job_search_project_catalog.job_search_project_schema.gold_table_job_search
group by employer_name_id
order by total_offres desc
limit 5
), tbl_top_company_names(
-- this table to do a join with dim_job_employer to get the company name and website
select 
    emp.employer_name as company,
   case 
        when emp.employer_website is null then 'No website'
        else emp.employer_website
    end 
        as web_site,
    r.total_offres as offres
from tbl_top_company as r
left join dim_job_employer as emp
on r.employer_name_id = emp.employer_name_id
)

select *
from tbl_top_company_names
