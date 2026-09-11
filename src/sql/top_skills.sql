with 
        tbl_list_skills as (
        -- this table is for creating a list of skills as an array per row
                                select 
                                        split(lower(job_top_skills),",")  as skills
                                from job_search_project_catalog.job_search_project_schema.gold_table_job_search
) ,     tbl_explode_skills as (
        -- this table is for exploding the array of skills into a new rows
                                select 
                                        explode(skills) as skill
                                from tbl_list_skills
),
        tbl_trim_skills as (
        -- this table is for removing space in the beginning and the end of each skills and replace special characters like "é" to "e" 
                                select 
                                        translate(trim(skill),"éàèùêîôûç","eaeaeiouc") as skill
                                from tbl_explode_skills
),

        tbl_no_special_char_skills as (
        -- this table is for removing special characters like '( ' or ' - ' from the skills
                                select 
                                        regexp_replace(skill, "[^a-z0-9]"," ") as skill
                                from tbl_trim_skills
),
        tbl_standardize_skills as (
        -- this table for standardizing the skills
                                select case
                                        when            skill like  '%power bi%' 
                                                        or skill like '%powerquery%'  
                                                        or skill like '%power query%' 
                                                        or skill like '%powerbi%'  
                                                then 'power bi'
                                        when skill like  '%looker%' then 'looker'
                                        when skill like  '%snowflake%' then 'snowflake'
                                        when skill like  '%google cloud%' 
                                                or skill like  '%gcp%' 
                                                or skill like '%bigquery%'
                                                or skill like '%big query%'
                                                then 'google cloud platform'
                                        when skill like  '%databricks%' then 'databricks'
                                        when skill like  '%aws%' then 'aws'
                                        when            skill like  '%azure%' 
                                                        or skill like '%fabric%'
                                                then 'azure / fabric'
                                        when skill like  '%python%' then 'python'
                                        when skill like  '%sql%' then 'sql'
                                        when skill like  '%dbt%' then 'dbt'
                                        when skill like  '%big data%' then 'big data'
                                        when            skill like  '%intelligence artificielle%' 
                                                        or skill like  '%ia%' 
                                                        or skill like  '%ai%' 
                                                        or skill like  '%machine learning%'
                                                        or skill like  '%deep learning%'
                                                then 'intelligence artificielle'
                                        when skill like  '%data engineering%' then 'data engineering'
                                        when skill like  '%data science%' then 'data science'
                                        when            skill like  '%data analytics%'
                                                        or skill like '%analyse de donnees%' 
                                                        or skill like '%data analysis%'
                                                        or skill like '%analytics%'
                                                then 'data analytics'
                                        when skill like  '%data visualization%' then 'data visualization'
                                        else skill
                                        end
                                                as skill
                                from 
                                        tbl_no_special_char_skills)

                select 
                        initcap(skill) as skill,count(*) as nb_skills_per_offres
                from 
                        tbl_standardize_skills
                group by
                        skill
                order by 
                        nb_skills_per_offres desc
                limit 
                        10 