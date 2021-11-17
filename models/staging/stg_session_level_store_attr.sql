with user_recode as
    (
    select 
        session_id
        ,aest_datetime
        ,hit_number
        ,CASE 
            WHEN user_region = 'Australia' THEN 'au'
            WHEN user_region = 'United Kingdom' THEN 'uk'
            WHEN user_region = 'South Africa' THEN 'za'
            WHEN user_region = 'New Zealand' THEN 'nz'
            WHEN user_region = 'United States' THEN 'us'
            WHEN user_region = 'Singapore' THEN 'sg'
            WHEN user_region = 'Malaysia' THEN 'my'
            WHEN user_region = 'Hong Kong' THEN 'hc'
        ELSE 'no site' END AS case_site_region_recode  
    from {{ ref('GA_hits_rollup')}}
    order by 1,2 asc
    )

select DISTINCT
    session_id
    ,aest_datetime
    ,LAST_VALUE(case_site_region_recode) OVER (PARTITION BY session_id ORDER BY hit_number ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
        as site_region_recode   
FROM user_recode 