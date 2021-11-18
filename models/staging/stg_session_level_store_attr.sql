with user_recode as
    (
    select 
        session_id
        ,aest_datetime
        ,hit_number
        ,CASE 
            WHEN site_region IN ('au', 'uk', 'za', 'nz', 'us', 'sg', 'my', 'hc') THEN site_region
            WHEN site_region IS NULL AND user_region = 'Australia' THEN 'au' 
            WHEN site_region IS NULL AND user_region = 'United Kingdom' THEN 'uk' 
            WHEN site_region IS NULL AND user_region = 'South Africa' THEN 'za'
            WHEN site_region IS NULL AND user_region = 'New Zealand' THEN 'nz'
            WHEN site_region IS NULL AND user_region = 'United States' THEN 'us' 
            WHEN site_region IS NULL AND user_region = 'Singapore' THEN 'sg' 
            WHEN site_region IS NULL AND user_region = 'Malaysia' THEN 'my' 
            WHEN site_region IS NULL AND user_region = 'Hong Kong' THEN 'hc'
            WHEN site_region NOT IN ('au', 'uk', 'za', 'nz', 'us', 'sg', 'my', 'hc') THEN 'no_site'
        ELSE 'no_site' END AS case_site_region_recode  
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