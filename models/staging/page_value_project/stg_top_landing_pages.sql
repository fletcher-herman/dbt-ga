SELECT 
    ROW_NUMBER() 
        OVER (PARTITION BY site_region ORDER BY landing_count desc) as landing_rank
    ,aa.*
FROM (
    SELECT 
    IF(site_region IN ('au','za','us','nz','my','sg', 'uk','hc','on', 'br','th'), site_region, 'xxx') as site_region
    ,fullURL
    ,SUM(IF(hit_number = 1, 1, 0)) as landing_count
    FROM {{ ref('GA_hits_rollup') }}        
    GROUP BY site_region, fullURL
    HAVING landing_count >= 500
) aa
WHERE site_region != 'xxx'
ORDER BY site_region, landing_rank
