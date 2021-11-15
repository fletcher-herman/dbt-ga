SELECT 
    ROW_NUMBER() 
        OVER (PARTITION BY region ORDER BY landing_count desc) as landing_rank
    ,aa.*
FROM (
    SELECT 
    IF(offset_1 IN ('au','za','us','nz','my','sg', 'uk','hc','on', 'br','th'), offset_1, 'xxx') as region
    ,fullURL
    ,SUM(IF(hit_number = 1, 1, 0)) as landing_count
    FROM {{ ref('GA_hits_rollup') }}        
    GROUP BY region, fullURL
    HAVING landing_count >= 500
) aa
WHERE region != 'xxx'
ORDER BY region, landing_rank
