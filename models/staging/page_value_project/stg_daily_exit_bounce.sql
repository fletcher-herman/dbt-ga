SELECT 
    ROW_NUMBER () OVER (PARTITION BY fullURL ORDER BY date desc) as date_desc
    ,date
    ,IF(site_region IN ('au','za','us','nz','my','sg', 'uk','hc','on', 'br','th'), site_region, 'xxx') as site_region
    ,fullURL
    ,COUNT(hit_number) AS hit_number
    ,SUM(IF(hit_number = 1, 1, 0)) as landing_count
    ,SUM(IF(hit_number = max_hit, 1, 0)) as exit_count
    ,ROUND(SUM(IF(hit_number = max_hit, 1, 0)) / COUNT(hit_number), 4) * 100 AS exit_rate
    ,IFNULL(SUM(total_no_of_bounces), 0) as total_no_of_bounces
    ,ROUND(IFNULL(SUM(total_no_of_bounces), 0) / COUNT(hit_number), 4) * 100 AS bounce_rate   
  FROM ( 
      SELECT 
          CAST(aest_datetime as date) as date
          ,site_region 
          ,unique_visit_id 
          ,fullURL
          ,hit_number
          ,total_no_of_bounces 
          ,max_hit
      FROM {{ ref('GA_hits_rollup') }}
      ) aa
GROUP BY date, site_region, fullURL
HAVING site_region !='xxx' --AND hit_number >= 100
ORDER BY fullURL, date desc