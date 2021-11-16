SELECT
    session_id
    ,aest_datetime
    ,site_region
    ,hostname
    ,pagepath
    ,fullURL
    ,datasource
    ,type
    ,COUNT(*) AS pageviews
    ,COUNT(DISTINCT fullURL) OVER(PARTITION BY session_id) AS unique_pageviews_within_session
    ,1 as unique_pageviews
  FROM (
        SELECT * 
        FROM {{ ref('GA_hits_rollup') }}
  )
  GROUP BY
    session_id, site_region, hostname, pagePath, fullURL, aest_datetime , datasource, type
  ORDER BY
    pageviews DESC