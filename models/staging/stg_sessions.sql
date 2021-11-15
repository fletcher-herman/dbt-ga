SELECT
    session_id
    ,date
    ,hostname
    ,pagepath
    ,fullURL
    ,revenue
    ,COUNT(*) AS pageviews
    ,COUNT(DISTINCT fullURL) OVER(PARTITION BY session_id) AS unique_pageviews_within_session
    ,1 as unique_pageviews
  FROM (
        SELECT * 
        FROM {{ ref('GA_hits_rollup') }}
        WHERE type = "PAGE" 
  )
  GROUP BY
    session_id, hostname, pagePath, revenue, fullURL, date 
  ORDER BY
    pageviews DESC