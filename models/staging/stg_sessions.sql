SELECT
    session_id
    ,aest_datetime
    ,site_region
    ,hostname
    ,pagepath
    ,fullURL
    ,datasource
    ,type
    ,transactionRevenue_AUD
    ,COUNT(*) AS pageviews
    ,COUNT(DISTINCT fullURL) OVER(PARTITION BY session_id) AS unique_pageviews_within_session
    ,1 as unique_pageviews
  FROM (
        SELECT h.*, transactionRevenue_AUD 
        FROM {{ ref('GA_hits_rollup') }} h
          LEFT JOIN {{ ref('GA_trans_rollup')}}
          USING (session_id)
  )
  GROUP BY
    session_id, site_region, hostname, pagePath, fullURL, aest_datetime , datasource, type, transactionRevenue_AUD
  ORDER BY
    pageviews DESC