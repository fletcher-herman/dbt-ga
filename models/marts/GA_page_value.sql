SELECT 
  CAST(aest_datetime as date) as date
  ,fullURL
  ,SUM(transactionRevenue_AUD) / SUM(unique_pageviews) AS PageValue
  ,SUM(transactionRevenue_AUD) AS revenue_AUD
  ,SUM(unique_pageviews) AS Unique_Pageviews
FROM 
  {{ ref('stg_sessions') }}
WHERE 
    fullURL IN (select fullURL from {{ ref('stg_top_landing_pages')}})
GROUP BY 
1, 2
ORDER BY Date ASC, revenue_AUD DESC
