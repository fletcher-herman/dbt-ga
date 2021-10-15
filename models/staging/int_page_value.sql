SELECT 
  date
  ,fullURL
  ,SUM(revenue) / SUM(unique_pageviews) AS PageValue
  ,SUM(revenue) AS Revenue
  ,SUM(unique_pageviews) AS Unique_Pageviews
FROM 
  {{ ref('stg_sessions') }}
WHERE 
    fullURL IN (select fullURL from {{ ref('stg_top_landing_pages')}})
GROUP BY 
1, 2
ORDER BY Date ASC, Revenue DESC