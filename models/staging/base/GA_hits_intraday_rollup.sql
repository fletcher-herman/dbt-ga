SELECT
    TIMESTAMP_SECONDS(visitStartTime) as aest_datetime
    ,SPLIT(hits.page.pagePath, '/')[SAFE_OFFSET(1)] as site_region 
    ,CONCAT(fullVisitorId, CAST(visitId AS STRING)) AS unique_visit_id 
    ,CONCAT(hits.page.hostname, hits.page.pagePath) as fullURL
    ,channelGrouping
    ,hits.page.hostname
    ,hits.page.pagePath
    ,hits.hitNumber AS hit_number
    ,CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) AS session_id
    ,(totals.totalTransactionRevenue / 1000000) AS revenue_AUD
    ,hits.dataSource
    ,hits.type
    ,SUM(totals.bounces) OVER(PARTITION BY CONCAT(fullVisitorId, CAST(visitId AS STRING))) AS total_no_of_bounces 
    ,MAX(hits.hitNumber) OVER(PARTITION BY CONCAT(fullVisitorId, CAST(visitId AS STRING))) AS max_hit 
FROM {{ source('132581016', 'ga_sessions_intraday_*')}}, UNNEST (hits) AS hits
--WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 750 DAY)) 
--    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))