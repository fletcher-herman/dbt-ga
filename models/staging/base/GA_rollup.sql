SELECT
    PARSE_DATE ("%Y%m%d", date) As date
    ,SPLIT(hits.page.pagePath, '/')[SAFE_OFFSET(1)] as offset_1 
    ,CONCAT(fullVisitorId, CAST(visitId AS STRING)) AS unique_visit_id 
    ,CONCAT(hits.page.hostname, hits.page.pagePath) as fullURL
    ,hits.page.hostname
    ,hits.page.pagePath
    ,hits.hitNumber AS hit_number
    ,CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) AS session_id
    ,(totals.totalTransactionRevenue * 0.906 / 1000000) AS revenue # 0.906 accounts for Aus Tax, 1000000 accounts for GA to BQ export
    ,hits.type
    ,SUM(totals.bounces) OVER(PARTITION BY CONCAT(fullVisitorId, CAST(visitId AS STRING))) AS total_no_of_bounces 
    ,MAX(hits.hitNumber) OVER(PARTITION BY CONCAT(fullVisitorId, CAST(visitId AS STRING))) AS max_hit 
FROM `cog-ga-365-big-query.132581016.ga_sessions_*`, UNNEST (hits) AS hits
WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 62 DAY)) 
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND hits.type = 'PAGE' AND hits.dataSource = 'web'