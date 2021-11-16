SELECT
    CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) AS session_id
    ,TIMESTAMP_SECONDS(visitStartTime) as aest_datetime
    ,SPLIT(hits.page.pagePath, '/')[SAFE_OFFSET(1)] as site_region 
    ,MAX((totals.totalTransactionRevenue / 1000000)) as transactionRevenue_AUD
    ,MAX((cast(hits.transaction.transactionTax as INT64) / 1000000)) as transactionTax_AUD
    ,MAX((cast(hits.transaction.transactionShipping as INT64) / 1000000)) as transactionShipping_AUD
    ,MAX((cast(hits.transaction.localTransactionRevenue as INT64) / 1000000)) as transactionRevenue_local
    ,MAX((cast(hits.transaction.localTransactionTax as INT64) / 1000000)) as transactionTax_local
    ,MAX((cast(hits.transaction.localTransactionShipping as INT64) / 1000000)) as transactionShipping_local
FROM {{ source('132581016', 'ga_sessions_intraday_*')}}, UNNEST (hits) AS hits
WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 750 DAY)) 
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) 
    AND hits.transaction.transactionId IS NOT NULL
    AND SPLIT(hits.page.pagePath, '/')[SAFE_OFFSET(1)] IN ('au','nz','za','us','my','sg','hc','uk')
GROUP BY 1,2,3    