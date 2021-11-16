SELECT
    TIMESTAMP_SECONDS(visitStartTime) as aest_datetime
    ,CONCAT(fullVisitorId, CAST(visitId AS STRING)) AS unique_visit_id 
    ,MAX((totals.totalTransactionRevenue / 1000000)) as transactionRevenue_AUD
    ,MAX((cast(hits.transaction.transactionTax as INT64) / 1000000)) as transactionTax_AUD
    ,MAX((cast(hits.transaction.transactionShipping as INT64) / 1000000)) as transactionShipping_AUD
FROM {{ source('132581016', 'ga_sessions_*')}}, UNNEST (hits) AS hits
WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 750 DAY)) 
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) 
    AND hits.transaction.transactionId IS NOT NULL
GROUP BY 1,2    