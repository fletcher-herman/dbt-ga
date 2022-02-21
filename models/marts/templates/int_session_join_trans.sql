-- some peak (2021) stats for Brendan were at channel level

WITH disc_sessions
AS
    (
SELECT 
    session_id
    ,local_date
    ,aest_datetime
    ,user_region
    ,channelGrouping
    ,dataSource
    ,ROW_NUMBER() OVER 
        (PARTITION BY session_id ORDER BY aest_datetime asc) as rn
FROM
    {{ ref('GA_hits_rollup')}}  
    )

SELECT 
    ds.session_id
    ,ds.local_date
    ,ds.aest_datetime
    ,ds.channelGrouping
    ,ds.dataSource
    ,ds.user_region
    ,site_region_recode
    ,IFNULL(transactionRevenue_AUD, 0) as transactionRevenue_AUD
    ,IFNULL(transactionTax_AUD, 0) as transactionTax_AUD
    ,IFNULL(transactionShipping_AUD, 0) as transactionShipping_AUD
FROM
    disc_sessions ds
LEFT JOIN {{ ref('stg_session_level_store_attr')}}
    USING(session_id)
LEFT JOIN {{ ref('GA_trans_rollup')}}
    USING(session_id)
WHERE rn = 1