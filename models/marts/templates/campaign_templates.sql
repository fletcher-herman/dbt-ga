SELECT
     h.aest_datetime
    ,EXTRACT(YEAR FROM h.aest_datetime) as year_flag
    ,EXTRACT(DATE FROM h.aest_datetime) as date
    ,h.site_region
    ,channelGrouping
    ,h.session_id
    ,MAX((transactionRevenue_AUD) - (transactionTax_AUD + transactionShipping_AUD)) as revenue_AUD
FROM {{ ref('GA_hits_rollup') }} h
    LEFT JOIN {{ ref('GA_trans_rollup')}} 
    USING (session_id)
GROUP BY 1,2,3,4,5,6
HAVING site_region IN ('au','nz','za','us','my','sg','hc')

UNION DISTINCT 

SELECT
     h.aest_datetime
    ,EXTRACT(YEAR FROM h.aest_datetime) as year_flag
    ,EXTRACT(DATE FROM h.aest_datetime) as date
    ,h.site_region
    ,channelGrouping
    ,h.session_id
    ,MAX((transactionRevenue_AUD) - (transactionTax_AUD + transactionShipping_AUD)) as revenue_AUD
FROM {{ ref('GA_hits_intraday_rollup') }} h
    LEFT JOIN {{ ref('GA_trans_intraday_rollup')}} 
    USING (session_id)
GROUP BY 1,2,3,4,5,6
HAVING site_region IN ('au','nz','za','us','my','sg','hc')