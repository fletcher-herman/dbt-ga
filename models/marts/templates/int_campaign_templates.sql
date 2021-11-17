SELECT
     h.aest_datetime
    ,EXTRACT(YEAR FROM h.aest_datetime) as year_flag
    ,EXTRACT(DATE FROM h.aest_datetime) as date
    ,h.site_region
    ,h.user_region
    ,h.dataSource
    ,h.channelGrouping
    ,h.session_id
    ,(transactionRevenue_AUD) - (transactionTax_AUD + transactionShipping_AUD) as revenue_AUD
FROM {{ ref('GA_hits_rollup') }} h
    LEFT JOIN {{ ref('GA_trans_rollup')}} 
    USING (unique_visit_id)
WHERE h.hit_number = 1    
--WHERE h.site_region IN ('au','nz','za','us','my','sg','hc','uk')