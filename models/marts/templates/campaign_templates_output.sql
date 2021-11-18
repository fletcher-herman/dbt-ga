WITH rev_cte
AS
    (
        SELECT
            EXTRACT(date FROM aest_datetime) as date
            ,user_region
            ,dataSource
            ,channelGrouping
            ,SUM(transactionRevenue_AUD) as transactionRevenue_AUD
            ,SUM(transactionTax_AUD) as transactionTax_AUD
            ,SUM(transactionShipping_AUD) as transactionShipping_AUD
        FROM
            {{ ref('GA_trans_rollup')}}    
    )

SELECT 
    EXTRACT(YEAR FROM h.aest_datetime) AS year_flag
    ,EXTRACT(DATE FROM aest_datetime) as date
    ,h.dataSource
    ,h.channelGrouping
    ,sls.site_region_recode
    ,h.user_region
    ,COUNT(distinct h.session_id) AS sessions
    ,COUNT(distinct t.session_id) AS transactions
    ,SUM(IF(t.session_id IS NOT NULL,1,0)) / COUNT((h.session_id)) AS conversion_rate
    
    --,SUM((transactionRevenue_AUD) - (transactionTax_AUD + transactionShipping_AUD)) AS revenue_without_tax_shipping
    --,SUM(transactionRevenue_AUD) / SUM(IF(transactionRevenue_AUD is not null,1,0)) AS AOV
FROM {{ ref('GA_hits_rollup')}} h
LEFT JOIN {{ ref('GA_trans_rollup')}} t
    USING (session_id)
LEFT JOIN {{ ref('int_session_level_store_attr')}} sls
    USING (session_id)
--LEFT JOIN rev_cte
 --   ON              
GROUP BY 1,2,3,4,5,6



   -- CASE 
    --    WHEN sls.site_region_recode ='us' THEN DATETIME(h.aest_datetime, "America/Los_Angeles")        
    --    WHEN sls.site_region_recode ='hk' THEN DATETIME(h.aest_datetime, "HongKong") 
    --    WHEN sls.site_region_recode ='sg' THEN DATETIME(h.aest_datetime, "Asia/Singapore") 
    --    WHEN sls.site_region_recode ='my' THEN DATETIME(h.aest_datetime, "Asia/Kuala_Lumpur") 
    --    WHEN sls.site_region_recode ='au' THEN DATETIME(h.aest_datetime, "Australia/Sydney") 
    --    WHEN sls.site_region_recode ='nz' THEN DATETIME(h.aest_datetime, "Pacific/Auckland")  
    --    WHEN sls.site_region_recode ='uk' THEN DATETIME(h.aest_datetime, "Europe/London")  
    --    WHEN sls.site_region_recode ='za' THEN DATETIME(h.aest_datetime, "Africa/Johannesburg")                                   
     --   ELSE null END) AS date