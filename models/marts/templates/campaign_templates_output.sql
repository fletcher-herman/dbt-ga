SELECT
    EXTRACT(YEAR from aest_datetime) as year_flag
    ,aest_date
    ,dataSource
    ,channelGrouping
    ,site_region_recode as site_region
    ,user_region
    ,COUNT(distinct session_id) as sessions
    ,SUM(IF(transactionRevenue_AUD IS NOT NULL, 1, 0)) as transactions
    ,SUM(transactionRevenue_AUD) as revenue_AUD
    ,SUM(transactionTax_AUD) as tax_AUD
    ,SUM(transactionShipping_AUD) as shipping_AUD
FROM {{ ref('int_session_join_trans')}}    
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