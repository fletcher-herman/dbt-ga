-- TRACKER Session level -  
-- with My Account use (and flag for which sections viewed) - % of sessions and conversion rate and AOV

with myacc_summary
as
(
    select 
        
        h.local_date
        ,session_id
        ,MAX(CONTAINS_SUBSTR(pagePath, 'account/perks')) as myacc_perks
        ,MAX(CONTAINS_SUBSTR(pagePath, 'account/orders')) as myacc_orders
        ,MAX(CONTAINS_SUBSTR(pagePath, 'account/details')) as myacc_details
        ,MAX(CONTAINS_SUBSTR(pagePath, 'account/address')) as myacc_address
        ,MAX(CONTAINS_SUBSTR(pagePath, '/wishlist')) as myacc_wishlist
        ,(IFNULL(transactionRevenue_AUD, 0) - (IFNULL(transactionShipping_AUD, 0) + IFNULL(transactionTax_AUD, 0))) as transactionRevenue_AUD
    
    from {{ ref('GA_hits_rollup')}} h
        left join {{ ref('GA_trans_rollup')}}
        using (session_id)
    where h.dataSource = 'web'
    group by 1,2,8
)

select 
    
    local_date
    ,myacc_perks 
    ,myacc_orders 
    ,myacc_details 
    ,myacc_address 
    ,myacc_wishlist 
    ,count(*) as sessions
    ,SUM(IF(transactionRevenue_AUD >0, 1, 0)) as transactions
    ,AVG(transactionRevenue_AUD) as AOV
    
from myacc_summary
group by 1,2,3,4,5,6  
order by 1 desc  



