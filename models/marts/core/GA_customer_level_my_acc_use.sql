-- TRACKER Session level -  
-- with My Account use (and flag for which sections viewed) - % of sessions and conversion rate and AOV

-- NOT IN WORKING ORDER ATM 21/02/22 FH

with myacc_summary
as
(
    select 
        
        --h.local_date
        h.fullVisitorId        
        ,session_id
        ,max(CONTAINS_SUBSTR(pagePath, 'account/perks')) as myacc_perks
        ,max(CONTAINS_SUBSTR(pagePath, 'account/orders')) as myacc_orders
        ,max(CONTAINS_SUBSTR(pagePath, 'account/details')) as myacc_details
        ,max(CONTAINS_SUBSTR(pagePath, 'account/address')) as myacc_address
        ,max(CONTAINS_SUBSTR(pagePath, '/wishlist')) as myacc_wishlist
        ,(IFNULL(transactionRevenue_AUD, 0) - (IFNULL(transactionShipping_AUD, 0) + IFNULL(transactionTax_AUD, 0))) as transactionRevenue_AUD
    
    from {{ ref('GA_hits_rollup')}} h
        left join {{ ref('GA_trans_rollup')}}
        using (session_id)
    where h.dataSource = 'web' and h.local_date >= date_sub(current_date(), INTERVAL 30 DAY)
    group by 1,2,8
)

select 
    
    --fullVisitorId
    myacc_perks 
    ,myacc_orders 
    ,myacc_details 
    ,myacc_address 
    ,myacc_wishlist 
    ,count(distinct session_id) as sessions
    ,count(distinct fullVisitorId) as unique_users
    ,sum(if(transactionRevenue_AUD >0, 1, 0)) as transactions
    ,sum(transactionRevenue_AUD) as total_sales
    
from myacc_summary

group by 1,2,3,4,5 
order by 1 desc  



