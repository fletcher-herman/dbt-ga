select
    year_flag
    ,EXTRACT(date FROM
    case 
        when site_region='us' then DATETIME(aest_datetime, "America/Los_Angeles")        
        when site_region='hk' then DATETIME(aest_datetime, "HongKong") 
        when site_region='sg' then DATETIME(aest_datetime, "Asia/Singapore") 
        when site_region='my' then DATETIME(aest_datetime, "Asia/Kuala_Lumpur") 
        when site_region='au' then DATETIME(aest_datetime, "Australia/Sydney") 
        when site_region='nz' then DATETIME(aest_datetime, "Pacific/Auckland")  
        when site_region='uk' then DATETIME(aest_datetime, "Europe/London")  
        when site_region='za' then DATETIME(aest_datetime, "Africa/Johannesburg")                                   
        else null end) as date
    ,channelGrouping
    ,count((distinct session_id)) as disc_sessions
    ,SUM(IF(revenue_AUD is not null,1,0)) as transactions
    ,SUM(IF(revenue_AUD is not null,1,0)) / count((session_id)) as conversion_rate
    ,SUM(revenue_AUD) as revenue
    ,SUM(revenue_AUD) / SUM(IF(revenue_AUD is not null,1,0)) as AOV
from {{ ref('int_campaign_templates')}} 
group by 1,2,3 
order by 1 desc