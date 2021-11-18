WITH disc_sessions
AS
    (
SELECT 
    session_id
    ,aest_datetime
    ,user_region
    ,channelGrouping
    ,dataSource
    ,ROW_NUMBER() OVER 
        (PARTITION BY session_id ORDER BY aest_datetime asc) as rn
FROM
    `cog-ga-365-big-query`.`dbt_fherman`.`GA_hits_rollup`  
    )

SELECT 
    ds.session_id
    ,ds.aest_datetime
    ,ds.channelGrouping
    ,ds.user_region
    ,site_region_recode
    ,transactionRevenue_AUD
    ,transactionTax_AUD
    ,transactionShipping_AUD
FROM
    disc_sessions ds
LEFT JOIN `cog-ga-365-big-query`.`dbt_fherman`.`int_session_level_store_attr`
    USING(session_id)
LEFT JOIN `cog-ga-365-big-query`.`dbt_fherman`.`GA_trans_rollup`
    USING(session_id)
WHERE rn = 1