-- Product Category dimension lookup 

-- note: very convoluted and with PLP change (late 2021) incluce more issue.

with unioned as 
    (
    SELECT
        SPLIT(hits.page.pagePath, '/')[SAFE_OFFSET(1)] as site_region 
        ,prods.productSKU
        ,prods.v2ProductName
        ,prods.v2ProductCategory 
        ,prods.productVariant
        ,MIN(TIMESTAMP_SECONDS(visitStartTime)) as start_timestamp
        ,MAX(TIMESTAMP_SECONDS(visitStartTime)) as end_timestamp
    FROM {{ source('132581016', 'ga_sessions_*')}}, 
        UNNEST (hits) AS hits,
        UNNEST (hits.product) as prods
    WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 750 DAY)) 
        AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) 
    GROUP BY 1,2,3,4,5

    UNION DISTINCT

    SELECT
        SPLIT(hits.page.pagePath, '/')[SAFE_OFFSET(1)] as site_region 
        ,prods.productSKU
        ,prods.v2ProductName
        ,prods.v2ProductCategory 
        ,prods.productVariant
        ,MIN(TIMESTAMP_SECONDS(visitStartTime)) as start_timestamp
        ,MAX(TIMESTAMP_SECONDS(visitStartTime)) as end_timestamp
    FROM {{ source('132581016', 'ga_sessions_intraday_*')}}, 
        UNNEST (hits) AS hits,
        UNNEST (hits.product) as prods
    WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 750 DAY)) 
        AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) 
    GROUP BY 1,2,3,4,5
    ORDER BY 2
    )

select * from unioned    

