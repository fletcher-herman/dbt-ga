  SELECT
    date
    ,hostname
    ,pagePath
    ,fullURL
    ,session_id
    ,revenue
    ,type
  FROM
    {{ ref('GA_rollup') }} 