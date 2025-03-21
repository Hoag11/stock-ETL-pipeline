SELECT
    symbol as company_id
    ,id
    ,title
    ,source
    ,publish_date 
FROM {{ source('raw', 'news') }}