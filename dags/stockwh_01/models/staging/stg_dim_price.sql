SELECT
    symbol as company_id
    ,time as date
    ,open
    ,high
    ,low
    ,close
    ,volume
FROM {{ source('raw', 'price') }}