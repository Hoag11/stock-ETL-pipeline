SELECT
    rsi,
    rs,
    price,
    price_change,
    price_change_ratio,
    price_change_ratio_1m,
    id,
    title,
    source,
    publish_date,
    symbol
FROM {{ source('raw', 'news') }}