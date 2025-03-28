SELECT
    time,
    open,
    high,
    low,
    close,
    volume,
    symbol
FROM {{ source('raw', 'price') }}