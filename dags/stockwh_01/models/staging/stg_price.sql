SELECT
    time,
    open * 1000 AS open,
    high * 1000 AS high,
    low * 1000 AS low,
    close * 1000 AS close,
    volume,
    symbol
FROM {{ source('raw', 'price') }}