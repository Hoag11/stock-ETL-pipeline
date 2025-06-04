SELECT
    period,
    quarter,
    year,
    invest_cost * 1000000000 AS invest_cost,
    from_invest * 1000000000 AS from_invest,
    from_financial * 1000000000 AS from_financial,
    from_sale * 1000000000 AS from_sale,
    free_cash_flow * 1000000000 AS free_cash_flow,
    symbol
FROM {{ source('raw', 'cash') }}