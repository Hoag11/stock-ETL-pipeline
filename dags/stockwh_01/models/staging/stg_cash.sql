SELECT
    period,
    quarter,
    year,
    invest_cost,
    from_invest,
    from_financial,
    from_sale,
    free_cash_flow,
    symbol
FROM {{ source('raw', 'cash') }}