SELECT
    period,
    quarter,
    year,
    short_asset * 1000000000 AS short_asset,
    cash * 1000000000 AS cash,
    short_invest * 1000000000 AS short_invest,
    short_receivable * 1000000000 AS short_receivable,
    inventory * 1000000000 AS inventory,
    long_asset * 1000000000 AS long_asset,
    fixed_asset * 1000000000 AS fixed_asset,
    asset * 1000000000 AS asset,
    debt * 1000000000 AS debt,
    short_debt * 1000000000 AS short_debt,
    long_debt * 1000000000 AS long_debt,
    equity * 1000000000 AS equity,
    capital * 1000000000 AS capital,
    other_debt * 1000000000 AS other_debt,
    un_distributed_income * 1000000000 AS un_distributed_income,
    minor_share_holder_profit * 1000000000 AS minor_share_holder_profit,
    payable,
    symbol
FROM {{ source('raw', 'balance') }}