SELECT
    symbol as company_id
    ,period
    ,quarter
    ,year
    ,short_asset
    ,long_asset
    ,fixed_asset
    ,asset
    ,cash
    ,short_invest
    ,short_receivable
    ,inventory
    ,debt
    ,short_debt
    ,long_debt
    ,equity
    ,capital
    ,other_debt
    ,un_distributed_income
    ,minor_share_holder_profit
    ,payable
    ,CASE
        WHEN period LIKE '%Q1' THEN (LEFT(period, 4) || '-03-31')::DATE
        WHEN period LIKE '%Q2' THEN (LEFT(period, 4) || '-06-30')::DATE
        WHEN period LIKE '%Q3' THEN (LEFT(period, 4) || '-09-30')::DATE
        WHEN period LIKE '%Q4' THEN (LEFT(period, 4) || '-12-31')::DATE
    END AS report_date
from {{ source('raw', 'balance') }}