SELECT
    symbol as company_id
    ,period
    ,quarter
    ,year
    ,invest_cost
    ,from_invest
    ,from_financial
    ,from_sale
    ,free_cash_flow
    ,CASE
        WHEN period LIKE '%Q1' THEN (LEFT(period, 4) || '-03-31')::DATE
        WHEN period LIKE '%Q2' THEN (LEFT(period, 4) || '-06-30')::DATE
        WHEN period LIKE '%Q3' THEN (LEFT(period, 4) || '-09-30')::DATE
        WHEN period LIKE '%Q4' THEN (LEFT(period, 4) || '-12-31')::DATE
    END AS report_date
FROM {{ source('raw', 'cash') }}