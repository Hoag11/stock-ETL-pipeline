SELECT
    symbol as company_id
    ,period
    ,quarter
    ,year
    ,revenue
    ,year_revenue_growth
    ,quarter_revenue_growth
    ,cost_of_good_sold
    ,gross_profit
    ,operation_expense
    ,operation_profit
    ,year_operation_profit_growth
    ,quarter_operation_profit_growth
    ,interest_expense
    ,pre_tax_profit
    ,post_tax_profit
    ,share_holder_income
    ,year_share_holder_income_growth
    ,quarter_share_holder_income_growth
    ,ebitda
    ,CASE
        WHEN period LIKE '%Q1' THEN (LEFT(period, 4) || '-03-31')::DATE
        WHEN period LIKE '%Q2' THEN (LEFT(period, 4) || '-06-30')::DATE
        WHEN period LIKE '%Q3' THEN (LEFT(period, 4) || '-09-30')::DATE
        WHEN period LIKE '%Q4' THEN (LEFT(period, 4) || '-12-31')::DATE
    END AS report_date
FROM {{ source('raw', 'income') }}