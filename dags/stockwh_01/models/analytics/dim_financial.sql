{{ config(
    unique_key='financial_id'
) }}

WITH income AS (
    SELECT
        company_id,
        period,
        quarter,
        year,
        revenue,
        year_revenue_growth,
        quarter_revenue_growth,
        cost_of_good_sold,
        gross_profit,
        operation_expense,
        operation_profit,
        year_operation_profit_growth,
        quarter_operation_profit_growth,
        interest_expense,
        pre_tax_profit,
        post_tax_profit,
        share_holder_income,
        year_share_holder_income_growth,
        quarter_share_holder_income_growth,
        ebitda
    FROM {{ ref('dim_income') }}
),
balance AS (
    SELECT
        company_id,
        period,
        short_asset,
        cash,
        short_invest,
        short_receivable,
        inventory,
        long_asset,
        fixed_asset,
        asset,
        debt,
        short_debt,
        long_debt,
        equity,
        capital,
        other_debt,
        un_distributed_income,
        minor_share_holder_profit,
        payable
    FROM {{ ref('dim_balance') }}
),
cash AS (
    SELECT
        company_id,
        period,
        invest_cost,
        from_invest,
        from_financial,
        from_sale,
        free_cash_flow
    FROM {{ ref('dim_cash') }}
)
SELECT
    -- Tạo financial_id duy nhất bằng cách kết hợp symbol và period
    (i.company_id || '_' || i.period) AS financial_id,
    i.company_id,
    i.period,
    i.quarter,
    i.year,
    i.revenue,
    i.year_revenue_growth,
    i.quarter_revenue_growth,
    i.cost_of_good_sold,
    i.gross_profit,
    i.operation_expense,
    i.operation_profit,
    i.year_operation_profit_growth,
    i.quarter_operation_profit_growth,
    i.interest_expense,
    i.pre_tax_profit,
    i.post_tax_profit,
    i.share_holder_income,
    i.year_share_holder_income_growth,
    i.quarter_share_holder_income_growth,
    i.ebitda,
    b.short_asset,
    b.cash,
    b.short_invest,
    b.short_receivable,
    b.inventory,
    b.long_asset,
    b.fixed_asset,
    b.asset,
    b.debt,
    b.short_debt,
    b.long_debt,
    b.equity,
    b.capital,
    b.other_debt,
    b.un_distributed_income,
    b.minor_share_holder_profit,
    b.payable,
    c.invest_cost,
    c.from_invest,
    c.from_financial,
    c.from_sale,
    c.free_cash_flow
FROM income i
LEFT JOIN balance b ON i.company_id = b.company_id AND i.period = b.period
LEFT JOIN cash c ON i.company_id = c.company_id AND i.period = c.period