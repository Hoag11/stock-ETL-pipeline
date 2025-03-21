WITH price_data AS (
    SELECT
        p.company_id,
        TO_CHAR(p.date, 'YYYYMMDD')::INTEGER AS date_id,
        p.open,
        p.high,
        p.low,
        p.close,
        p.volume
    FROM {{ ref('dim_price') }} p
),
news_data AS (
    SELECT
        n.company_id,
        TO_CHAR(n.publish_date::DATE, 'YYYYMMDD')::INTEGER AS date_id,
        COUNT(*) AS news_count
    FROM {{ ref('dim_news') }} n
    GROUP BY n.company_id, TO_CHAR(n.publish_date::DATE, 'YYYYMMDD')::INTEGER
),
financial_data AS (
    SELECT
        i.report_date,
        TO_CHAR(i.report_date, 'YYYYMMDD')::INTEGER AS date_id,
        i.revenue,
        i.operation_profit,
        i.post_tax_profit,
        i.share_holder_income,
        i.ebitda,
        b.short_asset,
        b.long_asset,
        b.asset,
        b.debt,
        b.equity,
        b.un_distributed_income,
        c.invest_cost,
        c.from_invest,
        c.from_financial,
        c.from_sale,
        c.free_cash_flow
    FROM {{ ref('dim_income') }} i
    LEFT JOIN {{ ref('dim_balance') }} b ON i.period = b.period
    LEFT JOIN {{ ref('dim_cash') }} c ON i.period = c.period
),
all_dates AS (
    SELECT DISTINCT date_id
    FROM (
        SELECT date_id FROM price_data
        UNION
        SELECT date_id FROM news_data
        UNION
        SELECT date_id FROM financial_data
    ) combined
),
all_combinations AS (
    SELECT
        c.company_id,
        d.date_id
    FROM all_dates d
    CROSS JOIN {{ ref('dim_profile') }} c
)
SELECT
    ROW_NUMBER() OVER () AS symbol_id,
    ac.company_id,
    ac.date_id,
    p.open,
    p.high,
    p.low,
    p.close,
    p.volume,
    n.news_count,
    f.revenue,
    f.operation_profit,
    f.post_tax_profit,
    f.share_holder_income,
    f.ebitda,
    f.short_asset,
    f.long_asset,
    f.asset,
    f.debt,
    f.equity,
    f.un_distributed_income,
    f.invest_cost,
    f.from_invest,
    f.from_financial,
    f.from_sale,
    f.free_cash_flow
FROM all_combinations ac
LEFT JOIN price_data p ON ac.company_id = p.company_id AND ac.date_id = p.date_id
LEFT JOIN news_data n ON ac.company_id = n.company_id AND ac.date_id = n.date_id
LEFT JOIN financial_data f ON ac.date_id = f.date_id
JOIN {{ ref('dim_profile') }} c ON ac.company_id = c.company_id
JOIN {{ ref('dim_date') }} dt ON ac.date_id = dt.date_id