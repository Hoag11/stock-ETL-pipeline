{{ config(
    materialized='table',
    indexes=[
        {'columns': ['company_id', 'date_id'], 'type': 'btree'},
        {'columns': ['financial_id'], 'type': 'btree'}
    ]
) }}

WITH price_data AS (
    SELECT
        company_id,
        date,
        close AS price,
        volume,
        open,
        high,
        low
    FROM {{ ref('dim_price') }}
),
financial_data AS (
    SELECT
        company_id,
        period,
        (company_id || '_' || period) AS financial_id
    FROM {{ ref('dim_income') }}
)
SELECT
    p.company_id,
    to_char(p.date, 'YYYYMMDD')::int AS date_id,
    COALESCE(f.financial_id, 'UNKNOWN') AS financial_id, 
    p.price,
    p.volume,
    p.open,
    p.high,
    p.low
FROM price_data p
LEFT JOIN financial_data f
    ON p.company_id = f.company_id
    AND f.period = (
        SELECT MAX(period)
        FROM {{ ref('dim_income') }}
        WHERE company_id = p.company_id
        AND period <= to_char(p.date, 'YYYY-Q')
    )