WITH dates AS (
    SELECT DISTINCT time AS date
    FROM {{ source('raw', 'price') }}
    UNION
    SELECT DISTINCT publish_date::DATE AS date
    FROM {{ source('raw', 'news') }}
    UNION
    SELECT DISTINCT CASE
        WHEN period LIKE '%Q1' THEN (LEFT(period, 4) || '-03-31')::DATE
        WHEN period LIKE '%Q2' THEN (LEFT(period, 4) || '-06-30')::DATE
        WHEN period LIKE '%Q3' THEN (LEFT(period, 4) || '-09-30')::DATE
        WHEN period LIKE '%Q4' THEN (LEFT(period, 4) || '-12-31')::DATE
    END AS date
    FROM {{ source('raw', 'income') }}
)
SELECT
    TO_CHAR(date, 'YYYYMMDD')::INTEGER AS date_id,
    date,
    EXTRACT(DOW FROM date) AS day_of_week,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(QUARTER FROM date) AS quarter,
    EXTRACT(YEAR FROM date) AS year
FROM dates
WHERE date IS NOT NULL