SELECT
    company_name,
    company_profile,
    history_dev,
    company_promise,
    business_risk,
    key_developments,
    business_strategies,
    symbol
FROM {{ source('raw', 'profile') }}