SELECT 
    symbol as company_id
    ,company_name
    ,company_profile
    ,history_dev
    ,company_promise
    ,business_risk
    ,key_developments
    ,business_strategies
FROM {{source('raw', 'profile')}}