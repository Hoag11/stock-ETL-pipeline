CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.balance (
    period character varying,
    quarter integer,
    year integer,
    short_asset numeric,
    cash numeric,
    short_invest numeric,
    short_receivable numeric,
    inventory numeric,
    long_asset numeric,
    fixed_asset numeric,
    asset numeric,
    debt numeric,
    short_debt numeric,
    long_debt numeric,
    equity numeric,
    capital numeric,
    other_debt numeric,
    un_distributed_income numeric,
    minor_share_holder_profit numeric,
    payable numeric,
    symbol character varying
);

CREATE TABLE IF NOT EXISTS raw.cash (
    period character varying,
    quarter integer,
    year integer,
    invest_cost numeric,
    from_invest numeric,
    from_financial numeric,
    from_sale numeric,
    free_cash_flow numeric,
    symbol character varying
);

CREATE TABLE IF NOT EXISTS raw.income (
    period character varying,
    quarter integer,
    year integer,
    revenue numeric,
    year_revenue_growth numeric,
    quarter_revenue_growth numeric,
    cost_of_good_sold numeric,
    gross_profit numeric,
    operation_expense numeric,
    operation_profit numeric,
    year_operation_profit_growth numeric,
    quarter_operation_profit_growth numeric,
    interest_expense numeric,
    pre_tax_profit numeric,
    post_tax_profit numeric,
    share_holder_income numeric,
    year_share_holder_income_growth numeric,
    quarter_share_holder_income_growth numeric,
    ebitda numeric,
    symbol character varying
);

CREATE TABLE IF NOT EXISTS raw.news (
    rsi numeric,
    rs numeric,
    price numeric,
    price_change numeric,
    price_change_ratio numeric,
    price_change_ratio_1m numeric,
    id numeric,
    title character varying,
    source character varying,
    publish_date date,
    symbol character varying
);

CREATE TABLE IF NOT EXISTS raw.price (
    time date,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume numeric,
    symbol character varying
);

CREATE TABLE IF NOT EXISTS raw.profile (
    company_name character varying,
    company_profile text,
    history_dev text,
    company_promise text,
    business_risk text,
    key_developments text,
    business_strategies text,
    symbol character varying
);