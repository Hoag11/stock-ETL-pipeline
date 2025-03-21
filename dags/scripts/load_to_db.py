import pandas as pd
from sqlalchemy import create_engine
import os
from concurrent.futures import ThreadPoolExecutor
import scripts.config as config

# Định nghĩa cấu trúc mong muốn và ánh xạ cột
INCOME_COLUMNS = {
    'period': ['period', 'report_period', 'quarter_year'],
    'quarter': ['quarter', 'qtr'],
    'year': ['year', 'fiscal_year'],
    'revenue': ['revenue', 'total_revenue', 'sales'],
    'year_revenue_growth': ['year_revenue_growth', 'revenue_growth_yoy'],
    'quarter_revenue_growth': ['quarter_revenue_growth', 'revenue_growth_qoq'],
    'cost_of_good_sold': ['cost_of_good_sold', 'cogs'],
    'gross_profit': ['gross_profit', 'gross_margin'],
    'operation_expense': ['operation_expense', 'operating_expense'],
    'operation_profit': ['operation_profit', 'operating_profit'],
    'year_operation_profit_growth': ['year_operation_profit_growth', 'operation_profit_growth_yoy'],
    'quarter_operation_profit_growth': ['quarter_operation_profit_growth', 'operation_profit_growth_qoq'],
    'interest_expense': ['interest_expense', 'interest_cost'],
    'pre_tax_profit': ['pre_tax_profit', 'profit_before_tax'],
    'post_tax_profit': ['post_tax_profit', 'profit_after_tax'],
    'share_holder_income': ['share_holder_income', 'net_income_to_shareholders'],
    'year_share_holder_income_growth': ['year_share_holder_income_growth', 'share_holder_income_growth_yoy'],
    'quarter_share_holder_income_growth': ['quarter_share_holder_income_growth', 'share_holder_income_growth_qoq'],
    'ebitda': ['ebitda']
}

BALANCE_COLUMNS = {
    'period': ['period', 'report_period', 'quarter_year'],
    'quarter': ['quarter', 'qtr'],
    'year': ['year', 'fiscal_year'],
    'short_asset': ['short_asset', 'current_assets'],
    'cash': ['cash', 'cash_and_equivalents'],
    'short_invest': ['short_invest', 'short_term_investments'],
    'short_receivable': ['short_receivable', 'accounts_receivable'],
    'inventory': ['inventory', 'inventories'],
    'long_asset': ['long_asset', 'non_current_assets'],
    'fixed_asset': ['fixed_asset', 'fixed_assets'],
    'asset': ['asset', 'total_assets'],
    'debt': ['debt', 'total_liabilities'],
    'short_debt': ['short_debt', 'current_liabilities'],
    'long_debt': ['long_debt', 'non_current_liabilities'],
    'equity': ['equity', 'total_equity'],
    'capital': ['capital', 'share_capital'],
    'other_debt': ['other_debt', 'other_liabilities'],
    'un_distributed_income': ['un_distributed_income', 'retained_earnings'],
    'minor_share_holder_profit': ['minor_share_holder_profit', 'minority_interest'],
    'payable': ['payable', 'accounts_payable']
}

CASH_COLUMNS = {
    'period': ['period', 'report_period', 'quarter_year'],
    'quarter': ['quarter', 'qtr'],
    'year': ['year', 'fiscal_year'],
    'invest_cost': ['invest_cost', 'investment_cost'],
    'from_invest': ['from_invest', 'cash_from_investing'],
    'from_financial': ['from_financial', 'cash_from_financing'],
    'from_sale': ['from_sale', 'cash_from_operations', 'cash_from_sales'],
    'free_cash_flow': ['free_cash_flow', 'fcf']
}

def normalize_period(period):
    """Chuẩn hóa định dạng period (ví dụ: 'Q1-2024' -> '2024-Q1')"""
    if pd.isna(period):
        return None
    period = str(period).strip()
    if '-' in period:
        q, year = period.split('-')
        q = q.replace('Q', '').strip()
        return f"{year}-Q{q}"
    return period

def extract_quarter_year(df):
    """Trích xuất quarter và year từ period nếu không có cột riêng"""
    if 'quarter' not in df.columns or 'year' not in df.columns:
        df['quarter'] = df['period'].str.extract(r'Q(\d)')[0].astype(int)
        df['year'] = df['period'].str.extract(r'(\d{4})')[0].astype(int)
    return df

def normalize_columns(df, column_mapping):
    """Ánh xạ và chuẩn hóa cột từ file CSV sang cấu trúc mong muốn"""
    normalized_df = pd.DataFrame()
    for target_col, possible_names in column_mapping.items():
        for name in possible_names:
            if name in df.columns:
                normalized_df[target_col] = df[name]
                break
        if target_col not in normalized_df:
            normalized_df[target_col] = None  

    # Chuẩn hóa period
    if 'period' in normalized_df:
        normalized_df['period'] = normalized_df['period'].apply(normalize_period)

    # Trích xuất quarter và year nếu cần
    normalized_df = extract_quarter_year(normalized_df)

    return normalized_df

def load_news_for_symbol(symbol, base_path, engine):
    file_path = f'{base_path}/company_news/{symbol}_news.csv'
    if os.path.exists(file_path):
        df_news = pd.read_csv(file_path)
        df_news['symbol'] = symbol
        df_news.to_sql('news', engine, schema='raw', if_exists='append', index=False)
        print(f"Đã đẩy dữ liệu news cho {symbol}")
    else:
        print(f"File {file_path} không tồn tại, bỏ qua lưu news cho {symbol}.")

def load_profile_for_symbol(symbol, base_path, engine):
    file_path = f'{base_path}/company_info/{symbol}_profile.csv'
    if os.path.exists(file_path):
        df_profile = pd.read_csv(file_path)
        df_profile['symbol'] = symbol
        df_profile.to_sql('profile', engine, schema='raw', if_exists='append', index=False)
        print(f"Đã đẩy dữ liệu profile cho {symbol}")
    else:
        print(f"File {file_path} không tồn tại, bỏ qua lưu profile cho {symbol}.")

def load_income_for_symbol(symbol, base_path, engine):
    file_path = f'{base_path}/company_report/{symbol}_income.csv'
    if os.path.exists(file_path):
        df_income = pd.read_csv(file_path)
        # Chuẩn hóa cấu trúc
        df_income = normalize_columns(df_income, INCOME_COLUMNS)
        df_income['symbol'] = symbol
        df_income.to_sql('income', engine, schema='raw', if_exists='append', index=False)
        print(f"Đã đẩy dữ liệu báo cáo thu nhập cho {symbol}")
    else:
        print(f"File {file_path} không tồn tại, bỏ qua lưu income cho {symbol}.")

def load_balance_for_symbol(symbol, base_path, engine):
    file_path = f'{base_path}/company_report/{symbol}_balance.csv'
    if os.path.exists(file_path):
        df_balance = pd.read_csv(file_path)
        # Chuẩn hóa cấu trúc
        df_balance = normalize_columns(df_balance, BALANCE_COLUMNS)
        df_balance['symbol'] = symbol
        df_balance.to_sql('balance', engine, schema='raw', if_exists='append', index=False)
        print(f"Đã đẩy dữ liệu cân đối kế toán cho {symbol}")
    else:
        print(f"File {file_path} không tồn tại, bỏ qua lưu balance cho {symbol}.")

def load_cash_for_symbol(symbol, base_path, engine):
    file_path = f'{base_path}/company_report/{symbol}_cash.csv'
    if os.path.exists(file_path):
        df_cash = pd.read_csv(file_path)
        # Chuẩn hóa cấu trúc
        df_cash = normalize_columns(df_cash, CASH_COLUMNS)
        df_cash['symbol'] = symbol
        df_cash.to_sql('cash', engine, schema='raw', if_exists='append', index=False)
        print(f"Đã đẩy dữ liệu dòng tiền cho {symbol}")
    else:
        print(f"File {file_path} không tồn tại, bỏ qua lưu cash cho {symbol}.")

def load_price_for_symbol(symbol, base_path, engine):
    file_path = f'{base_path}/price/{symbol}_price.csv'
    if os.path.exists(file_path):
        df_price = pd.read_csv(file_path)
        df_price['symbol'] = symbol
        df_price.to_sql('price', engine, schema='raw', if_exists='append', index=False)
        print(f"Đã đẩy dữ liệu giá cho {symbol}")

def load_to_postgres():
    engine = create_engine('postgresql://postgres:postgres@stock_b7156c-postgres-1:5432/stockwh')
    base_path = '/usr/local/airflow/data/raw_data'

    # Load profile, income, balance, cash song song
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(lambda symbol: load_price_for_symbol(symbol, base_path, engine), config.all_symbols)
        executor.map(lambda symbol: load_news_for_symbol(symbol, base_path, engine), config.all_symbols)
        executor.map(lambda symbol: load_profile_for_symbol(symbol, base_path, engine), config.all_symbols)
        executor.map(lambda symbol: load_income_for_symbol(symbol, base_path, engine), config.all_symbols)
        executor.map(lambda symbol: load_balance_for_symbol(symbol, base_path, engine), config.all_symbols)
        executor.map(lambda symbol: load_cash_for_symbol(symbol, base_path, engine), config.all_symbols)

if __name__ == "__main__":
    load_to_postgres()