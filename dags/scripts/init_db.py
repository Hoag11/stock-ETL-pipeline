from sqlalchemy import create_engine

def init_db():
    engine = create_engine('postgresql://postgres:postgres@stock_b7156c-postgres-1:5432/stockwh')
    with engine.connect() as conn:
        conn.execute("""
            TRUNCATE TABLE raw.price;
            TRUNCATE TABLE raw.news;
            TRUNCATE TABLE raw.cash;
            TRUNCATE TABLE raw.income;
            TRUNCATE TABLE raw.profile;
            TRUNCATE TABLE raw.balance;
        """)
        print("Đã xóa dữ liệu và giữ cấu trúc các bảng trong schema raw.")

if __name__ == "__main__":
    init_db()