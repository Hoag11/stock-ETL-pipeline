from vnstock import Vnstock 
from vnstock3.botbuilder.noti import Messenger
from datetime import datetime
import os

def run():
    # Khởi tạo Telegram Messenger
    noti = Messenger(
        platform='telegram',
        channel='-4644357398',
        token_key='8031535646:AAGFjLf_qGi8kuvutg1xY4b9nxdx42qjMQc'
    )

    # Khởi tạo đối tượng Vnstock với nguồn TCBS
    stock = Vnstock().stock(symbol='TCBS', source='TCBS')

    # Ngày bắt đầu và kết thúc
    start_date = '2025-03-01'
    end_date = datetime.now().strftime('%Y-%m-%d')

    # Danh sách mã
    symbols = ["HPG", "FPT", "VNM"]

    # Tạo thư mục
    file_path = 'otp/airflow/data/price'
    os.makedirs(file_path, exist_ok=True)

    # Hàm định dạng thông báo
    def format_price_message(symbol, df_price):
        if df_price.empty or df_price is None:
            return f"Không có dữ liệu cho {symbol}"

        latest_data = df_price.iloc[-1]
        message = (
            f"**Dữ liệu giá cổ phiếu {symbol} ({latest_data['time']})**\n"
            f"- Giá mở cửa: {latest_data['open']}\n"
            f"- Giá cao nhất: {latest_data['high']}\n"
            f"- Giá thấp nhất: {latest_data['low']}\n"
            f"- Giá đóng cửa: {latest_data['close']}\n"
            f"- Khối lượng: {latest_data['volume']}"
        )
        return message

    # Thu thập và gửi dữ liệu
    for symbol in symbols:
        try:
            df_price = stock.quote.history(symbol=symbol, start=start_date, end=end_date)
            df_price.to_csv(f'{file_path}/{symbol}_price_history.csv', index=False)
            print(f"Đã lưu dữ liệu cho {symbol}")

            message = format_price_message(symbol, df_price)
            noti.send_message(message=message)
            print(f"Đã gửi thông báo cho {symbol}")

        except Exception as e:
            error_message = f"Lỗi khi xử lý {symbol}: {str(e)}"
            noti.send_message(message=error_message)
            print(error_message)

if __name__ == '__main__':
    run()
