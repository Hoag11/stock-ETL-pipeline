from vnstock3 import Vnstock 
from vnstock3.botbuilder.noti import Messenger
from datetime import datetime
import os

# Khởi tạo Telegram Messenger
noti = Messenger(
    platform='telegram',
    channel='-4644357398',  # Thay bằng channel ID của bạn
    token_key='8031535646:AAGFjLf_qGi8kuvutg1xY4b9nxdx42qjMQc'  # Thay bằng token bot của bạn
)

# Khởi tạo đối tượng Vnstock với nguồn TCBS
stock = Vnstock().stock(symbol='VCI', source='VCI')

# Định nghĩa ngày bắt đầu và kết thúc
start_date = '2025-03-01'  # Có thể thay đổi ngày bắt đầu
end_date = datetime.now().strftime('%Y-%m-%d')  # Ngày hiện tại

# Danh sách mã cổ phiếu
symbols = ['HPG', 'FPT', 'VNM']

# Đảm bảo thư mục lưu trữ tồn tại
file_path = 'otp/airflow/data/price'
os.makedirs(file_path, exist_ok=True)

# Hàm định dạng thông tin để gửi qua Telegram
def format_price_message(symbol, df_price):
    if df_price.empty or df_price is None:
        return f"Không có dữ liệu cho {symbol}"
    
    # Lấy dữ liệu mới nhất (dòng cuối cùng)
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

def run():
    # Thu thập và gửi dữ liệu
    for symbol in symbols:
        try:
            # Lấy dữ liệu lịch sử giá trực tiếp từ Vnstock
            df_price = stock.quote.history(symbol=symbol, start=start_date, end=end_date)
            
            # Lưu vào file CSV
            df_price.to_csv(f'{file_path}/{symbol}_price_history.csv', index=False)
            print(f"Đã lưu dữ liệu cho {symbol}")

            # Tạo và gửi thông báo qua Telegram
            message = format_price_message(symbol, df_price)
            noti.send_message(message=message)
            print(f"Đã gửi thông báo cho {symbol}")

        except Exception as e:
            error_message = f"Lỗi khi xử lý {symbol}: {str(e)}"
            noti.send_message(message=error_message)
            print(error_message)
