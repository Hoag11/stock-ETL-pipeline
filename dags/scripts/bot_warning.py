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

    stock = Vnstock().stock(symbol='TCBS', source='TCBS')
    start_date = '2025-03-01'
    end_date = datetime.now().strftime('%Y-%m-%d')
    symbols = ["HPG", "FPT", "VNM"]

    file_path = 'otp/airflow/data/price'
    os.makedirs(file_path, exist_ok=True)

    def format_price_message(symbol, df_price):
        if df_price.empty or df_price is None:
            return f"Không có dữ liệu cho {symbol}"

        df_price["price_change_pct"] = df_price["close"].pct_change() * 100
        df_price["volume_ma5"] = df_price["volume"].rolling(window=5).mean()

        latest_data = df_price.iloc[-1]
        price_change = df_price["price_change_pct"].iloc[-1]
        avg_volume_5 = df_price["volume_ma5"].iloc[-1]
        latest_volume = latest_data["volume"]

        message = (
            f"**{symbol} ({latest_data['time']})**"
            f"- Giá mở cửa: {latest_data['open']}"
            f"- Giá cao nhất: {latest_data['high']}"
            f"- Giá thấp nhất: {latest_data['low']}"
            f"- Giá đóng cửa: {latest_data['close']}"
            f"- Khối lượng: {latest_volume} (TB 5 ngày: {avg_volume_5:.0f})"
            f"- Biến động giá: {price_change:.2f}%"
        )

        if abs(price_change) > 5:
            message += "\n⚠️ Biến động giá mạnh!"
        if latest_volume > 2 * avg_volume_5:
            message += "\n🚨 Volume đột biến!"

        return message

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