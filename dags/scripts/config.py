import random
from vnstock import Vnstock 

max_workers = 1
rate_limit = random.randint(15, 50)

all_symbols = [
    "HPG",  # Thép Hòa Phát
    "VNM",  # Sữa Vinamilk
    "FPT",  # Công nghệ FPT
    "VCB",  # Ngân hàng Vietcombank
    "BID",  # Ngân hàng BIDV
    "CTG",  # VietinBank
    "MWG",  # Bán lẻ Thế Giới Di Động
    "VHM",  # Bất động sản Vinhomes
    "MSN",  # Masan Group
    "TCB",  # Ngân hàng Techcombank
]