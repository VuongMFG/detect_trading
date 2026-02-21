import os
import re
import glob
from datetime import datetime, time as dtime
import pandas as pd
import numpy as np

# ======================
# CONFIG
# ======================
FOLDER = r"C:\Users\Admin\Desktop\trading_data"
TIMEFRAME = "60min"
COL_SYMBOL_IDX = 0
COL_K_IDX = 10
SESSIONS = [
    ("0900", "1000"),
    ("1000", "1100"),
    ("1100", "1130"),
    ("1300", "1400"),
    ("1400", "1430"),
]
TS_PATTERN = re.compile(r"^(?P<ts>\d{8}_\d{6})_.*\.csv$", re.IGNORECASE)

# Detect config
MIN_LOWER_WICK_PCT_RANGE = 0.45   # wick dưới >= 45% range
MIN_LOWER_WICK_MULT_BODY = 2.0    # wick dưới >= 2 lần body
MIN_CLOSE_POSITION = 0.70         # close nằm trong top 30%
REQUIRE_BULLISH_CLOSE = False

# ======================
# OHLC Helpers
# ======================
def parse_ts_from_filename(path: str) -> datetime | None:
    m = TS_PATTERN.match(os.path.basename(path))
    if not m:
        return None
    return datetime.strptime(m.group("ts"), "%Y%m%d_%H%M%S")

def read_symbol_price_from_file(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if df.shape[1] <= COL_K_IDX:
        return pd.DataFrame(columns=["symbol", "price"])
    x = df.iloc[:, [COL_SYMBOL_IDX, COL_K_IDX]].copy()
    x.columns = ["symbol", "price"]
    x["symbol"] = x["symbol"].astype(str).str.strip()
    x = x[(x["symbol"] != "") & (x["symbol"].str.upper() != "CK")]
    x["price"] = pd.to_numeric(x["price"], errors="coerce")
    x = x.dropna(subset=["price"])
    return x

def hhmm_to_time(hhmm: str) -> dtime:
    return dtime(int(hhmm[:2]), int(hhmm[2:]))

def build_ticks_from_folder(folder: str) -> pd.DataFrame:
    files = glob.glob(os.path.join(folder, "*.csv"))
    rows = []
    for f in files:
        ts = parse_ts_from_filename(f)
        if ts is None:
            continue
        try:
            sp = read_symbol_price_from_file(f)
        except Exception:
            continue
        if sp.empty:
            continue
        sp["time"] = ts
        rows.append(sp)
    if not rows:
        return pd.DataFrame(columns=["time", "symbol", "price"])
    data = pd.concat(rows, ignore_index=True)
    data = data.sort_values(["symbol", "time"])
    return data

def ohlc_for_session(data: pd.DataFrame, start_hhmm: str, end_hhmm: str, timeframe: str) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["date", "session", "time", "symbol", "open", "high", "low", "close"])
    start_t = hhmm_to_time(start_hhmm)
    end_t = hhmm_to_time(end_hhmm)
    mask = (data["time"].dt.time >= start_t) & (data["time"].dt.time < end_t)
    sub = data.loc[mask].copy()
    if sub.empty:
        return pd.DataFrame(columns=["date", "session", "time", "symbol", "open", "high", "low", "close"])
    session_name = f"{start_hhmm}-{end_hhmm}"
    out = []
    for sym, g in sub.groupby("symbol"):
        s = g.set_index("time")["price"].sort_index()
        ohlc = s.resample(timeframe).ohlc().dropna()
        ohlc["symbol"] = sym
        out.append(ohlc.reset_index())
    if not out:
        return pd.DataFrame(columns=["date", "session", "time", "symbol", "open", "high", "low", "close"])
    res = pd.concat(out, ignore_index=True)
    res["date"] = res["time"].dt.strftime("%Y%m%d")
    res["session"] = session_name
    res = res[["date", "session", "time", "symbol", "open", "high", "low", "close"]]
    return res.sort_values(["date", "session", "time", "symbol"])

def export_sessions(folder: str, timeframe: str):
    data = build_ticks_from_folder(folder)
    if data.empty:
        print("Không có dữ liệu tick hợp lệ từ các file trong folder (kiểm tra tên file/timestamp & cột K).")
        return
    data["time"] = pd.to_datetime(data["time"], errors="coerce")
    data = data.dropna(subset=["time"])
    for start_hhmm, end_hhmm in SESSIONS:
        sess_df = ohlc_for_session(data, start_hhmm, end_hhmm, timeframe)
        if sess_df.empty:
            print(f"Session {start_hhmm}-{end_hhmm}: không có dữ liệu.")
            continue
        out_path = os.path.join(folder, f"OHLC_{timeframe}_{start_hhmm}-{end_hhmm}.csv")
        sess_df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"OK: {start_hhmm}-{end_hhmm} -> {out_path} ({len(sess_df)} dòng)")
    all_out = []
    for start_hhmm, end_hhmm in SESSIONS:
        all_out.append(ohlc_for_session(data, start_hhmm, end_hhmm, timeframe))
    all_out = [x for x in all_out if not x.empty]
    if all_out:
        merged = pd.concat(all_out, ignore_index=True).sort_values(["date", "session", "time", "symbol"])
        merged_path = os.path.join(folder, f"OHLC_ALL_SESSIONS_{timeframe}.csv")
        merged.to_csv(merged_path, index=False, encoding="utf-8-sig")
        print(f"OK: Tổng hợp -> {merged_path} ({len(merged)} dòng)")

# ======================
# Detect Helpers
# ======================
def add_lower_wick_signal(csv_path):
    df = pd.read_csv(csv_path)
    for c in ["open", "high", "low", "close"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    o = df["open"]
    h = df["high"]
    l = df["low"]
    c = df["close"]
    eps = 1e-9
    candle_range = (h - l).clip(lower=0)
    body = (c - o).abs()
    lower_wick = (np.minimum(o, c) - l).clip(lower=0)
    lower_wick_pct_range = lower_wick / (candle_range + eps)
    lower_wick_mult_body = lower_wick / (body + eps)
    close_position = (c - l) / (candle_range + eps)
    cond1 = lower_wick_pct_range >= MIN_LOWER_WICK_PCT_RANGE
    cond2 = lower_wick_mult_body >= MIN_LOWER_WICK_MULT_BODY
    cond3 = close_position >= MIN_CLOSE_POSITION
    cond4 = (c >= o) if REQUIRE_BULLISH_CLOSE else True
    signal = cond1 & cond2 & cond3 & cond4
    df["signal"] = np.where(signal, "yes", "no")
    if (df["signal"] == "yes").sum() == 0:
        # Tạo file trắng với header
        df.iloc[0:0].to_csv(csv_path, index=False)
        print("Không detect được nến rút chân nào, file detect.csv là file trắng.")
    else:
        df.to_csv(csv_path, index=False)
        print(f"Đã thêm cột 'signal' vào file. Số dòng có nến rút chân: {(df['signal'] == 'yes').sum()}")

# ======================
# Main
# ======================
if __name__ == "__main__":
    import time
    import argparse
    # Không cần chọn mode, chạy tuần tự các bước
    folder = FOLDER
    timeframe = TIMEFRAME
    csv_path = os.path.join(folder, 'detect.csv')

    # Bước 1: xuất OHLC từng phiên và tổng hợp vào ohlc.csv
    export_sessions(folder, timeframe)
    # Tổng hợp tất cả các phiên vào ohlc.csv
    all_ohlc = []
    for start_hhmm, end_hhmm in SESSIONS:
        file_path = os.path.join(folder, f"OHLC_{timeframe}_{start_hhmm}-{end_hhmm}.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            all_ohlc.append(df)
    if all_ohlc:
        ohlc_all = pd.concat(all_ohlc, ignore_index=True)
        ohlc_all.to_csv(os.path.join(folder, "ohlc.csv"), index=False)
        print(f"Đã tạo file tổng hợp OHLC: {os.path.join(folder, 'ohlc.csv')}")
    else:
        print("Không có dữ liệu OHLC để tổng hợp.")

    # Bước 2: detect signal nến rút chân
    if not os.path.exists(csv_path):
        # Tạo file detect.csv trắng với header
        pd.DataFrame(columns=["date","session","time","symbol","open","high","low","close"]).to_csv(csv_path, index=False)
    add_lower_wick_signal(csv_path)

    # Bước 3: auto update sessions
    def get_current_session():
        now = datetime.now().time()
        for start_hhmm, end_hhmm in SESSIONS:
            start_t = hhmm_to_time(start_hhmm)
            end_t = hhmm_to_time(end_hhmm)
            if start_t <= now < end_t:
                return start_hhmm, end_hhmm
        return None, None

    while True:
        start_hhmm, end_hhmm = get_current_session()
        if start_hhmm:
            session_name = f"{start_hhmm}-{end_hhmm}"
            out_path = os.path.join(folder, f"OHLC_{timeframe}_{session_name}.csv")
            # Xóa file cũ
            if os.path.exists(out_path):
                os.remove(out_path)
            # Xuất dữ liệu phiên hiện tại
            data = build_ticks_from_folder(folder)
            data["time"] = pd.to_datetime(data["time"], errors="coerce")
            data = data.dropna(subset=["time"])
            sess_df = ohlc_for_session(data, start_hhmm, end_hhmm, timeframe)
            sess_df.to_csv(out_path, index=False, encoding="utf-8-sig")
            print(f"Đã cập nhật file {out_path} ({len(sess_df)} dòng)")
        else:
            print("Không nằm trong phiên nào, chờ...")
        time.sleep(60)  # Kiểm tra mỗi phút
    # ...existing code...
