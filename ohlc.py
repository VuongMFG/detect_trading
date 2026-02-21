import os
import re
import glob
from datetime import datetime, time as dtime

import pandas as pd

# ======================
# CONFIG
# ======================
FOLDER = r"C:\Users\Admin\Desktop\trading_data"

# Khung nến OHLC bạn muốn xuất (resample)
# gợi ý: "1min", "5min", "15min"
TIMEFRAME = "60min"

# Excel column: A = 1 (index 0), K = 11 (index 10)
COL_SYMBOL_IDX = 0
COL_K_IDX = 10

# Các khoảng cố định (giờ:phút)
SESSIONS = [
    ("0900", "1000"),
    ("1000", "1100"),
    ("1100", "1130"),
    ("1300", "1400"),
    ("1400", "1430"),
]

# Tên file: 20260220_144417_....csv
TS_PATTERN = re.compile(r"^(?P<ts>\d{8}_\d{6})_.*\.csv$", re.IGNORECASE)


# ======================
# Helpers
# ======================
def parse_ts_from_filename(path: str) -> datetime | None:
    m = TS_PATTERN.match(os.path.basename(path))
    if not m:
        return None
    return datetime.strptime(m.group("ts"), "%Y%m%d_%H%M%S")


def read_symbol_price_from_file(path: str) -> pd.DataFrame:
    """
    Đọc 1 file CSV kiểu iBoard export, lấy:
      - symbol: cột A
      - price : cột K (Khớp lệnh - Giá)
    """
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
    """
    Gom toàn bộ tick: time + symbol + price (cột K)
    """
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
    """
    Lọc dữ liệu theo khoảng giờ trong ngày, rồi resample OHLC theo timeframe cho từng mã.
    """
    if data.empty:
        return pd.DataFrame(columns=["date", "session", "time", "symbol", "open", "high", "low", "close"])

    start_t = hhmm_to_time(start_hhmm)
    end_t = hhmm_to_time(end_hhmm)

    # Lọc theo time-of-day
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

    # đảm bảo time là datetime
    data["time"] = pd.to_datetime(data["time"], errors="coerce")
    data = data.dropna(subset=["time"])

    # Export mỗi session ra 1 file CSV (gọn, dễ dùng)
    for start_hhmm, end_hhmm in SESSIONS:
        sess_df = ohlc_for_session(data, start_hhmm, end_hhmm, timeframe)
        if sess_df.empty:
            print(f"Session {start_hhmm}-{end_hhmm}: không có dữ liệu.")
            continue

        # Nếu bạn muốn tách theo ngày, phần date trong file name sẽ tự gồm các ngày có dữ liệu.
        out_path = os.path.join(folder, f"OHLC_{timeframe}_{start_hhmm}-{end_hhmm}.csv")
        sess_df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"OK: {start_hhmm}-{end_hhmm} -> {out_path} ({len(sess_df)} dòng)")

    # (Tuỳ chọn) xuất 1 file tổng hợp tất cả session
    all_out = []
    for start_hhmm, end_hhmm in SESSIONS:
        all_out.append(ohlc_for_session(data, start_hhmm, end_hhmm, timeframe))
    all_out = [x for x in all_out if not x.empty]
    if all_out:
        merged = pd.concat(all_out, ignore_index=True).sort_values(["date", "session", "time", "symbol"])
        merged_path = os.path.join(folder, f"OHLC_ALL_SESSIONS_{timeframe}.csv")
        merged.to_csv(merged_path, index=False, encoding="utf-8-sig")
        print(f"OK: Tổng hợp -> {merged_path} ({len(merged)} dòng)")


if __name__ == "__main__":
    export_sessions(FOLDER, TIMEFRAME)