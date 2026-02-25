import time
import random
import pandas as pd
from vnstock import Listing, Quote

# ================= CONFIG =================
SOURCE = "vci"          # nếu vẫn bị limit, đổi "tcbs"
START = "2025-01-01"    # lấy dư để chắc chắn đủ >= 60 phiên
INTERVAL = "1D"
TAKE_LAST_N = 60

SLEEP_EACH_SYMBOL = (1.2, 2.2)   # nghỉ ngẫu nhiên giữa các mã (giây)
PAUSE_BETWEEN_BATCH = 120        # nghỉ giữa 2 lượt (giây) -> có thể tăng 180 nếu vẫn dính limit

OUT_FINAL = "VN30_daily_last60.csv"
OUT_BATCH1 = "VN30_batch1.csv"
OUT_BATCH2 = "VN30_batch2.csv"
OUT_FAILED = "VN30_failed.csv"
# ==========================================

def normalize_history(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    df = df.copy()

    # chuẩn hoá time/date
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
    elif "date" in df.columns:
        df["time"] = pd.to_datetime(df["date"], errors="coerce")
    else:
        raise ValueError("No time/date column")

    keep = [c for c in ["time", "open", "high", "low", "close", "volume", "value"] if c in df.columns]
    needed = {"time", "open", "high", "low", "close", "volume"}
    if not needed.issubset(set(keep)):
        raise ValueError(f"Missing required columns. Have: {df.columns.tolist()}")

    df = (df[keep]
          .dropna(subset=["time", "close", "volume"])
          .sort_values("time")
          .tail(TAKE_LAST_N)
          .reset_index(drop=True))

    df.insert(0, "symbol", symbol)

    # ép kiểu số cho chắc
    for c in ["open", "high", "low", "close", "volume"] + (["value"] if "value" in df.columns else []):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["close", "volume"])

    return df

def fetch_batch(symbols, batch_name: str):
    quote = Quote(source=SOURCE, symbol=symbols[0])  # init; khi history sẽ truyền symbol cụ thể
    batch_frames = []
    failed = []

    for i, sym in enumerate(symbols, 1):
        try:
            df = quote.history(symbol=sym, start=START, end=None, interval=INTERVAL)
            if df is None or len(df) == 0:
                raise ValueError("Empty data returned")

            df = normalize_history(df, sym)
            batch_frames.append(df)

            print(f"[{batch_name}] OK {sym} ({i}/{len(symbols)}) rows={len(df)}")

        except Exception as e:
            failed.append((sym, f"{batch_name}: {e}"))
            print(f"[{batch_name}] FAIL {sym} ({i}/{len(symbols)}): {e}")

        # nghỉ giữa từng mã để tránh burst
        time.sleep(random.uniform(*SLEEP_EACH_SYMBOL))

    return batch_frames, failed

# ================= MAIN =================
print("Đang lấy danh sách VN30...")
listing = Listing(source=SOURCE)
vn30 = listing.symbols_by_group("VN30")
symbols = vn30.tolist() if hasattr(vn30, "tolist") else list(vn30)
symbols = [str(s).strip().upper() for s in symbols if str(s).strip()]
print(f"Tổng số mã VN30: {len(symbols)}")

first = symbols[:15]
second = symbols[15:]

print("\n=== LƯỢT 1/2: 15 mã đầu ===")
frames1, failed1 = fetch_batch(first, "BATCH1")
if frames1:
    df1 = pd.concat(frames1, ignore_index=True)
    df1.to_csv(OUT_BATCH1, index=False, encoding="utf-8-sig")
    print(f"Đã lưu tạm: {OUT_BATCH1} | rows={len(df1)} | symbols={df1['symbol'].nunique()}")

print(f"\nNghỉ {PAUSE_BETWEEN_BATCH}s trước khi chạy lượt 2...")
time.sleep(PAUSE_BETWEEN_BATCH)

print("\n=== LƯỢT 2/2: 15 mã sau ===")
frames2, failed2 = fetch_batch(second, "BATCH2")
if frames2:
    df2 = pd.concat(frames2, ignore_index=True)
    df2.to_csv(OUT_BATCH2, index=False, encoding="utf-8-sig")
    print(f"Đã lưu tạm: {OUT_BATCH2} | rows={len(df2)} | symbols={df2['symbol'].nunique()}")

# Gộp kết quả
all_frames = []
if frames1:
    all_frames.append(pd.concat(frames1, ignore_index=True))
if frames2:
    all_frames.append(pd.concat(frames2, ignore_index=True))

if all_frames:
    final_df = pd.concat(all_frames, ignore_index=True)
    final_df.to_csv(OUT_FINAL, index=False, encoding="utf-8-sig")
    print(f"\n✅ Đã lưu file cuối: {OUT_FINAL} | rows={len(final_df)} | symbols={final_df['symbol'].nunique()}")
else:
    print("\n❌ Không có dữ liệu nào được tải.")

# Lưu lỗi (nếu có)
failed = failed1 + failed2
if failed:
    pd.DataFrame(failed, columns=["symbol", "error"]).to_csv(OUT_FAILED, index=False, encoding="utf-8-sig")
    print(f"⚠️ Có {len(failed)} mã lỗi -> xem {OUT_FAILED}")

print("\nHoàn tất.")
