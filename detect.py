import pandas as pd
import numpy as np

# ===== CONFIG =====
CSV_PATH = r"C:\Users\Admin\Desktop\trading_data\5_wick_lower_sample.csv"

MIN_LOWER_WICK_PCT_RANGE = 0.45   # wick dưới >= 45% range
MIN_LOWER_WICK_MULT_BODY = 2.0    # wick dưới >= 2 lần body
MIN_CLOSE_POSITION = 0.70         # close nằm trong top 30%
REQUIRE_BULLISH_CLOSE = False
# ===================


def add_lower_wick_signal(csv_path):
    df = pd.read_csv(csv_path)

    # đảm bảo numeric
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

    # thêm cột signal vào dataframe
    df["signal"] = np.where(signal, "yes", "no")

    # ghi đè lại file gốc
    df.to_csv(csv_path, index=False)

    print("Đã thêm cột 'signal' vào file.")


if __name__ == "__main__":
    add_lower_wick_signal(CSV_PATH)