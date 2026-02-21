from selenium import webdriver
from selenium.webdriver.common.by import By
import keyboard
import os
import time
import re
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

FOLDER_TO_WATCH = r"C:\Users\Admin\Desktop\trading_data"

# File đã rename sẽ có dạng: 20260220_143501_original.csv
RENAMED_PATTERN = re.compile(r"^\d{8}_\d{6}_")

recently_handled = {}  # path -> last_time
COOLDOWN_SECONDS = 5

def make_unique_path(folder, filename):
    base, ext = os.path.splitext(filename)
    candidate = filename
    i = 1
    while os.path.exists(os.path.join(folder, candidate)):
        candidate = f"{base}_{i}{ext}"
        i += 1
    return os.path.join(folder, candidate)

def wait_until_stable(file_path, timeout=120):
    start = time.time()
    last_size = -1
    stable_count = 0

    while time.time() - start < timeout:
        if not os.path.exists(file_path):
            time.sleep(0.2)
            continue

        try:
            size = os.path.getsize(file_path)
        except OSError:
            size = -1

        if size == last_size and size > 0:
            stable_count += 1
            if stable_count >= 3:
                return True
        else:
            stable_count = 0
            last_size = size

        time.sleep(0.5)

    return False

def should_ignore(file_path):
    now = time.time()

    # cooldown theo path để tránh bắn event nhiều lần
    last = recently_handled.get(file_path)
    if last and (now - last) < COOLDOWN_SECONDS:
        return True

    # dọn cache cũ cho gọn
    for k in list(recently_handled.keys()):
        if now - recently_handled[k] > 30:
            recently_handled.pop(k, None)

    return False

def rename_csv(file_path):
    folder, filename = os.path.split(file_path)

    # ignore file tạm
    if filename.lower().endswith(".crdownload"):
        return

    # chỉ CSV
    if not filename.lower().endswith(".csv"):
        return

    # nếu đã có prefix timestamp thì bỏ qua (ngăn rename loop)
    if RENAMED_PATTERN.match(filename):
        return

    # tránh xử lý lặp do nhiều event
    if should_ignore(file_path):
        return

    # đánh dấu đang xử lý
    recently_handled[file_path] = time.time()

    if not wait_until_stable(file_path, timeout=120):
        print(f"[Rename] Timeout/chưa ổn định: {filename}")
        return

    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_filename = f"{now_str}_{filename}"
    new_path = make_unique_path(folder, new_filename)

    try:
        os.rename(file_path, new_path)
        # đánh dấu cả path mới để event tiếp theo bị ignore
        recently_handled[new_path] = time.time()
        print(f"[Rename] OK: {filename} -> {os.path.basename(new_path)}")
    except Exception as e:
        print(f"[Rename] Lỗi: {filename}: {e}")

class RenameHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        rename_csv(event.src_path)

    def on_moved(self, event):
        if event.is_directory:
            return
        rename_csv(event.dest_path)

if __name__ == "__main__":
    # Chrome download về đúng folder
    options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": FOLDER_TO_WATCH,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=options)

    observer = Observer()
    observer.schedule(RenameHandler(), FOLDER_TO_WATCH, recursive=False)
    observer.start()
    print(f"Đang theo dõi: {FOLDER_TO_WATCH} (Ctrl+C để dừng)")

    try:
        driver.get("https://iboard.ssi.com.vn/")

        wait = WebDriverWait(driver, 20)

        print("Bắt đầu auto export mỗi 30 giây...")

        while True:
            try:
                # chờ button sẵn sàng để click
                button = wait.until(
                    EC.element_to_be_clickable((By.ID, "btnExportPriceboard"))
                )
                button.click()

                print("Đã click Export lúc:", datetime.now().strftime("%H:%M:%S"))

            except Exception as e:
                print("Lỗi khi click:", e)

            time.sleep(10)  # đợi 30 giây rồi lặp lại

    except KeyboardInterrupt:
        print("Dừng chương trình.")
    finally:
        observer.stop()
        observer.join()
        driver.quit()