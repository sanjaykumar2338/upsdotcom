import os
import sys
import time
import random
import csv
import pathlib
import threading
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup

try:
    import openpyxl
except ImportError:
    openpyxl = None

# ------------ Config ------------
FEDEX_BASE_URL = "https://www.fedexfreight.fedex.com/servicemaps.jsp"
MAX_WORKERS = 2  # can increase to 4 if desired
HTTP_TIMEOUT = 40
WAIT_TIMEOUT = 20
HEADLESS = False
HUMAN_DELAY_RANGE_MS = (3000, 4000)
PAUSE_EVERY = 100
PAUSE_SECONDS = 60
DEBUG_SCREENSHOT_DIR = "debug_shots"

STATUS_OK = "OK"
STATUS_NO_DATA = "NO_DATA"
STATUS_ERROR = "ERROR"
STATUS_TIMEOUT = "TIMEOUT"

NO_DATA_SNIPPETS = [
    "no results",
    "no information",
    "invalid",
    "not found",
    "try again",
    "either the zip code does not exist",
]

# ------------ Shared state ------------
write_lock = threading.Lock()
stats_lock = threading.Lock()
stop_event = threading.Event()
stats = {"processed": 0, "skipped": 0, "total": 0}
worker_thread_global = None

def default_output_for_input(path: str) -> str:
    p = pathlib.Path(path)
    ext = p.suffix
    return str(p.with_name(f"result_{p.stem}{ext}"))


# ------------ Helpers ------------
def log(msg):
    print(msg, flush=True)


def collapse(txt: str) -> str:
    return " ".join((txt or "").split())


def ensure_dir(p: pathlib.Path):
    p.parent.mkdir(parents=True, exist_ok=True)


def ensure_header_csv(path, cols):
    if (not os.path.exists(path)) or os.path.getsize(path) == 0:
        with open(path, "w", newline="", encoding="utf-8") as fp:
            csv.writer(fp).writerow(cols)
        return cols
    try:
        with open(path, newline="", encoding="utf-8") as fp:
            reader = csv.reader(fp)
            first_row = next(reader, None)
            if first_row != cols:
                rows = [first_row] + list(reader) if first_row else list(reader)
                with open(path, "w", newline="", encoding="utf-8") as out:
                    w = csv.writer(out)
                    w.writerow(cols)
                    w.writerows(r for r in rows if r)
    except Exception:
        with open(path, "w", newline="", encoding="utf-8") as fp:
            csv.writer(fp).writerow(cols)
    return cols


def ensure_header_xlsx(path, cols):
    if not openpyxl:
        raise RuntimeError("openpyxl not installed; required for XLSX output.")
    from openpyxl import Workbook, load_workbook

    if not os.path.exists(path) or os.path.getsize(path) == 0:
        wb = Workbook()
        ws = wb.active
        ws.append(cols)
        wb.save(path)
        return cols
    wb = load_workbook(path)
    ws = wb.active
    existing = [c.value for c in next(ws.iter_rows(max_row=1))] if ws.max_row else []
    if existing != cols:
        data_rows = list(ws.iter_rows(values_only=True))[1:] if ws.max_row else []
        wb = Workbook()
        ws = wb.active
        ws.append(cols)
        for r in data_rows:
            if r:
                ws.append(r)
    wb.save(path)
    return cols


def append_row_csv(path, cols, row, tries=20, base_sleep=0.25):
    for i in range(tries):
        try:
            with write_lock, open(path, "a", newline="", encoding="utf-8") as fp:
                w = csv.DictWriter(fp, fieldnames=cols)
                w.writerow(row)
                fp.flush()
                os.fsync(fp.fileno())
            return
        except PermissionError:
            time.sleep(base_sleep * (1 + i / 3.0))
    alt = f"{os.path.splitext(path)[0]}_{os.getpid()}_{int(time.time())}.csv"
    with write_lock, open(alt, "a", newline="", encoding="utf-8") as fp:
        w = csv.DictWriter(fp, fieldnames=cols)
        if fp.tell() == 0:
            w.writeheader()
        w.writerow(row)


def append_row_xlsx(path, cols, row):
    if not openpyxl:
        raise RuntimeError("openpyxl not installed; required for XLSX output.")
    from openpyxl import load_workbook

    with write_lock:
        wb = load_workbook(path)
        ws = wb.active
        ws.append([row.get(c, "") for c in cols])
        wb.save(path)


def ensure_header_any(path, cols, use_xlsx):
    return ensure_header_xlsx(path, cols) if use_xlsx else ensure_header_csv(path, cols)


def append_row_any(path, cols, row, use_xlsx):
    if use_xlsx:
        return append_row_xlsx(path, cols, row)
    return append_row_csv(path, cols, row)


def read_input_rows(path: str):
    p = pathlib.Path(path)
    ext = p.suffix.lower()
    zips = []
    if ext == ".csv":
        with open(path, encoding="utf-8-sig", newline="") as f:
            first = True
            for row in csv.reader(f):
                if not row:
                    continue
                z = str(row[0]).strip()
                if first and not z.isdigit():
                    first = False
                    continue
                first = False
                if z:
                    zips.append(z)
    elif ext == ".xlsx":
        if not openpyxl:
            raise RuntimeError("openpyxl not installed; required for XLSX input.")
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        sheet = wb.active
        first = True
        for row in sheet.iter_rows(values_only=True):
            if not row:
                continue
            v = row[0]
            if v is None:
                continue
            z = str(v).strip()
            if first and not z.isdigit():
                first = False
                continue
            first = False
            if z:
                zips.append(z)
        wb.close()
    else:
        raise RuntimeError("Unsupported input type. Use .csv or .xlsx")
    return zips


def detect_block(body_text: str) -> bool:
    t = body_text.lower()
    return any(
        s in t
        for s in [
            "access denied",
            "request blocked",
            "sign in",
            "login",
            "too many requests",
            "temporarily unavailable",
            "captcha",
        ]
    )


def extract_fedex_message(driver):
    from selenium.webdriver.common.by import By

    candidates = [
        "[role='alert']",
        ".error",
        ".alert",
        ".message",
        ".validation",
    ]
    for sel in candidates:
        try:
            elems = driver.find_elements(By.CSS_SELECTOR, sel)
            for e in elems:
                txt = collapse(e.text)
                if txt:
                    return clean_message(txt)
        except Exception:
            continue
    try:
        body = driver.find_element(By.TAG_NAME, "body").text
    except Exception:
        body = ""
    return clean_message(body)


def clean_message(text: str) -> str:
    if not text:
        return ""
    text = collapse(text)
    low = text.lower()
    start_keys = ["sorry", "error", "no information", "no results", "invalid", "not found"]
    start_idx = 0
    for k in start_keys:
        pos = low.find(k)
        if pos != -1:
            start_idx = pos
            break
    text = text[start_idx:].strip()
    # cut at support/help/tracking
    cut_keys = ["support", "tracking", "ups sites", "connect with us"]
    low = text.lower()
    cut = len(text)
    for k in cut_keys:
        pos = low.find(k)
        if pos != -1:
            cut = min(cut, pos)
    return text[:cut].strip()


def detect_fedex_no_data(driver):
    from selenium.webdriver.common.by import By

    try:
        err_elems = driver.find_elements(By.CSS_SELECTOR, ".errortext, span.errortext, div.errortext")
        for e in err_elems:
            txt = collapse(e.text)
            if txt:
                return True
    except Exception:
        pass

    try:
        body = driver.find_element(By.TAG_NAME, "body").text
    except Exception:
        body = ""
    low = body.lower()
    return any(snippet in low for snippet in NO_DATA_SNIPPETS)


def get_hidden_map_url(driver):
    from selenium.webdriver.common.by import By

    hidden_ids = ["popUpMapURLHidden", "mapURLHidden", "popUpMapUrlHidden"]
    for hid in hidden_ids:
        try:
            elem = driver.find_element(By.ID, hid)
            val = (elem.get_attribute("value") or "").strip()
            if val:
                return val
        except Exception:
            continue
    return ""


def get_pdf_map_link(driver):
    from selenium.webdriver.common.by import By

    try:
        links = driver.find_elements(By.CSS_SELECTOR, "a[href$='.pdf']")
        for a in links:
            href = (a.get_attribute("href") or "").strip()
            if "fedexfreight.fedex.com/maps" in href.lower():
                return href
    except Exception:
        pass
    return ""


def wait_fedex_result_or_terminal(driver, prev_marker, timeout=WAIT_TIMEOUT):
    from selenium.webdriver.common.by import By

    end = time.time() + timeout

    def current_body():
        try:
            return collapse(driver.find_element(By.TAG_NAME, "body").text)
        except Exception:
            return ""

    # quick checks
    pdf_link = get_pdf_map_link(driver)
    if pdf_link:
        return {"status": STATUS_OK, "map_src": pdf_link, "result_text": "Success"}
    hidden_val = get_hidden_map_url(driver)
    if hidden_val:
        return {"status": STATUS_OK, "map_src": hidden_val, "result_text": "Success"}
    if detect_fedex_no_data(driver):
        return {"status": STATUS_NO_DATA, "clean_msg": extract_fedex_message(driver)}

    while time.time() < end:
        pdf_link = get_pdf_map_link(driver)
        if pdf_link:
            return {"status": STATUS_OK, "map_src": pdf_link, "result_text": "Success"}
        hidden_val = get_hidden_map_url(driver)
        if hidden_val:
            return {"status": STATUS_OK, "map_src": hidden_val, "result_text": "Success"}
        if detect_fedex_no_data(driver):
            return {"status": STATUS_NO_DATA, "clean_msg": extract_fedex_message(driver)}

        txt = current_body()
        if txt and txt != prev_marker:
            return {"status": STATUS_OK, "map_src": "", "result_text": clean_message(txt)}
        time.sleep(0.25)

    return {"status": STATUS_TIMEOUT, "err": "Timed out waiting for results"}


def build_driver():
    from selenium import webdriver

    opts = webdriver.ChromeOptions()
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1280,900")
    if HEADLESS:
        opts.add_argument("--headless=new")
    prefs = {"profile.default_content_setting_values.notifications": 2}
    opts.add_experimental_option("prefs", prefs)
    return webdriver.Chrome(options=opts)


def pause_if_needed():
    should_pause = False
    with stats_lock:
        if stats["processed"] and stats["processed"] % PAUSE_EVERY == 0:
            should_pause = True
    if should_pause:
        log(f"[PAUSE] Processed {PAUSE_EVERY} records — waiting {PAUSE_SECONDS} seconds to avoid blocking")
        time.sleep(PAUSE_SECONDS)


def save_debug(driver, zip_code, reason):
    try:
        shot_dir = pathlib.Path(DEBUG_SCREENSHOT_DIR)
        shot_dir.mkdir(parents=True, exist_ok=True)
        stamp = int(time.time())
        path = shot_dir / f"FEDEX_{zip_code}_{reason}_{stamp}.png"
        driver.save_screenshot(str(path))
    except Exception:
        pass


def ensure_fedex_ui(driver, wait):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC

    input_selectors = [
        (By.ID, "shipperZipCode"),
        (By.CSS_SELECTOR, "input[name*='zip']"),
        (By.CSS_SELECTOR, "input[id*='zip']"),
        (By.CSS_SELECTOR, "input[type='text']"),
    ]
    btn_selectors = [
        (By.ID, "view1"),
        (By.CSS_SELECTOR, "button[type='submit']"),
        (By.CSS_SELECTOR, "input[type='submit']"),
        (By.XPATH, "//button[contains(translate(.,'MAP','map'),'map') or contains(translate(.,'SUBMIT','submit'),'submit') or contains(translate(.,'SEARCH','search'),'search')]"),
    ]
    try:
        for sel in input_selectors:
            try:
                field = wait.until(EC.presence_of_element_located(sel))
                if field:
                    break
            except Exception:
                continue
        else:
            return None, None
        btn = None
        for sel in btn_selectors:
            try:
                btn = driver.find_element(*sel)
                if btn:
                    break
            except Exception:
                continue
        return field, btn
    except Exception:
        return None, None


def process_zip(zipcode, cols, out_path, use_xlsx):
    if stop_event.is_set():
        return
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    zipcode = str(zipcode).strip()
    status = STATUS_ERROR
    error_step = ""
    error_type = ""
    error_message = ""
    result_text = ""
    map_url = ""

    driver = None
    try:
        driver = build_driver()
        driver.set_page_load_timeout(HTTP_TIMEOUT)
        timeout_streak = 0

        attempts = 0
        while attempts < 2 and not stop_event.is_set():
            attempts += 1
            try:
                driver.get(FEDEX_BASE_URL)
                time.sleep(random.uniform(HUMAN_DELAY_RANGE_MS[0], HUMAN_DELAY_RANGE_MS[1]) / 1000.0)
                wait = WebDriverWait(driver, 15)
                field, btn = ensure_fedex_ui(driver, wait)
                if not field:
                    error_step, error_type, error_message = "FIND_INPUT", "NO_INPUT", "ZIP input not found"
                    save_debug(driver, zipcode, "no_input")
                    break
                try:
                    field.clear()
                    field.send_keys(zipcode)
                except Exception:
                    error_step, error_type, error_message = "FILL_INPUT", "FAILED", "Could not enter ZIP"
                    break
                try:
                    if btn:
                        btn.click()
                    else:
                        field.submit()
                except Exception:
                    try:
                        field.submit()
                    except Exception:
                        pass

                prev_marker = ""
                try:
                    prev_marker = driver.find_element(By.TAG_NAME, "body").text
                except Exception:
                    prev_marker = ""

                state = wait_fedex_result_or_terminal(driver, prev_marker, timeout=WAIT_TIMEOUT)
                time.sleep(random.uniform(3000, 4000) / 1000.0)

                if state.get("status") == STATUS_NO_DATA:
                    status = STATUS_NO_DATA
                    error_step, error_type = "WAIT_RESULT", "NO_DATA"
                    result_text = "No results found"
                    error_message = "No transit map available for this ZIP code"
                    break
                if state.get("status") == STATUS_TIMEOUT:
                    timeout_streak += 1
                    if timeout_streak >= 3:
                        log("[WAIT] Multiple timeouts; pausing 60s and reloading base page.")
                        save_debug(driver, zipcode, "timeout")
                        time.sleep(60)
                        timeout_streak = 0
                    continue

                map_url = state.get("map_src", "")
                if map_url:
                    status = STATUS_OK
                    result_text = state.get("result_text", "")
                else:
                    status = STATUS_NO_DATA
                    error_step, error_type = "WAIT_RESULT", "NO_DATA"
                    result_text = "No results found"
                    error_message = "No transit map available for this ZIP code"
                break
            except Exception as e:
                error_step, error_type, error_message = "UNEXPECTED", "EXCEPTION", str(e)
                save_debug(driver, zipcode, "unexpected")
                break

        # If loop ended without setting a final status, mark timeout
        if status not in (STATUS_OK, STATUS_NO_DATA) and not error_step:
            status = STATUS_TIMEOUT
            error_step, error_type, error_message = "WAIT_RESULT", "RESULT_TIMEOUT", "Timed out waiting for results"
        # If marked OK but no map URL, convert to NO_DATA
        if status == STATUS_OK and not map_url:
            status = STATUS_NO_DATA
            error_step, error_type = "WAIT_RESULT", "NO_DATA"
            result_text = "No results found"
            error_message = "No transit map available for this ZIP code"

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    row = {
        "ZIP": zipcode,
        "STATUS": status,
        "ERROR_STEP": error_step,
        "ERROR_TYPE": error_type,
        "ERROR_MESSAGE": error_message,
        "RESULT_TEXT": result_text,
        "MAP_URL": map_url,
        "PAGE_URL": FEDEX_BASE_URL,
    }
    append_row_any(out_path, cols, row, use_xlsx)
    with stats_lock:
        stats["processed"] += 1
        if status != STATUS_OK:
            stats["skipped"] += 1
    pause_if_needed()
    time.sleep(random.uniform(HUMAN_DELAY_RANGE_MS[0], HUMAN_DELAY_RANGE_MS[1]) / 1000.0)


def run_batch(input_path, output_path):
    stop_event.clear()
    zips = read_input_rows(input_path)
    if not zips:
        raise RuntimeError("No ZIP codes found in input.")
    with stats_lock:
        stats["total"] = len(zips)
        stats["processed"] = 0
        stats["skipped"] = 0
    use_xlsx = pathlib.Path(input_path).suffix.lower() == ".xlsx"
    out_path = pathlib.Path(output_path)
    if use_xlsx and out_path.suffix.lower() != ".xlsx":
        out_path = out_path.with_suffix(".xlsx")
    if (not use_xlsx) and out_path.suffix.lower() != ".csv":
        out_path = out_path.with_suffix(".csv")
    out_path = str(out_path.resolve())
    cols = ["ZIP", "STATUS", "ERROR_STEP", "ERROR_TYPE", "ERROR_MESSAGE", "RESULT_TEXT", "MAP_URL", "PAGE_URL"]
    ensure_header_any(out_path, cols, use_xlsx)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(process_zip, z, cols, out_path, use_xlsx): z for z in zips}
        for _ in as_completed(futs):
            if stop_event.is_set():
                break
    log("Done.")


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        inp = sys.argv[1]
        outp = sys.argv[2] if len(sys.argv) > 2 else default_output_for_input(inp)
        run_batch(inp, outp)
    else:
        import tkinter as tk
        from tkinter import filedialog, messagebox

        app = tk.Tk()
        app.title("FedEx Freight Transit Map Scraper")
        app.geometry("760x280")
        app.resizable(False, False)
        app.columnconfigure(0, weight=1)
        app.rowconfigure(0, weight=1)

        input_var = tk.StringVar()
        output_var = tk.StringVar()
        status_var = tk.StringVar(value="Processed: 0 | Skipped: 0")
        worker_thread_global = None

        def choose_input():
            path = filedialog.askopenfilename(
                title="Select input file",
                filetypes=[("CSV/XLSX", "*.csv;*.xlsx;*.xls"), ("All files", "*.*")],
            )
            if path:
                input_var.set(path)
                if not output_var.get():
                    output_var.set(default_output_for_input(path))

        def choose_output():
            path = filedialog.asksaveasfilename(
                title="Select output file",
                defaultextension=".csv",
                filetypes=[("CSV/XLSX", "*.csv;*.xlsx;*.xls"), ("All files", "*.*")],
            )
            if path:
                output_var.set(path)

        def refresh_status():
            with stats_lock:
                proc = stats.get("processed", 0)
                skip = stats.get("skipped", 0)
                total = stats.get("total", 0)
            total_txt = f"/{total}" if total else ""
            status_var.set(f"Processed: {proc}{total_txt} | Skipped: {skip}")
            if not stop_event.is_set():
                app.after(500, refresh_status)

        def start():
            global worker_thread_global
            if worker_thread_global and worker_thread_global.is_alive():
                messagebox.showinfo("Scraper", "Job already running.")
                return
            inp = input_var.get().strip()
            outp = output_var.get().strip()
            if not inp:
                messagebox.showerror("Scraper", "Please select an input file.")
                return
            if not outp:
                messagebox.showerror("Scraper", "Please select an output file.")
                return
            with stats_lock:
                stats["processed"] = 0
                stats["skipped"] = 0
                stats["total"] = 0
            stop_event.clear()

            def run_job():
                try:
                    run_batch(inp, outp)
                    messagebox.showinfo("Scraper", f"Done. Output: {outp}")
                except Exception as e:
                    messagebox.showerror("Scraper", f"Error: {e}")

            worker_thread_global = threading.Thread(target=run_job, daemon=True)
            worker_thread_global.start()

        def stop():
            stop_event.set()
            messagebox.showinfo("Scraper", "Stop requested. Current run will halt shortly.")

        def resume():
            stop_event.clear()
            messagebox.showinfo("Scraper", "Resume requested. Start a new job to continue.")

        def exit_app():
            stop_event.set()
            app.destroy()

        main = tk.Frame(app)
        main.grid(row=0, column=0, sticky="nsew", padx=16, pady=16)
        main.columnconfigure(0, weight=0)
        main.columnconfigure(1, weight=1)
        main.columnconfigure(2, weight=0)

        tk.Label(main, text="Input File:").grid(row=0, column=0, sticky="w", padx=(0, 10), pady=8)
        tk.Entry(main, textvariable=input_var, width=50).grid(row=0, column=1, sticky="ew")
        tk.Button(main, text="Browse", command=choose_input, width=10).grid(row=0, column=2, padx=(10, 0))

        tk.Label(main, text="Output File:").grid(row=1, column=0, sticky="w", padx=(0, 10), pady=8)
        tk.Entry(main, textvariable=output_var, width=50).grid(row=1, column=1, sticky="ew")
        tk.Button(main, text="Browse", command=choose_output, width=10).grid(row=1, column=2, padx=(10, 0))

        btn_frame = tk.Frame(main)
        btn_frame.grid(row=2, column=0, columnspan=3, pady=14, sticky="ew")
        btn_frame.columnconfigure((0, 1, 2, 3), weight=1, uniform="btns")
        tk.Button(btn_frame, text="Start", command=start, width=12).grid(row=0, column=0, padx=6)
        tk.Button(btn_frame, text="Stop", command=stop, width=12).grid(row=0, column=1, padx=6)
        tk.Button(btn_frame, text="Resume", command=resume, width=12).grid(row=0, column=2, padx=6)
        tk.Button(btn_frame, text="Exit", command=exit_app, width=12).grid(row=0, column=3, padx=6)

        tk.Label(main, textvariable=status_var, font=("Helvetica", 10, "bold")).grid(
            row=3, column=0, columnspan=3, pady=8, sticky="w"
        )

        refresh_status()
        app.mainloop()
