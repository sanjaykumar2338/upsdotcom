import os
import sys
import time
import random
import csv
import pathlib
import threading
import re
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup

try:
    import openpyxl
except ImportError:
    openpyxl = None

# Optional readers/writers to support additional formats
try:
    import xlrd  # noqa: F401
    import pyxlsb  # noqa: F401
    from odf import opendocument  # noqa: F401
    import lxml  # noqa: F401
except Exception:
    pass

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
INPUT_EXT_MAP = {
    ".xlsx": "xlsx",
    ".xlsm": "xlsx",
    ".xltx": "xlsx",
    ".xltm": "xlsx",
    ".xlam": "xlsx",
    ".xls": "xls",
    ".xlt": "xls",
    ".xlsb": "xlsb",
    ".ods": "ods",
    ".csv": "csv",
    ".tsv": "tsv",
    ".txt": "txt",
    ".prn": "prn",
    ".json": "json",
    ".xml": "xml",
    ".slk": "slk",
}
OUTPUT_EXT_MAP = {
    ".xlsx": "xlsx",
    ".xlsm": "xlsx",
    ".xltx": "xlsx",
    ".xltm": "xlsx",
    ".xlam": "xlsx",
    ".xls": "xls",
    ".xlt": "xls",
    ".xlsb": "xlsb",
    ".ods": "ods",
    ".csv": "csv",
    ".tsv": "tsv",
    ".txt": "txt",
    ".prn": "prn",
    ".json": "json",
    ".xml": "xml",
    ".slk": "slk",
}

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
results_rows = []
results_lock = threading.Lock()
NORMALIZE_TESTS = {
    "00501": "00501",
    "501": "00501",
    "0501": "00501",
    "501.0": "00501",
}

def default_output_for_input(path: str) -> str:
    p = pathlib.Path(path)
    ext = p.suffix
    return str(p.with_name(f"result_{p.stem}{ext if ext else '.csv'}"))


# ------------ Helpers ------------
def log(msg):
    print(msg, flush=True)


def collapse(txt: str) -> str:
    return " ".join((txt or "").split())


def ensure_dir(p: pathlib.Path):
    p.parent.mkdir(parents=True, exist_ok=True)


def normalize_zip(val) -> str:
    s = "" if val is None else str(val).strip()
    if not s:
        return ""
    # Strip trailing .0 or .00 from Excel/CSV numeric coercion
    if re.fullmatch(r"\d+\.0+", s):
        s = s.split(".")[0]
    # Remove non-digits
    s = re.sub(r"[^\d]", "", s)
    if not s:
        return ""
    if len(s) < 5:
        s = s.zfill(5)
    return s


def resolve_ext_format(ext: str, mapping) -> str:
    return mapping.get(ext.lower(), "")


def read_input_rows(path: str):
    import pandas as pd

    p = pathlib.Path(path)
    ext = p.suffix.lower()
    fmt = resolve_ext_format(ext, INPUT_EXT_MAP)
    if not fmt:
        raise RuntimeError("Unsupported input type.")

    def first_col_to_zips(df):
        vals = []
        for v in df.iloc[:, 0].tolist():
            if v is None:
                continue
            nz = normalize_zip(v)
            if nz:
                vals.append(nz)
        # drop header row if it is non-numeric text
        if vals and not vals[0].isdigit():
            vals = vals[1:]
        return vals

    try:
        if fmt in ("csv", "tsv", "txt", "prn"):
            sep = "\t" if fmt in ("tsv", "prn") else None
            df = pd.read_csv(path, sep=sep, header=None, engine="python", dtype=str)
            return first_col_to_zips(df)
        if fmt in ("xlsx", "xls", "xlam", "xlsm", "xltx", "xltm"):
            engine = "xlrd" if fmt == "xls" else None
            try:
                df = pd.read_excel(path, engine=engine, dtype=str)
                return first_col_to_zips(df)
            except Exception as e:
                if fmt == "xlam":
                    try:
                        df = pd.read_csv(path, sep=None, engine="python", header=None, dtype=str)
                        return first_col_to_zips(df)
                    except Exception:
                        raise RuntimeError(f"xlam file could not be read as workbook ({e})")
                raise
        if fmt == "xlsb":
            df = pd.read_excel(path, engine="pyxlsb", dtype=str)
            return first_col_to_zips(df)
        if fmt == "ods":
            df = pd.read_excel(path, engine="odf", dtype=str)
            return first_col_to_zips(df)
        if fmt == "json":
            df = pd.read_json(path, dtype=str)
            # If JSON is a list of values, pandas will create a single column named 0
            return first_col_to_zips(df)
        if fmt == "xml":
            df = pd.read_xml(path)
            return first_col_to_zips(df)
        if fmt == "slk":
            # basic SYLK parsing: keep numeric tokens after 'K'
            vals = []
            with open(path, encoding="utf-8-sig", errors="ignore") as f:
                for line in f:
                    if line.startswith("ID") or line.startswith("E"):
                        continue
                    parts = line.strip().split(";")
                    for part in parts:
                        if part.startswith("K"):
                            candidate = part[1:].strip().strip('"')
                            if candidate:
                                vals.append(candidate)
            if vals and not vals[0].isdigit():
                vals = vals[1:]
            return vals
    except Exception as e:
        raise RuntimeError(f"Could not read input file: {e}")
    raise RuntimeError("Unsupported input type.")


def write_output_file(path: str, cols, rows):
    import pandas as pd

    p = pathlib.Path(path)
    ext = p.suffix.lower()
    fmt = resolve_ext_format(ext, OUTPUT_EXT_MAP) or "csv"
    out_path = p

    # handle formats that are not directly writable
    if fmt in ("xlsb", "slk"):
        log(f"[WARN] Writing {fmt} is not fully supported. Saving as .xlsx instead.")
        out_path = p.with_suffix(".xlsx")
        fmt = "xlsx"

    def read_existing_dataframe():
        try:
            if not out_path.exists() or out_path.stat().st_size == 0:
                return pd.DataFrame(columns=cols)
            if fmt in ("csv", "tsv", "txt", "prn"):
                sep = "\t" if fmt in ("tsv", "prn") else None
                return pd.read_csv(out_path, sep=sep, dtype=str)
            if fmt in ("xlsx", "xlsm", "xltx", "xltm", "xlam"):
                return pd.read_excel(out_path, dtype=str)
            if fmt in ("xls",):
                return pd.read_excel(out_path, engine="xlrd", dtype=str)
            if fmt == "ods":
                return pd.read_excel(out_path, engine="odf", dtype=str)
            if fmt == "json":
                return pd.read_json(out_path, dtype=str)
            if fmt == "xml":
                return pd.read_xml(out_path)
        except Exception:
            return pd.DataFrame(columns=cols)
        return pd.DataFrame(columns=cols)

    existing = read_existing_dataframe()
    new_df = pd.DataFrame(rows, columns=cols)
    if not existing.empty:
        df = pd.concat([existing, new_df], ignore_index=True)
    else:
        df = new_df

    if fmt == "csv":
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
    elif fmt == "tsv":
        df.to_csv(out_path, index=False, encoding="utf-8-sig", sep="\t")
    elif fmt in ("txt", "prn"):
        df.to_csv(out_path, index=False, encoding="utf-8-sig", sep="\t")
    elif fmt == "json":
        df.to_json(out_path, orient="records", indent=2, force_ascii=False)
    elif fmt == "xml":
        df.to_xml(out_path, index=False, root_name="rows", row_name="row", encoding="utf-8")
    elif fmt in ("xlsx", "xlsm", "xltx", "xltm", "xlam"):
        df.to_excel(out_path, index=False, engine="openpyxl")
    elif fmt == "xls":
        df.to_excel(out_path, index=False, engine="xlwt")
    elif fmt == "ods":
        df.to_excel(out_path, index=False, engine="odf")
    else:
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
    return str(out_path)


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
        log(f"[PAUSE] Processed {PAUSE_EVERY} records - waiting {PAUSE_SECONDS} seconds to avoid blocking")
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


def process_zip(zipcode, cols):
    if stop_event.is_set():
        return
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    zipcode = normalize_zip(zipcode)
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
    with results_lock:
        results_rows.append(row)
    with stats_lock:
        stats["processed"] += 1
        if status != STATUS_OK:
            stats["skipped"] += 1
    pause_if_needed()
    time.sleep(random.uniform(HUMAN_DELAY_RANGE_MS[0], HUMAN_DELAY_RANGE_MS[1]) / 1000.0)


def run_batch(input_path, output_path):
    stop_event.clear()
    with results_lock:
        results_rows.clear()
    # Quick sanity check on normalize_zip behavior
    for k, expected in NORMALIZE_TESTS.items():
        got = normalize_zip(k)
        if got != expected:
            log(f"[WARN] normalize_zip mismatch for {k}: got {got}, expected {expected}")
    zips = read_input_rows(input_path)
    if not zips:
        raise RuntimeError("No ZIP codes found in input.")
    with stats_lock:
        stats["total"] = len(zips)
        stats["processed"] = 0
        stats["skipped"] = 0
    out_path_obj = pathlib.Path(output_path)
    if not resolve_ext_format(out_path_obj.suffix, OUTPUT_EXT_MAP):
        log(f"[WARN] Output type {out_path_obj.suffix} not supported. Defaulting to .csv")
        out_path_obj = out_path_obj.with_suffix(".csv")
    out_path = str(out_path_obj.resolve())
    cols = ["ZIP", "STATUS", "ERROR_STEP", "ERROR_TYPE", "ERROR_MESSAGE", "RESULT_TEXT", "MAP_URL", "PAGE_URL"]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(process_zip, z, cols): z for z in zips}
        for _ in as_completed(futs):
            if stop_event.is_set():
                break
    with results_lock:
        rows_copy = list(results_rows)
    actual_out = write_output_file(out_path, cols, rows_copy)
    log(f"Done. Output saved to {actual_out}")
    return actual_out


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        inp = sys.argv[1]
        outp = sys.argv[2] if len(sys.argv) > 2 else default_output_for_input(inp)
        final_out = run_batch(inp, outp)
        log(f"Output saved to {final_out}")
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
                filetypes=[
                    (
                        "Data files",
                        "*.xlsx;*.xlsm;*.xlsb;*.xltx;*.xltm;*.xlam;*.xls;*.xlt;*.ods;*.csv;*.tsv;*.txt;*.prn;*.json;*.xml;*.slk",
                    ),
                    ("All files", "*.*"),
                ],
            )
            if path:
                input_var.set(path)
                if not output_var.get():
                    output_var.set(default_output_for_input(path))

        def choose_output():
            path = filedialog.asksaveasfilename(
                title="Select output file",
                defaultextension=".csv",
                filetypes=[
                    (
                        "Data files",
                        "*.xlsx;*.xlsm;*.xlsb;*.xltx;*.xltm;*.xlam;*.xls;*.xlt;*.ods;*.csv;*.tsv;*.txt;*.prn;*.json;*.xml;*.slk",
                    ),
                    ("All files", "*.*"),
                ],
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
                    actual_out = run_batch(inp, outp)
                    messagebox.showinfo("Scraper", f"Done. Output: {actual_out}")
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
