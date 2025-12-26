# run_option_two_concurrent.py
import csv, re, time, random, sys, os, pathlib, threading, queue, requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import errno

INPUT_FILE  = sys.argv[1] if len(sys.argv) > 1 else "TEST123.csv"
OUTPUT_NAME = sys.argv[2] if len(sys.argv) > 2 else "OUTPUT2.csv"
CONCURRENCY = 12
MIN_MS, MAX_MS, TIMEOUT = 200, 500, 30
USER_AGENT = "Mozilla/5.0 (OptionTwoHTTP/1.0)"
BASE = "https://ziplook.house.gov/htbin/findrep_house?ZIP={zip}"

NOT_FOUND_RE = re.compile(r"The ZIP code\s+(\d{5})\s+was not found", re.I)

def _space(s): return " ".join((s or "").split())

def read_zips(path):
    out, first = [], True
    with open(path, encoding="utf-8-sig") as f:
        for row in csv.reader(f):
            if not row: continue
            v = row[0].strip()
            if first and not v.isdigit():
                first = False; continue
            first = False
            if v: out.append(v)
    return out

def extract_not_found_or_overlap(soup):
    """
    Returns one of:
      - 'The ZIP code 12345 was not found.'
      - 'The information you provided (Zip code: 12345 ) overlaps multiple congressional districts.'
      - ''  (if neither applies)
    """
    # Prefer the small results panel if present, else use full page text
    panel = soup.select_one(".relatedContent")
    for node in (panel, soup):
        if not node:
            continue
        txt = " ".join(node.get_text(" ", strip=True).split())

        # 1) Not found (strip any trailing 'Please try again.' variants)
        m = NOT_FOUND_RE.search(txt)
        if m:
            zipcode = m.group(1)
            return f"The ZIP code {zipcode} was not found."

        # 2) Overlap (keep the site's sentence as-is)
        overlap = re.search(
            r"The information you provided\s*\(Zip code:\s*\d{5}\s*\)\s*overlaps multiple congressional districts\.",
            txt,
            re.I,
        )
        if overlap:
            return overlap.group(0).strip()

    return ""

def derive_district_from_info(soup):
    txt = _space(soup.get_text(" ", strip=True))
    m = re.search(r"is located in the\s+(\d+)(?:st|nd|rd|th)\s+Congressional district of\s+([A-Za-z .-]+)\.", txt, re.I)
    if m: return f"{m.group(2).strip()} District {m.group(1)}"
    m = re.search(r"is located in the\s+At[-\s]?Large\s+Congressional district of\s+([A-Za-z .-]+)\.", txt, re.I)
    if m: return f"{m.group(1).strip()} At-Large"
    return ""

def parse_page(html, zipcode):
    soup = BeautifulSoup(html, "html.parser")
    msg = extract_not_found_or_overlap(soup)
    reps, fallback = [], derive_district_from_info(soup)
    for p in soup.select("#PossibleReps p.rep"):
        a = p.select_one("a")
        name = a.get_text(strip=True) if a else ""
        parts = [t for t in p.stripped_strings if t and t != name]
        party = parts[0] if len(parts) >= 1 else ""
        district = ""
        for t in parts[1:]:
            if "District" in t or "At-Large" in t:
                district = t; break
        reps.append({"name": name, "party": party, "district": district or fallback})
    if not msg and len(reps) == 1:
        msg = f"Zip code: {zipcode}"
    return msg, reps

# ---------- Thread-safe I/O helpers ----------
write_lock = threading.Lock()

def ensure_header(path, max_reps):
    cols = ["ZIPCODES", "OVERLAPS MULTIPLE"] + \
           [f"{k}{i}" for i in range(1, max_reps+1) for k in ("REPRESENTATIVE","PARTY","DISTRICT")]
    if (not os.path.exists(path)) or os.path.getsize(path) == 0:
        with open(path, "w", newline="", encoding="utf-8") as fp:
            csv.writer(fp).writerow(cols)
    return cols

def append_row(path, cols, row, tries=20, base_sleep=0.25):
    for i in range(tries):
        try:
            with write_lock, open(path, "a", newline="", encoding="utf-8") as fp:
                w = csv.DictWriter(fp, fieldnames=cols)
                w.writerow(row)
                fp.flush(); os.fsync(fp.fileno())
            return
        except PermissionError as e:
            # Windows file is locked (Excel, AV, etc). Backoff and retry.
            time.sleep(base_sleep * (1 + i/3.0))
    # Fallback: write to a uniquely named file so no data is lost
    alt = f"{os.path.splitext(path)[0]}_{os.getpid()}_{int(time.time())}.csv"
    with write_lock, open(alt, "a", newline="", encoding="utf-8") as fp:
        w = csv.DictWriter(fp, fieldnames=cols)
        if fp.tell() == 0:
            w.writeheader()
        w.writerow(row)

# ---------- Worker ----------
thread_local = threading.local()
def get_session():
    if not hasattr(thread_local, "sess"):
        s = requests.Session()
        s.headers.update({"User-Agent": USER_AGENT})
        thread_local.sess = s
    return thread_local.sess

def fetch_and_parse(zipcode):
    sess = get_session()
    url = BASE.format(zip=zipcode)
    try:
        r = sess.get(url, timeout=TIMEOUT)
        if not r.ok:
            return zipcode, f"http_{r.status_code}", []
        msg, reps = parse_page(r.text, zipcode)
        return zipcode, msg, reps
    except Exception as e:
        return zipcode, f"exception:{str(e)[:160]}", []
    finally:
        time.sleep(random.uniform(MIN_MS/1000.0, MAX_MS/1000.0))

# ---------- Main ----------
def main():
    input_path = pathlib.Path(INPUT_FILE).resolve()
    out_path   = str((input_path.parent / OUTPUT_NAME).resolve())
    zips = read_zips(str(input_path))

    # Pass 1: concurrently probe to discover the maximum reps needed
    max_reps = 0
    results  = {}
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futs = {ex.submit(fetch_and_parse, z): z for z in zips}
        for fut in as_completed(futs):
            z, msg, reps = fut.result()
            results[z] = (msg, reps)
            max_reps = max(max_reps, len(reps))

    cols = ensure_header(out_path, max_reps)

    # Pass 2: write rows immediately (order doesn’t matter; we’re appending)
    for z in zips:
        msg, reps = results[z]
        row = {"ZIPCODES": z, "OVERLAPS MULTIPLE": msg}
        for i in range(max_reps):
            if i < len(reps):
                row[f"REPRESENTATIVE{i+1}"] = reps[i]["name"]
                row[f"PARTY{i+1}"] = reps[i]["party"]
                row[f"DISTRICT{i+1}"] = reps[i]["district"]
            else:
                row[f"REPRESENTATIVE{i+1}"] = row[f"PARTY{i+1}"] = row[f"DISTRICT{i+1}"] = ""
        append_row(out_path, cols, row)

    print(f"Done: {out_path} (concurrency={CONCURRENCY})")

if __name__ == "__main__":
    main()