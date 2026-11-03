#!/usr/bin/env python3
"""
LeadMiner X v3 — commercial lead extraction (2026-11-03)

Single engine: the official Google Places API (New). The v2 browser
scraper is gone — automating google.com/maps violates Google's Terms of
Service, and no commercial build can ship it. The API path is also the
better economics:

  * Text Search: ~$32 / 1,000 requests (2026 pricing)
  * One request returns up to 20 complete leads  =>  ~$0.002 / lead

What v3 adds over v2:
  * PERSISTENT DEDUP — every lead ever mined is remembered in
    ~/.leadminer/leads.db (keyed by Google place_id, falling back to
    name+address), so re-mining a city never re-pays for the same leads.
  * DEEP SWEEP — a grid search over the location's bounding box
    (Nominatim geocoding) that defeats Google's ~60-result cap per query.
  * SETUP WIZARD + "Test my key" — the 5-minute Google Cloud setup,
    spelled out in the window, with one-click key validation.
  * LICENSE GATE — free trial of 25 leads (counter persisted in the same
    SQLite DB); a Gumroad license key (see license_check.py) unlocks
    unlimited mining.

Design kept from v2: worker-thread execution, queue-based UI logging,
cooperative stop flag, streaming CSV (append + flush per record), phone
normalization, review-count parsing, 429 backoff, paged Text Search with
a single field mask (every lead field in one call, no Place Details).

Setup:
  pip install customtkinter requests
  API key: Google Cloud Console -> enable "Places API (New)" -> create API
  key (billing must be enabled). Paste it in the app or set
  GOOGLE_PLACES_API_KEY.
"""

import csv
import math
import os
import queue
import random
import re
import sqlite3
import threading
import time
import urllib.parse

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    import customtkinter as ctk
    HAS_CTK = True
except ImportError:
    ctk = None
    HAS_CTK = False

import license_check
from license_check import TRIAL_LIMIT

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

APP_NAME = "LeadMiner X v3"

DATA_KEYS = ["Name", "Category", "Rating", "Reviews",
             "Address", "Phone", "Website", "Search Location"]

APP_DIR = os.path.join(os.path.expanduser("~"), ".leadminer")
DB_PATH = os.path.join(APP_DIR, "leads.db")

PLACES_API_URL = "https://places.googleapis.com/v1/places:searchText"
# One field mask pulls every lead field in a single call — no separate
# Place Details requests needed. places.id keys the persistent dedup store
# (threaded through internally; it is never a CSV column).
PLACES_FIELD_MASK = (
    "places.id,places.displayName,places.formattedAddress,"
    "places.nationalPhoneNumber,places.internationalPhoneNumber,"
    "places.websiteUri,places.rating,places.userRatingCount,"
    "places.businessStatus,places.types,nextPageToken"
)

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_USER_AGENT = "LeadMinerX/3.0"

GRID_CELL_DEGREES = 0.03     # ~3.3 km cells
GRID_MAX_CELLS = 16          # 4 x 4 — cells grow if the bbox needs more

STATUS_LABELS = {
    "OPERATIONAL": "Operational",
    "CLOSED_TEMPORARILY": "Temporarily Closed",
    "CLOSED_PERMANENTLY": "Permanently Closed",
}


# ---------------------------------------------------------------------------
# Persistent store: cross-run dedup + trial counter (SQLite)
# ---------------------------------------------------------------------------

class LeadStore:
    """Owns ~/.leadminer/leads.db.

    seen_leads remembers every lead ever written to a CSV, keyed by Google
    place_id when the API provided one, else by normalized (name, address).
    meta holds the free-trial counter so the 25-lead trial survives
    restarts. All access is serialized with a lock: the worker thread
    writes while the GUI thread reads counts.
    """

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self._lock = threading.Lock()
        directory = os.path.dirname(db_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        with self._lock, self._conn:
            self._conn.execute(
                """CREATE TABLE IF NOT EXISTS seen_leads (
                       place_id    TEXT,
                       name_key    TEXT NOT NULL,
                       address_key TEXT NOT NULL,
                       name        TEXT,
                       address     TEXT,
                       first_seen  TEXT NOT NULL
                   )""")
            # A lead is found by place_id OR by (name, address): a business
            # first seen under the fallback key must still dedup when a
            # later run sees it with a place_id, and vice versa.
            self._conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_seen_place_id "
                "ON seen_leads(place_id) WHERE place_id IS NOT NULL")
            self._conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_seen_name_addr "
                "ON seen_leads(name_key, address_key)")
            self._conn.execute(
                """CREATE TABLE IF NOT EXISTS meta (
                       key   TEXT PRIMARY KEY,
                       value TEXT NOT NULL
                   )""")

    # -- key normalization --------------------------------------------------
    @staticmethod
    def _name_key(name):
        return (name or "").strip().lower()

    @staticmethod
    def _address_key(address):
        return (address or "").strip().lower()

    # -- dedup ----------------------------------------------------------------
    def contains(self, place_id, name, address):
        """True if this lead was written by ANY previous run (or this one)."""
        name_key = self._name_key(name)
        address_key = self._address_key(address)
        with self._lock:
            if place_id:
                row = self._conn.execute(
                    "SELECT 1 FROM seen_leads WHERE place_id = ? "
                    "OR (name_key = ? AND address_key = ?) LIMIT 1",
                    (place_id, name_key, address_key)).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT 1 FROM seen_leads "
                    "WHERE name_key = ? AND address_key = ? LIMIT 1",
                    (name_key, address_key)).fetchone()
        return row is not None

    def add(self, place_id, name, address):
        """Remember a lead as seen. Safe against races (INSERT OR IGNORE)."""
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO seen_leads "
                "(place_id, name_key, address_key, name, address, first_seen) "
                "VALUES (?, ?, ?, ?, ?, datetime('now'))",
                (place_id or None, self._name_key(name),
                 self._address_key(address), name, address))

    def count(self):
        """Total leads in history (all runs, all time)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM seen_leads").fetchone()
        return row[0]

    def reset(self):
        """Forget every seen lead. The trial counter is NOT reset —
        clearing history must not mint a fresh free trial."""
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM seen_leads")

    # -- trial counter --------------------------------------------------------
    def trial_used(self):
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key = 'trial_used'").fetchone()
        return int(row[0]) if row else 0

    def increment_trial(self):
        """Add one consumed trial lead; returns the new total."""
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO meta (key, value) VALUES ('trial_used', '1') "
                "ON CONFLICT(key) DO UPDATE SET value = "
                "CAST(value AS INTEGER) + 1")
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key = 'trial_used'").fetchone()
        return int(row[0])


# ---------------------------------------------------------------------------
# Shared pipeline: logging, pacing, streaming CSV, dedup, trial gate
# ---------------------------------------------------------------------------

class LeadPipeline:
    """Turns records into CSV rows. The engine only produces records.

    Dedup is persistent (LeadStore), so a lead is skipped if ANY past run
    already wrote it. Without a license, only TRIAL_LIMIT leads may ever
    be written; the run stops cleanly the moment the trial runs out.
    """

    def __init__(self, log_queue, stop_event, store, licensed):
        self.log_queue = log_queue
        self.stop_event = stop_event
        self.store = store
        self.licensed = licensed
        self._csv_file = None
        self._csv_writer = None
        self.total = 0            # new leads written this run
        self.skipped_seen = 0     # leads skipped as already-seen this run
        self.trial_limit_hit = False

    # -- logging / pacing -------------------------------------------------
    def log(self, message):
        self.log_queue.put(message)

    def pace(self, lo=1.0, hi=2.2):
        deadline = time.time() + random.uniform(lo, hi)
        while time.time() < deadline:
            if self.stop_event.is_set():
                return
            time.sleep(0.1)

    # -- trial gate ---------------------------------------------------------
    def trial_exhausted(self):
        """True when an unlicensed run has no trial leads left."""
        return (not self.licensed
                and self.store.trial_used() >= TRIAL_LIMIT)

    # -- streaming CSV ----------------------------------------------------
    def open_csv(self, path):
        already_has_data = os.path.exists(path) and os.path.getsize(path) > 0
        self._csv_file = open(path, "a", newline="", encoding="utf-8")
        self._csv_writer = csv.DictWriter(self._csv_file, fieldnames=DATA_KEYS)
        if not already_has_data:
            self._csv_writer.writeheader()
            self._csv_file.flush()

    def close_csv(self):
        if self._csv_file:
            self._csv_file.close()
            self._csv_file = None

    def add_record(self, record, place_id=None):
        """Write a lead if it is new. Returns True if written.

        Skips (and counts) leads already in the persistent store. On an
        unlicensed install each written lead consumes one trial lead; when
        the trial runs out the run stops cleanly via the stop flag.
        """
        name = record.get("Name", "")
        if not name:
            return False
        if self.store.contains(place_id, name, record.get("Address", "")):
            self.skipped_seen += 1
            return False
        if not self.licensed:
            if self.store.trial_used() >= TRIAL_LIMIT:
                self.trial_limit_hit = True
                self.log("[!] Free trial limit reached — "
                         "activate to continue.")
                self.stop_event.set()
                return False
            self.store.increment_trial()
        self.store.add(place_id, name, record.get("Address", ""))
        # Single writer thread => appends are effectively atomic; flush so a
        # crash loses at most the in-flight row, never the run.
        self._csv_writer.writerow(record)
        self._csv_file.flush()
        self.total += 1
        self.log(f"[✓] Scraped ({self.total}): {name.strip()}")
        return True

    # -- parsing helpers --------------------------------------------------
    @staticmethod
    def parse_review_count(text):
        """'240' -> '240', '1.2K' -> '1200', '3M' -> '3000000'."""
        if text is None:
            return "N/A"
        if isinstance(text, (int, float)):
            return str(int(text))
        text = str(text).strip().upper().replace(",", "")
        m = re.fullmatch(r"([\d.]+)([KM]?)", text)
        if not m:
            digits = re.sub(r"\D", "", text)
            return digits or "N/A"
        num, suffix = float(m.group(1)), m.group(2)
        return str(int(num * {"": 1, "K": 1_000, "M": 1_000_000}[suffix]))

    @staticmethod
    def normalize_phone(raw):
        if not raw or raw == "N/A":
            return "N/A"
        digits = re.sub(r"\D", "", str(raw))
        if len(digits) == 10:
            digits = "1" + digits
        if len(digits) == 11 and digits.startswith("1"):
            return "+" + digits
        return "+" + digits if digits else "N/A"


# ---------------------------------------------------------------------------
# Places API helpers: key test + geocoding for the deep sweep
# ---------------------------------------------------------------------------

def _places_error(resp):
    """Extract (status, message) from a Places API error response."""
    try:
        err = resp.json().get("error", {})
        return err.get("status", ""), err.get("message", resp.text[:200])
    except Exception:
        return "", resp.text[:200]


def test_api_key(api_key):
    """Fire ONE minimal Text Search to validate a key.

    Returns (ok, plain_language_message). Costs the user a single
    pageSize-1 request — the cheapest possible proof the key works.
    """
    if not api_key:
        return False, "No API key entered."
    if not HAS_REQUESTS:
        return False, "The 'requests' package is not installed."
    try:
        resp = requests.post(
            PLACES_API_URL,
            headers={
                "Content-Type": "application/json",
                "X-Goog-Api-Key": api_key,
                "X-Goog-FieldMask": "places.displayName",
            },
            json={"textQuery": "coffee in New York, NY", "pageSize": 1},
            timeout=15,
        )
    except requests.RequestException as exc:
        return False, f"Network error — could not reach Google ({exc})."
    if resp.status_code == 200:
        return True, "Key works ✓"
    status, message = _places_error(resp)
    if resp.status_code == 403 or status == "PERMISSION_DENIED":
        return False, ("Key rejected: the key is invalid, 'Places API "
                       "(New)' is not enabled on the project, or billing "
                       "is not enabled. Check the setup steps below.")
    if resp.status_code == 400 or status == "INVALID_ARGUMENT":
        return False, f"Request rejected by Google: {message}"
    if resp.status_code == 429:
        return False, ("Rate limited (429) — the key itself looks valid; "
                       "wait a minute and try again.")
    return False, f"Places API error {resp.status_code}: {message}"


def geocode_bbox(location):
    """Resolve a location string to (min_lat, min_lon, max_lat, max_lon)
    via Nominatim. Returns (bbox, None) or (None, reason_string)."""
    if not HAS_REQUESTS:
        return None, "'requests' package not installed"
    try:
        resp = requests.get(
            NOMINATIM_URL,
            params={"q": location, "format": "json", "limit": 1},
            headers={"User-Agent": NOMINATIM_USER_AGENT},
            timeout=15,
        )
    except requests.RequestException as exc:
        return None, f"network error ({exc})"
    if resp.status_code != 200:
        return None, f"geocoder returned HTTP {resp.status_code}"
    try:
        results = resp.json()
        box = results[0]["boundingbox"]   # [min_lat, max_lat, min_lon, max_lon]
        min_lat, max_lat = float(box[0]), float(box[1])
        min_lon, max_lon = float(box[2]), float(box[3])
    except (ValueError, KeyError, IndexError, TypeError):
        return None, f"no geocoding result for '{location}'"
    return (min_lat, min_lon, max_lat, max_lon), None


def grid_cells(bbox):
    """Split a bbox into cells of ~GRID_CELL_DEGREES, capped at
    GRID_MAX_CELLS (cells grow past 0.03° when the area is large).
    Returns a list of (low_lat, low_lon, high_lat, high_lon)."""
    min_lat, min_lon, max_lat, max_lon = bbox
    lat_span = max(max_lat - min_lat, 0.001)
    lon_span = max(max_lon - min_lon, 0.001)
    # At most sqrt(MAX) cells per axis keeps the total under the cap while
    # honoring the ~0.03° cell size for small areas.
    max_axis = int(math.sqrt(GRID_MAX_CELLS))
    cell_lat = max(GRID_CELL_DEGREES, lat_span / max_axis)
    cell_lon = max(GRID_CELL_DEGREES, lon_span / max_axis)
    n_lat = max(1, math.ceil(lat_span / cell_lat))
    n_lon = max(1, math.ceil(lon_span / cell_lon))
    cells = []
    for i in range(n_lat):
        for j in range(n_lon):
            low_lat = min_lat + i * cell_lat
            low_lon = min_lon + j * cell_lon
            cells.append((low_lat, low_lon,
                          min(low_lat + cell_lat, max_lat),
                          min(low_lon + cell_lon, max_lon)))
    return cells


# ---------------------------------------------------------------------------
# Engine: official Google Places API (New) — the only engine in v3
# ---------------------------------------------------------------------------

class PlacesAPIEngine:
    """Turns each (keyword, location) job into paged Text Search requests.
    One request returns up to 20 places with every field we need.

    Normal mode searches "keyword in location" (Google caps that at ~60
    results). Deep-sweep mode geocodes the location and searches a grid of
    rectangles instead, defeating the cap cell by cell.
    """

    def __init__(self, pipeline, api_key):
        self.p = pipeline
        self.api_key = api_key

    def _request(self, text_query, page_token=None, rectangle=None):
        body = {"textQuery": text_query, "pageSize": 20}
        if page_token:
            body["pageToken"] = page_token
        if rectangle is not None:
            low_lat, low_lon, high_lat, high_lon = rectangle
            body["locationRestriction"] = {
                "rectangle": {
                    "low": {"latitude": low_lat, "longitude": low_lon},
                    "high": {"latitude": high_lat, "longitude": high_lon},
                }
            }
        resp = requests.post(
            PLACES_API_URL,
            headers={
                "Content-Type": "application/json",
                "X-Goog-Api-Key": self.api_key,
                "X-Goog-FieldMask": PLACES_FIELD_MASK,
            },
            json=body,
            timeout=30,
        )
        if resp.status_code == 429:
            self.p.log("[!] Rate limited (429) — backing off 10s.")
            time.sleep(10)
            return self._request(text_query, page_token, rectangle)
        if resp.status_code != 200:
            _, err = _places_error(resp)
            self.p.log(f"[-] Places API error {resp.status_code}: {err}")
            if resp.status_code in (400, 403):
                self.p.log("    Check: API key valid? 'Places API (New)' "
                           "enabled? Billing enabled on the project?")
            return None
        return resp.json()

    @staticmethod
    def _to_record(place, location):
        name = (place.get("displayName") or {}).get("text", "N/A")
        types = place.get("types") or []
        category = types[0].replace("_", " ").title() if types else "N/A"
        status = STATUS_LABELS.get(place.get("businessStatus", ""), "N/A")
        record = {
            "Name": name,
            "Category": category,
            "Rating": str(place.get("rating", "N/A")),
            "Reviews": LeadPipeline.parse_review_count(
                place.get("userRatingCount", "N/A")),
            "Address": place.get("formattedAddress", "N/A"),
            "Phone": LeadPipeline.normalize_phone(
                place.get("nationalPhoneNumber")
                or place.get("internationalPhoneNumber", "N/A")),
            "Website": place.get("websiteUri", "N/A"),
            "Search Location": location,
        }
        return record, place.get("id"), status

    def _search_pages(self, text_query, location_label, rectangle=None):
        """Run one paged Text Search (normal or single grid cell)."""
        page_token, pages = None, 0
        while not self.p.stop_event.is_set() and pages < 60:
            data = self._request(text_query, page_token, rectangle)
            if data is None:
                return                      # error already logged
            for place in data.get("places", []):
                if self.p.stop_event.is_set():
                    break
                record, place_id, status = self._to_record(
                    place, location_label)
                if self.p.add_record(record, place_id) and status != "N/A":
                    self.p.log(f"    └ status: {status}")
            page_token = data.get("nextPageToken")
            pages += 1
            if not page_token:
                break
            self.p.pace(0.2, 0.5)           # gentle on quotas

    def run_job(self, keyword, location):
        query = f"{keyword} in {location}"
        self.p.log(f"[+] API search: '{query}'")
        self._search_pages(query, location)
        self.p.log(f"[+] Finished '{query}'.")

    def run_grid_job(self, keyword, location):
        """Deep sweep: geocode, split the bbox into a grid, search each
        cell with a location-restricted Text Search. Falls back to a
        normal search (logging why) when geocoding fails."""
        bbox, reason = geocode_bbox(location)
        if bbox is None:
            self.p.log(f"[!] Deep sweep unavailable for '{location}': "
                       f"{reason}. Falling back to a standard search.")
            self.run_job(keyword, location)
            return
        cells = grid_cells(bbox)
        self.p.log(f"[+] Deep sweep: '{keyword}' over '{location}' — "
                   f"{len(cells)} grid cell(s).")
        for i, cell in enumerate(cells, start=1):
            if self.p.stop_event.is_set():
                self.p.log("[!] Deep sweep stopped between cells.")
                break
            low_lat, low_lon, high_lat, high_lon = cell
            self.p.log(f"[*] Cell {i}/{len(cells)}: "
                       f"({low_lat:.4f}, {low_lon:.4f}) → "
                       f"({high_lat:.4f}, {high_lon:.4f})")
            self._search_pages(keyword, location, rectangle=cell)
        self.p.log(f"[+] Deep sweep finished for '{keyword}' "
                   f"in '{location}'.")

    def run(self, jobs, output_csv, deep_sweep=False):
        if not HAS_REQUESTS:
            self.p.log("[-] The 'requests' package is required: "
                       "pip install requests")
            return
        if not self.api_key:
            self.p.log("[-] No API key. Paste one in the app or set "
                       "GOOGLE_PLACES_API_KEY.")
            return
        if self.p.trial_exhausted():
            self.p.log("[!] Free trial limit reached — "
                       "activate to continue.")
            return
        self.p.open_csv(output_csv)
        try:
            for keyword, location in jobs:
                if self.p.stop_event.is_set():
                    self.p.log("[!] Stopping before next job.")
                    break
                if deep_sweep:
                    self.run_grid_job(keyword, location)
                else:
                    self.run_job(keyword, location)
        finally:
            self.p.close_csv()
        self.p.log(f"[✓] Done. {self.p.total} new lead(s) streamed to "
                   f"{output_csv}")
        self.p.log(f"[*] {self.p.skipped_seen} already-seen skipped "
                   f"(in your history from earlier runs).")
        self.p.log(f"[*] Lead history now holds "
                   f"{self.p.store.count()} lead(s) total.")


# ---------------------------------------------------------------------------
# GUI (only defined when customtkinter is importable, so the non-GUI core
# above can be imported headlessly for tests and tooling)
# ---------------------------------------------------------------------------

if HAS_CTK:
    from tkinter import messagebox

    class LeadMinerApp(ctk.CTk):
        def __init__(self):
            super().__init__()
            self.title(f"{APP_NAME} — Lead Extraction System")
            self.geometry("820x920")

            self.log_queue = queue.Queue()
            # (kind, payload) updates from worker threads -> UI thread
            self.status_queue = queue.Queue()
            self.stop_event = threading.Event()
            self.worker_thread = None

            self.store = LeadStore()
            self.licensed = False

            self._build_ui()
            self._refresh_counts()
            # Resolve any stored license in the background (may re-verify
            # with Gumroad if the cached check is older than 7 days).
            threading.Thread(target=self._license_state_worker,
                             daemon=True).start()
            self.after(100, self.poll_queues)

        # -- layout -------------------------------------------------------
        def _build_ui(self):
            ctk.CTkLabel(
                self,
                text="Official Google Places API engine — every lead "
                     "field in one call (~$0.002/lead).",
                text_color="#2e7d32").pack(pady=(10, 2), padx=12)

            # -- API key --------------------------------------------------
            keyf = ctk.CTkFrame(self)
            keyf.pack(pady=4, padx=12, fill="x")
            ctk.CTkLabel(keyf, text="Places API key:").grid(
                row=0, column=0, padx=8, pady=6)
            self.api_key_entry = ctk.CTkEntry(
                keyf, width=340, show="•",
                placeholder_text="or set GOOGLE_PLACES_API_KEY env var")
            self.api_key_entry.grid(row=0, column=1, padx=8, pady=6)
            preset = os.environ.get("GOOGLE_PLACES_API_KEY", "")
            if preset:
                self.api_key_entry.insert(0, preset)
            ctk.CTkButton(keyf, text="Test my key", width=110,
                          command=self.test_key).grid(
                              row=0, column=2, padx=8, pady=6)
            self.key_status_label = ctk.CTkLabel(keyf, text="Key not tested.")
            self.key_status_label.grid(row=0, column=3, padx=8, pady=6,
                                       sticky="w")

            # -- setup wizard ----------------------------------------------
            wiz = ctk.CTkFrame(self)
            wiz.pack(pady=4, padx=12, fill="x")
            ctk.CTkLabel(wiz, text="First-time setup (5 minutes)",
                         font=("Arial", 13, "bold")).pack(
                             anchor="w", padx=8, pady=(6, 2))
            ctk.CTkLabel(
                wiz, justify="left", wraplength=760,
                text="1. Go to console.cloud.google.com and create a "
                     "project.\n"
                     "2. APIs & Services → Library: enable "
                     "\"Places API (New)\".\n"
                     "3. Enable billing on the project (the API will not "
                     "run without it).\n"
                     "4. APIs & Services → Credentials: create an API key "
                     "and restrict it to Places API.\n"
                     "5. Paste the key above and click \"Test my key\"."
            ).pack(anchor="w", padx=8, pady=(0, 6))

            # -- license / activation --------------------------------------
            lic = ctk.CTkFrame(self)
            lic.pack(pady=4, padx=12, fill="x")
            self.license_status_label = ctk.CTkLabel(
                lic, text="Checking license…", wraplength=760,
                justify="left")
            self.license_status_label.grid(row=0, column=0, columnspan=3,
                                           padx=8, pady=(6, 2), sticky="w")
            ctk.CTkLabel(lic, text="License key:").grid(
                row=1, column=0, padx=8, pady=6, sticky="w")
            self.license_entry = ctk.CTkEntry(lic, width=340)
            self.license_entry.grid(row=1, column=1, padx=8, pady=6)
            ctk.CTkButton(lic, text="Activate", width=110,
                          command=self.activate).grid(
                              row=1, column=2, padx=8, pady=6)

            # -- search mode -------------------------------------------------
            self.mode_var = ctk.BooleanVar(value=False)
            ctk.CTkCheckBox(
                self, text="Bulk mode (one 'keyword | location' per line)",
                variable=self.mode_var,
                command=self._toggle_mode).pack(pady=(8, 4))

            self.single_frame = ctk.CTkFrame(self)
            self.single_frame.pack(pady=4)
            self.keyword_entry = ctk.CTkEntry(
                self.single_frame,
                placeholder_text="Keyword (e.g. Dentists)", width=300)
            self.keyword_entry.pack(pady=6)
            self.location_entry = ctk.CTkEntry(
                self.single_frame,
                placeholder_text="Location (e.g. Austin, TX)", width=300)
            self.location_entry.pack(pady=6)

            self.bulk_box = ctk.CTkTextbox(self, width=680, height=90)
            self.bulk_box.insert(
                "1.0", "Dentists | Austin, TX\nPlumbers | Miami, FL")

            # -- options ------------------------------------------------------
            opts = ctk.CTkFrame(self)
            opts.pack(pady=4, padx=12, fill="x")
            self.deep_sweep_var = ctk.BooleanVar(value=False)
            ctk.CTkCheckBox(
                opts,
                text="Deep sweep (grid search — beats the ~60-result cap, "
                     "uses more API calls)",
                variable=self.deep_sweep_var).grid(
                    row=0, column=0, padx=8, pady=6, sticky="w")
            ctk.CTkLabel(opts, text="Output CSV:").grid(
                row=1, column=0, padx=8, pady=6, sticky="w")
            self.output_entry = ctk.CTkEntry(opts, width=220)
            self.output_entry.insert(0, "leads_output.csv")
            self.output_entry.grid(row=1, column=1, padx=8, pady=6,
                                   sticky="w")

            # -- lead history ---------------------------------------------------
            hist = ctk.CTkFrame(self)
            hist.pack(pady=4, padx=12, fill="x")
            self.history_label = ctk.CTkLabel(hist, text="Leads in history: 0")
            self.history_label.grid(row=0, column=0, padx=8, pady=6,
                                    sticky="w")
            ctk.CTkButton(hist, text="Reset lead history", width=150,
                          command=self.reset_history).grid(
                              row=0, column=1, padx=8, pady=6)

            # -- actions ---------------------------------------------------------
            btns = ctk.CTkFrame(self)
            btns.pack(pady=8)
            self.start_btn = ctk.CTkButton(btns, text="Start Mining",
                                           command=self.start_mining)
            self.start_btn.grid(row=0, column=0, padx=10)
            self.stop_btn = ctk.CTkButton(btns, text="Stop",
                                          command=self.stop_mining,
                                          state="disabled",
                                          fg_color="#b3261e")
            self.stop_btn.grid(row=0, column=1, padx=10)

            self.textbox = ctk.CTkTextbox(self, width=760, height=200)
            self.textbox.pack(pady=8)

        # -- license state --------------------------------------------------
        def _license_state_worker(self):
            status, message = license_check.activation_state()
            self.status_queue.put(("license", (status, message)))

        def activate(self):
            key = self.license_entry.get().strip()
            if not key:
                self.log_queue.put("[!] Enter a license key first.")
                return
            self.log_queue.put("[*] Verifying license key…")

            def worker():
                status, message = license_check.verify_license(key)
                self.status_queue.put(("license", (status, message)))
            threading.Thread(target=worker, daemon=True).start()

        def _apply_license_result(self, status, message):
            self.log_queue.put(f"[*] License: {message}")
            if status == "valid":
                self.licensed = True
            elif status == "invalid":
                self.licensed = False
            # network_error: keep whatever state we already had.
            self._refresh_counts()

        # -- API key test -----------------------------------------------------
        def test_key(self):
            key = self.api_key_entry.get().strip() \
                or os.environ.get("GOOGLE_PLACES_API_KEY", "")
            self.key_status_label.configure(text="Testing…")

            def worker():
                ok, message = test_api_key(key)
                self.status_queue.put(("keytest", (ok, message)))
            threading.Thread(target=worker, daemon=True).start()

        def _apply_keytest_result(self, ok, message):
            self.log_queue.put(f"[*] {message}")
            if ok:
                self.key_status_label.configure(text="Key works ✓",
                                                text_color="#2e7d32")
            else:
                self.key_status_label.configure(text="Key failed ✗ — see log",
                                                text_color="#b3261e")

        # -- history ------------------------------------------------------------
        def _refresh_counts(self):
            self.history_label.configure(
                text=f"Leads in history: {self.store.count()}")
            if self.licensed:
                self.license_status_label.configure(
                    text="Licensed ✓ — unlimited mining.")
            else:
                used = self.store.trial_used()
                self.license_status_label.configure(
                    text=f"Free trial: {used} of {TRIAL_LIMIT} leads used. "
                         "Activate a license key for unlimited mining.")

        def reset_history(self):
            if not messagebox.askyesno(
                    "Reset lead history",
                    "Forget every lead collected so far? Future runs will "
                    "collect (and pay API costs for) those businesses "
                    "again. This does not reset your free-trial counter."):
                return
            self.store.reset()
            self._refresh_counts()
            self.log_queue.put("[*] Lead history cleared.")

        # -- search mode toggle ---------------------------------------------------
        def _toggle_mode(self):
            if self.mode_var.get():
                self.single_frame.pack_forget()
                self.bulk_box.pack(pady=4)
            else:
                self.bulk_box.pack_forget()
                self.single_frame.pack(pady=4)

        # -- worker control ---------------------------------------------------------
        def _collect_jobs(self):
            if self.mode_var.get():
                jobs = []
                for line in self.bulk_box.get("1.0", "end").splitlines():
                    if "|" not in line:
                        continue
                    kw, loc = (p.strip() for p in line.split("|", 1))
                    if kw and loc:
                        jobs.append((kw, loc))
                return jobs
            kw = self.keyword_entry.get().strip()
            loc = self.location_entry.get().strip()
            return [(kw, loc)] if kw and loc else []

        def start_mining(self):
            if not self.licensed and self.store.trial_used() >= TRIAL_LIMIT:
                self.textbox.insert(
                    "end", "[!] Free trial limit reached — "
                           "activate to continue.\n")
                self.textbox.see("end")
                return
            jobs = self._collect_jobs()
            if not jobs:
                self.textbox.insert(
                    "end", "[!] Enter a keyword + location, or bulk lines "
                           "as 'keyword | location'.\n")
                return
            self.stop_event.clear()
            self.start_btn.configure(state="disabled")
            self.stop_btn.configure(state="normal")
            self.textbox.delete("1.0", "end")

            pipeline = LeadPipeline(self.log_queue, self.stop_event,
                                    self.store, self.licensed)
            out = self.output_entry.get().strip() or "leads_output.csv"
            key = self.api_key_entry.get().strip() \
                or os.environ.get("GOOGLE_PLACES_API_KEY", "")
            engine = PlacesAPIEngine(pipeline, key)
            deep = self.deep_sweep_var.get()
            self.worker_thread = threading.Thread(
                target=engine.run, args=(jobs, out, deep), daemon=True)
            self.worker_thread.start()
            self.log_queue.put(
                f"[*] {len(jobs)} job(s) queued [Places API"
                f"{' + deep sweep' if deep else ''}].")

        def stop_mining(self):
            self.stop_event.set()
            self.log_queue.put("[!] Stopping...")
            self.start_btn.configure(state="normal")
            self.stop_btn.configure(state="disabled")

        # -- queues -------------------------------------------------------------------
        def poll_queues(self):
            while not self.log_queue.empty():
                self.textbox.insert("end",
                                    f"{self.log_queue.get_nowait()}\n")
                self.textbox.see("end")
            while not self.status_queue.empty():
                kind, payload = self.status_queue.get_nowait()
                if kind == "license":
                    self._apply_license_result(*payload)
                elif kind == "keytest":
                    self._apply_keytest_result(*payload)
            if (self.worker_thread and not self.worker_thread.is_alive()
                    and self.stop_btn.cget("state") == "normal"):
                self.start_btn.configure(state="normal")
                self.stop_btn.configure(state="disabled")
                self._refresh_counts()   # trial + history moved during run
            self.after(100, self.poll_queues)


if __name__ == "__main__":
    if not HAS_CTK:
        raise SystemExit(
            "customtkinter is required for the GUI: "
            "pip install customtkinter")
    app = LeadMinerApp()
    app.mainloop()
