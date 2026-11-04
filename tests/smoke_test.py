#!/usr/bin/env python3
"""
LeadMiner X — headless smoke tests for the non-GUI core.

Covers: parsing helpers, the persistent dedup/trial store, the CSV
pipeline + trial gate, the Gumroad license gate (stubbed HTTP), the API
key test (stubbed HTTP), a mocked Places search cycle end to end, API
failure handling, and the deep-sweep grid math. No live API calls are
made — every HTTP surface is stubbed.

Run:  python tests/smoke_test.py        (exits non-zero on any failure)
"""

import csv
import json
import os
import queue
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import requests  # noqa: E402

import leadminer_x as lx  # noqa: E402
import license_check as lc  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeResp:
    """Minimal stand-in for requests.Response."""

    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


def drain(q):
    out = []
    while not q.empty():
        out.append(q.get_nowait())
    return out


def make_pipeline(tmpdir, licensed=True, store=None):
    store = store or lx.LeadStore(os.path.join(tmpdir, "leads.db"))
    log_q = queue.Queue()
    stop = threading.Event()
    return lx.LeadPipeline(log_q, stop, store, licensed), log_q, stop, store


PLACE_A = {
    "id": "pid-a",
    "displayName": {"text": "Alpha Dentists"},
    "types": ["dentist"],
    "formattedAddress": "1 Main St, Austin, TX",
    "nationalPhoneNumber": "(512) 555-0100",
    "websiteUri": "https://alpha.example",
    "rating": 4.5,
    "userRatingCount": 240,
    "businessStatus": "OPERATIONAL",
}
PLACE_B = {
    "id": "pid-b",
    "displayName": {"text": "Beta Dental"},
    "types": ["dentist"],
    "formattedAddress": "2 Oak Ave, Austin, TX",
    "nationalPhoneNumber": "(512) 555-0200",
    "rating": 4.0,
    "userRatingCount": 12,
    "businessStatus": "OPERATIONAL",
}
PLACE_C = {
    "id": "pid-c",
    "displayName": {"text": "Gamma Smiles"},
    "types": ["dentist"],
    "formattedAddress": "3 Pine Rd, Austin, TX",
    "userRatingCount": 7,
}


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

class TestParsing(unittest.TestCase):
    def test_review_count_plain(self):
        self.assertEqual(lx.LeadPipeline.parse_review_count("240"), "240")

    def test_review_count_k_suffix(self):
        self.assertEqual(lx.LeadPipeline.parse_review_count("1.2K"), "1200")

    def test_review_count_m_suffix(self):
        self.assertEqual(lx.LeadPipeline.parse_review_count("3M"), "3000000")

    def test_review_count_int_input(self):
        self.assertEqual(lx.LeadPipeline.parse_review_count(240), "240")

    def test_review_count_none(self):
        self.assertEqual(lx.LeadPipeline.parse_review_count(None), "N/A")

    def test_review_count_malformed_decimal(self):
        # Must never raise — a bad value degrades to digits or "N/A".
        result = lx.LeadPipeline.parse_review_count("1.2.3")
        self.assertIn(result, ("123", "N/A"))

    def test_phone_10_digit(self):
        self.assertEqual(lx.LeadPipeline.normalize_phone("(512) 555-0100"),
                         "+15125550100")

    def test_phone_11_digit(self):
        self.assertEqual(lx.LeadPipeline.normalize_phone("1-512-555-0100"),
                         "+15125550100")

    def test_phone_missing(self):
        self.assertEqual(lx.LeadPipeline.normalize_phone(None), "N/A")
        self.assertEqual(lx.LeadPipeline.normalize_phone("N/A"), "N/A")
        self.assertEqual(lx.LeadPipeline.normalize_phone(""), "N/A")


# ---------------------------------------------------------------------------
# Persistent store: dedup + trial counter
# ---------------------------------------------------------------------------

class TestStore(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.db = os.path.join(self.tmp, "leads.db")
        self.store = lx.LeadStore(self.db)

    def test_add_contains_by_place_id(self):
        self.store.add("pid-1", "Alpha", "1 Main St")
        self.assertTrue(self.store.contains("pid-1", "Alpha", "1 Main St"))

    def test_fallback_name_addr(self):
        self.store.add(None, "Alpha", "1 Main St")
        self.assertTrue(self.store.contains(None, "Alpha", "1 Main St"))
        self.assertFalse(self.store.contains(None, "Alpha", "9 Other St"))

    def test_cross_key_dedup(self):
        # Seen under the fallback key -> found later WITH a place_id...
        self.store.add(None, "Alpha", "1 Main St")
        self.assertTrue(self.store.contains("pid-new", "Alpha", "1 Main St"))
        # ...and seen with a place_id -> found later without one.
        self.store.add("pid-2", "Beta", "2 Oak Ave")
        self.assertTrue(self.store.contains(None, "Beta", "2 Oak Ave"))

    def test_count_and_reset_keeps_trial(self):
        self.store.add("pid-1", "Alpha", "1 Main St")
        self.store.increment_trial()
        self.store.reset()
        self.assertEqual(self.store.count(), 0)
        self.assertEqual(self.store.trial_used(), 1)  # trial NOT reset

    def test_trial_counter_persists(self):
        self.store.increment_trial()
        self.store.increment_trial()
        reopened = lx.LeadStore(self.db)
        self.assertEqual(reopened.trial_used(), 2)


# ---------------------------------------------------------------------------
# Pipeline: CSV streaming, dedup, trial gate
# ---------------------------------------------------------------------------

class TestPipeline(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.csv_path = os.path.join(self.tmp, "out.csv")

    def _record(self, name="Alpha Dentists", addr="1 Main St"):
        return {"Name": name, "Category": "Dentist", "Rating": "4.5",
                "Reviews": "240", "Address": addr,
                "Phone": "+15125550100", "Website": "https://a.example",
                "Search Location": "Austin, TX"}

    def test_writes_and_counts(self):
        pipe, log_q, _, _ = make_pipeline(self.tmp)
        pipe.open_csv(self.csv_path)
        self.assertTrue(pipe.add_record(self._record(), "pid-a"))
        pipe.close_csv()
        self.assertEqual(pipe.total, 1)
        with open(self.csv_path, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["Name"], "Alpha Dentists")

    def test_dedup_skip_counted(self):
        pipe, _, _, _ = make_pipeline(self.tmp)
        pipe.open_csv(self.csv_path)
        pipe.add_record(self._record(), "pid-a")
        self.assertFalse(pipe.add_record(self._record(), "pid-a"))
        pipe.close_csv()
        self.assertEqual(pipe.skipped_seen, 1)
        self.assertEqual(pipe.total, 1)

    def test_trial_gate_stops_at_limit(self):
        store = lx.LeadStore(os.path.join(self.tmp, "leads.db"))
        for _ in range(lc.TRIAL_LIMIT - 1):
            store.increment_trial()
        pipe, log_q, stop, _ = make_pipeline(self.tmp, licensed=False,
                                             store=store)
        pipe.open_csv(self.csv_path)
        self.assertTrue(pipe.add_record(self._record("One", "1 A St"), "p1"))
        self.assertFalse(pipe.add_record(self._record("Two", "2 B St"), "p2"))
        pipe.close_csv()
        self.assertTrue(pipe.trial_limit_hit)
        self.assertTrue(stop.is_set())
        self.assertEqual(store.trial_used(), lc.TRIAL_LIMIT)

    def test_licensed_bypasses_trial(self):
        store = lx.LeadStore(os.path.join(self.tmp, "leads.db"))
        for _ in range(lc.TRIAL_LIMIT):
            store.increment_trial()
        pipe, _, _, _ = make_pipeline(self.tmp, licensed=True, store=store)
        pipe.open_csv(self.csv_path)
        self.assertTrue(pipe.add_record(self._record(), "pid-a"))
        pipe.close_csv()

    def test_empty_name_skipped(self):
        pipe, _, _, _ = make_pipeline(self.tmp)
        pipe.open_csv(self.csv_path)
        rec = self._record()
        rec["Name"] = ""
        self.assertFalse(pipe.add_record(rec, "pid-x"))
        pipe.close_csv()
        self.assertEqual(pipe.total, 0)


# ---------------------------------------------------------------------------
# License gate (HTTP stubbed at license_check._post_form)
# ---------------------------------------------------------------------------

class TestLicense(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.app_dir = os.path.join(self.tmp, ".leadminer")
        self.cache_path = os.path.join(self.app_dir, "license.json")
        patcher = mock.patch.multiple(
            lc, APP_DIR=self.app_dir, CACHE_PATH=self.cache_path)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _write_cache(self, key, age_days=0):
        os.makedirs(self.app_dir, exist_ok=True)
        when = datetime.now(timezone.utc) - timedelta(days=age_days)
        with open(self.cache_path, "w", encoding="utf-8") as fh:
            json.dump({"key": key, "verified_at": when.isoformat()}, fh)

    def test_valid_key_caches(self):
        with mock.patch.object(lc, "_post_form",
                               return_value=(200, {"success": True})):
            status, _ = lc.verify_license("KEY-1")
        self.assertEqual(status, "valid")
        self.assertIsNotNone(lc.read_cache())

    def test_invalid_key(self):
        with mock.patch.object(
                lc, "_post_form",
                return_value=(404, {"success": False,
                                    "message": "Invalid license key."})):
            status, msg = lc.verify_license("NOPE")
        self.assertEqual(status, "invalid")
        self.assertIn("Invalid", msg)

    def test_empty_key(self):
        status, _ = lc.verify_license("   ")
        self.assertEqual(status, "invalid")

    def test_network_error_no_cache(self):
        with mock.patch.object(lc, "_post_form",
                               side_effect=OSError("boom")):
            status, _ = lc.verify_license("KEY-1")
        self.assertEqual(status, "network_error")

    def test_offline_grace_fresh_cache(self):
        self._write_cache("KEY-1", age_days=1)
        with mock.patch.object(lc, "_post_form",
                               side_effect=OSError("boom")):
            status, _ = lc.verify_license("KEY-1")
        self.assertEqual(status, "valid")

    def test_offline_stale_cache_no_grace(self):
        self._write_cache("KEY-1", age_days=30)
        with mock.patch.object(lc, "_post_form",
                               side_effect=OSError("boom")):
            status, _ = lc.verify_license("KEY-1")
        self.assertEqual(status, "network_error")

    def test_refunded_key_rejected(self):
        # A refunded purchase must NOT unlock unlimited mining.
        payload = {"success": True, "purchase": {"refunded": True}}
        with mock.patch.object(lc, "_post_form",
                               return_value=(200, payload)):
            status, _ = lc.verify_license("KEY-REFUNDED")
        self.assertEqual(status, "invalid")

    def test_chargebacked_key_rejected(self):
        payload = {"success": True, "purchase": {"chargebacked": True}}
        with mock.patch.object(lc, "_post_form",
                               return_value=(200, payload)):
            status, _ = lc.verify_license("KEY-CB")
        self.assertEqual(status, "invalid")

    def test_activation_state_unactivated(self):
        status, _ = lc.activation_state()
        self.assertEqual(status, "unactivated")


# ---------------------------------------------------------------------------
# API key test (requests.post stubbed)
# ---------------------------------------------------------------------------

class TestKeyTest(unittest.TestCase):
    def test_key_valid(self):
        with mock.patch("requests.post",
                        return_value=FakeResp(200, {"places": []})):
            ok, _ = lx.test_api_key("AIza-test")
        self.assertTrue(ok)

    def test_key_forbidden(self):
        err = {"error": {"status": "PERMISSION_DENIED", "message": "no"}}
        with mock.patch("requests.post",
                        return_value=FakeResp(403, err, text="forbidden")):
            ok, msg = lx.test_api_key("AIza-bad")
        self.assertFalse(ok)
        self.assertIn("rejected", msg)

    def test_key_429(self):
        with mock.patch("requests.post",
                        return_value=FakeResp(429, {}, text="quota")):
            ok, msg = lx.test_api_key("AIza-test")
        self.assertFalse(ok)
        self.assertIn("Rate limited", msg)

    def test_key_network_error(self):
        with mock.patch("requests.post",
                        side_effect=requests.ConnectionError("down")):
            ok, msg = lx.test_api_key("AIza-test")
        self.assertFalse(ok)
        self.assertIn("Network error", msg)

    def test_key_empty(self):
        with mock.patch("requests.post") as post:
            ok, _ = lx.test_api_key("")
        self.assertFalse(ok)
        post.assert_not_called()


# ---------------------------------------------------------------------------
# Engine: mocked Places search cycle + failure handling
# ---------------------------------------------------------------------------

class TestEngine(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.csv_path = os.path.join(self.tmp, "out.csv")

    def _two_page_stub(self, calls):
        def stub(url, headers=None, json=None, timeout=None):
            calls.append(json)
            if json.get("pageToken"):
                return FakeResp(200, {"places": [PLACE_C]})
            return FakeResp(200, {"places": [PLACE_A, PLACE_B],
                                  "nextPageToken": "tok-2"})
        return stub

    def test_two_page_search_and_dedup_rerun(self):
        calls = []
        with mock.patch("requests.post", side_effect=self._two_page_stub(calls)):
            pipe, _, _, store = make_pipeline(self.tmp)
            lx.PlacesAPIEngine(pipe, "KEY").run(
                [("Dentists", "Austin, TX")], self.csv_path)
            self.assertEqual(pipe.total, 3)
            # Second run over the same store: everything already seen.
            pipe2, log_q2, _, _ = make_pipeline(self.tmp, store=store)
            lx.PlacesAPIEngine(pipe2, "KEY").run(
                [("Dentists", "Austin, TX")], self.csv_path)
            self.assertEqual(pipe2.total, 0)
            self.assertEqual(pipe2.skipped_seen, 3)
        with open(self.csv_path, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["Phone"], "+15125550100")

    def test_api_403_completes_with_log(self):
        err = {"error": {"status": "PERMISSION_DENIED",
                         "message": "API not enabled"}}
        with mock.patch("requests.post",
                        return_value=FakeResp(403, err, text="denied")):
            pipe, log_q, _, _ = make_pipeline(self.tmp)
            lx.PlacesAPIEngine(pipe, "KEY").run(
                [("Dentists", "Austin, TX")], self.csv_path)
        logs = drain(log_q)
        self.assertEqual(pipe.total, 0)
        self.assertTrue(any("403" in line for line in logs), logs)

    def test_network_exception_survived(self):
        # A dropped connection mid-search must surface in the log and let
        # the run end cleanly — never kill the worker thread silently.
        with mock.patch("requests.post",
                        side_effect=requests.ConnectionError("reset")):
            pipe, log_q, _, _ = make_pipeline(self.tmp)
            lx.PlacesAPIEngine(pipe, "KEY").run(
                [("Dentists", "Austin, TX")], self.csv_path)
        logs = drain(log_q)
        self.assertTrue(
            any("network" in line.lower() for line in logs), logs)

    def _pipeline_with_event(self, event):
        store = lx.LeadStore(os.path.join(self.tmp, "leads.db"))
        return lx.LeadPipeline(queue.Queue(), event, store, True)

    def test_429_bounded(self):
        # Persistent 429 (e.g. monthly quota exhausted) must NOT retry
        # forever: the request gives up after MAX_429_RETRIES retries,
        # returns None, and does not raise. The event stand-in makes
        # backoff waits instant so the test doesn't sit through them.
        class InstantEvent:
            def is_set(self):
                return False

            def set(self):
                pass

            def wait(self, timeout=None):
                return False

        calls = []

        def stub(url, headers=None, json=None, timeout=None):
            calls.append(1)
            return FakeResp(429, {}, text="quota")

        engine = lx.PlacesAPIEngine(
            self._pipeline_with_event(InstantEvent()), "KEY")
        with mock.patch("requests.post", side_effect=stub):
            result = engine._request("dentists in Austin")
        self.assertIsNone(result)
        self.assertEqual(len(calls), 1 + lx.PlacesAPIEngine.MAX_429_RETRIES)

    def test_429_stop_interrupts_backoff(self):
        # Pressing Stop during a 429 backoff must end the request at
        # once — not after the remaining 10/20/30s of waits.
        import time as _time

        def stub(url, headers=None, json=None, timeout=None):
            return FakeResp(429, {}, text="quota")

        event = threading.Event()
        engine = lx.PlacesAPIEngine(
            self._pipeline_with_event(event), "KEY")
        outcome = {}

        def call():
            try:
                outcome["result"] = engine._request("dentists in Austin")
            except Exception as exc:  # noqa: BLE001
                outcome["error"] = exc

        with mock.patch("requests.post", side_effect=stub):
            t = threading.Thread(target=call)
            t.start()
            _time.sleep(0.5)
            event.set()
            t.join(timeout=5)
        self.assertFalse(t.is_alive(), "Stop did not interrupt the backoff")
        self.assertNotIn("error", outcome)
        self.assertIsNone(outcome.get("result"))

    def test_bad_output_path_reported(self):
        bad = os.path.join(self.tmp, "no-such-dir", "out.csv")
        with mock.patch("requests.post",
                        return_value=FakeResp(200, {"places": [PLACE_A]})):
            pipe, log_q, _, _ = make_pipeline(self.tmp)
            lx.PlacesAPIEngine(pipe, "KEY").run(
                [("Dentists", "Austin, TX")], bad)
        logs = drain(log_q)
        self.assertTrue(
            any("could not open" in line.lower() for line in logs), logs)


# ---------------------------------------------------------------------------
# Deep sweep: geocoding + grid math
# ---------------------------------------------------------------------------

class TestGrid(unittest.TestCase):
    def test_small_bbox_coverage(self):
        bbox = (42.0, -83.0, 42.05, -82.95)
        cells = lx.grid_cells(bbox)
        self.assertLessEqual(len(cells), lx.GRID_MAX_CELLS)
        self.assertEqual(min(c[0] for c in cells), 42.0)
        self.assertEqual(min(c[1] for c in cells), -83.0)
        self.assertAlmostEqual(max(c[2] for c in cells), 42.05)
        self.assertAlmostEqual(max(c[3] for c in cells), -82.95)
        for low_lat, low_lon, high_lat, high_lon in cells:
            self.assertLess(low_lat, high_lat)
            self.assertLess(low_lon, high_lon)

    def test_large_bbox_capped(self):
        cells = lx.grid_cells((30.0, -120.0, 35.0, -115.0))
        self.assertEqual(len(cells), lx.GRID_MAX_CELLS)

    def test_tiny_bbox_single_cell(self):
        cells = lx.grid_cells((42.0, -83.0, 42.001, -82.999))
        self.assertEqual(len(cells), 1)

    def test_geocode_ok(self):
        payload = [{"boundingbox": ["42.0", "42.1", "-83.1", "-83.0"]}]
        with mock.patch("requests.get",
                        return_value=FakeResp(200, payload)):
            bbox, reason = lx.geocode_bbox("Troy, MI")
        self.assertIsNone(reason)
        self.assertEqual(bbox, (42.0, -83.1, 42.1, -83.0))

    def test_geocode_no_result(self):
        with mock.patch("requests.get", return_value=FakeResp(200, [])):
            bbox, reason = lx.geocode_bbox("Nowhere ZZ")
        self.assertIsNone(bbox)
        self.assertIsNotNone(reason)


if __name__ == "__main__":
    unittest.main(verbosity=2)
