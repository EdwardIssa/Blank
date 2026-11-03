#!/usr/bin/env python3
"""
LeadMiner X v3 — license verification (Gumroad) + local activation cache.

Commercial model: a free trial (TRIAL_LIMIT leads, enforced by the app via
the counter in the SQLite store) and a one-time Gumroad license key that
unlocks unlimited mining.

verify_license() talks to Gumroad's license API:
    POST https://api.gumroad.com/v2/licenses/verify
    product_permalink, license_key, increment_uses_count=false

A successful verification is cached in ~/.leadminer/license.json so the app
does not need the network on every launch. The cache is trusted for
REVERIFY_DAYS; past that it is re-verified. If the network is unreachable
during a re-verify but a cached verification exists inside the grace window,
the license is treated as valid — a customer who paid must never be locked
out by a flaky connection. A definitive "invalid" answer clears the cache.
"""

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

# PLACEHOLDER — must match the real Gumroad product permalink at listing
# time. Gumroad identifies the product by this slug (the part after
# gumroad.com/l/), NOT by a numeric product id.
GUMROAD_PRODUCT_PERMALINK = "leadminerx"

GUMROAD_VERIFY_URL = "https://api.gumroad.com/v2/licenses/verify"

APP_DIR = os.path.join(os.path.expanduser("~"), ".leadminer")
CACHE_PATH = os.path.join(APP_DIR, "license.json")

REVERIFY_DAYS = 7          # cached verifications are trusted this long
TRIAL_LIMIT = 25           # free-trial leads before activation is required

USER_AGENT = "LeadMinerX/3.0"


# ---------------------------------------------------------------------------
# HTTP helper (requests when available, stdlib urllib otherwise)
# ---------------------------------------------------------------------------

def _post_form(url, fields, timeout):
    """POST form fields; return (http_status, parsed_json_or_None).

    Raises OSError on any connection failure / timeout so callers can map
    it to a single "network_error" outcome.
    """
    if HAS_REQUESTS:
        try:
            resp = requests.post(url, data=fields, timeout=timeout,
                                 headers={"User-Agent": USER_AGENT})
        except requests.RequestException as exc:
            raise OSError(str(exc)) from exc
        try:
            return resp.status_code, resp.json()
        except ValueError:
            return resp.status_code, None

    data = urllib.parse.urlencode(fields).encode("utf-8")
    request = urllib.request.Request(url, data=data,
                                     headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", "replace")
            try:
                return resp.status, json.loads(body) if body else None
            except ValueError:
                return resp.status, None
    except urllib.error.HTTPError as exc:
        # An HTTP error status is still a definitive server answer.
        body = exc.read().decode("utf-8", "replace") if exc.fp else ""
        try:
            return exc.code, json.loads(body) if body else None
        except ValueError:
            return exc.code, None
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise OSError(str(exc)) from exc


# ---------------------------------------------------------------------------
# Local cache (~/.leadminer/license.json)
# ---------------------------------------------------------------------------

def read_cache():
    """Return the cached {'key', 'verified_at'} dict, or None."""
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if data.get("key") and data.get("verified_at"):
            return data
    except (OSError, ValueError):
        pass
    return None


def _write_cache(key):
    os.makedirs(APP_DIR, exist_ok=True)
    payload = {
        "key": key,
        "verified_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(CACHE_PATH, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)


def _clear_cache():
    try:
        os.remove(CACHE_PATH)
    except OSError:
        pass


def _cache_age(cache):
    """Age of a cache entry as a timedelta, or None if unparseable."""
    try:
        verified_at = datetime.fromisoformat(cache["verified_at"])
        if verified_at.tzinfo is None:
            verified_at = verified_at.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) - verified_at
    except (KeyError, TypeError, ValueError):
        return None


def needs_reverify():
    """True when there is no cached verification, or it is stale (> 7 days)."""
    cache = read_cache()
    if cache is None:
        return True
    age = _cache_age(cache)
    if age is None:
        return True
    return age > timedelta(days=REVERIFY_DAYS)


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def verify_license(key):
    """Verify a Gumroad license key.

    Returns (status, message) with status one of:
      "valid"         — key confirmed (or accepted under offline grace)
      "invalid"       — Gumroad definitively rejected the key
      "network_error" — server unreachable and no usable cache
    """
    key = (key or "").strip()
    if not key:
        return "invalid", "Enter a license key."

    try:
        http_status, payload = _post_form(
            GUMROAD_VERIFY_URL,
            {
                "product_permalink": GUMROAD_PRODUCT_PERMALINK,
                "license_key": key,
                "increment_uses_count": "false",
            },
            timeout=15,
        )
    except OSError as exc:
        # Offline grace: a recently verified cached copy of THIS key wins.
        cache = read_cache()
        if cache and cache.get("key") == key:
            age = _cache_age(cache)
            if age is not None and age <= timedelta(days=REVERIFY_DAYS):
                return ("valid",
                        "Could not reach the license server — using your "
                        "recently verified license (offline grace).")
        return ("network_error",
                f"Could not reach the license server ({exc}). "
                "Check your connection and try again.")

    if http_status == 200 and payload and payload.get("success"):
        _write_cache(key)
        return "valid", "License verified — unlimited mining unlocked."

    # Definitive failure (404 = unknown key, success=false, ...): surface
    # Gumroad's own message and drop any cache of this same key.
    message = (payload or {}).get(
        "message",
        f"License check failed (HTTP {http_status}).")
    cache = read_cache()
    if cache is None or cache.get("key") == key:
        _clear_cache()
    return "invalid", message


def activation_state():
    """Resolve the app's current license state without user input.

    Returns (status, message) with status one of:
      "valid"         — activated (fresh cache, or re-verified just now)
      "unactivated"   — no license on file; the free trial applies
      "invalid"       — a stored key was definitively rejected
      "network_error" — stored key could not be re-verified (no grace left)
    """
    cache = read_cache()
    if cache is None:
        return "unactivated", "No license activated — free trial applies."
    if not needs_reverify():
        return "valid", "License active."
    return verify_license(cache["key"])
