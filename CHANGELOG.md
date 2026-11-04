# Changelog

## 3.0.1 — 2026-11-04 (tightening pass)

- **Fixed:** a persistent HTTP 429 (e.g. the monthly Places quota running
  out) retried forever and could not be stopped — retries are now capped
  at 3 with a backoff the Stop button interrupts, then the run ends with
  a plain-language explanation.
- **Fixed:** a dropped network connection mid-search killed the run
  silently. It now logs what happened and ends the search cleanly;
  leads already saved are kept.
- **Fixed:** an output CSV path that can't be opened (missing folder,
  no write permission) now reports the problem in the log instead of
  the run dying without a word.
- **Fixed:** refunded or charged-back Gumroad license keys no longer
  unlock unlimited mining.
- **Fixed:** the review-count parser could crash on a malformed value
  ("1.2.3"); it now degrades to the bare digits.
- **Fixed:** Stop-button race — Start could be re-enabled while the
  previous run was still finishing, letting two runs write the same
  CSV. Start now stays disabled until the worker has fully exited.
- **Added:** headless smoke-test suite (`tests/smoke_test.py`) — 44
  checks over parsing, dedup/trial store, the license gate, the key
  test, a mocked search cycle, API failure handling, and grid math.

## 3.0.0 — 2026-11-03

- v3 commercial release: official Google Places API (New) engine,
  persistent SQLite dedup, deep-sweep grid search, first-run setup
  wizard with key test, 25-lead free trial + Gumroad license gate.
