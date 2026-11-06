# Changelog

## 3.1.0 — 2026-11-05 (email enrichment)

- **Added:** Email column (after Website). For every lead with a
  website, the run now reads the business's own site — homepage plus
  up to two likely contact pages — and records the most business-like
  address the business itself publishes (its own domain preferred,
  customer-facing inboxes like info@/contact@ first). Mailto links,
  plain-text addresses, and lightly obfuscated forms ("name [at]
  site [dot] com") are recognized; placeholders, error-tracker
  addresses, image filenames, and noreply@ senders are filtered out.
  No third-party lookup APIs; a business that publishes no email gets
  an empty cell. Found emails persist in the lead-history database
  (existing databases upgrade in place). Every site fetch is capped
  at 10 seconds and honors Stop, so a dead site can never hang a run.

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
