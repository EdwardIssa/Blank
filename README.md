# LeadMiner X v3.1

Desktop lead-extraction tool for Windows: business listings (name, category,
rating, review count, address, phone, website, email) streamed to CSV —
powered by Google's official **Places API (New)**. You bring your own
Google API key; everything runs on your machine.

## Why the official API

Scraping google.com/maps in a browser violates Google's Terms of Service, and
no evasion trick makes that commercially safe. The API is the legitimate path,
and the economics favor it: one request returns up to 20 complete leads.

**Costs, corrected (Nov 2026).** Google retired the $200/month Maps
Platform credit on March 1, 2025. Pricing is now a free usage cap per
SKU. Because this app's field mask requests phone number, website,
rating, and review count, its requests bill as **Text Search
Enterprise**: **1,000 requests free per month**, then **$35 per 1,000
requests**. Each request returns up to 20 leads, so the free cap covers
up to ~20,000 leads a month, and past the cap the API cost works out to
roughly **$0.002 per lead** (1,000 leads ≈ 50 requests ≈ $1.75). A
billing account is required even to use the free cap. (Source: Google
Maps Platform pricing list, checked 2026-11-03 —
https://developers.google.com/maps/billing-and-pricing/pricing)

## What's new in v3.1

- **Email enrichment** — the Places API never returns an email address,
  so for every lead with a website, LeadMiner now reads the business's
  own site (its homepage plus up to two likely contact pages) and adds
  the best published address to a new **Email** column, right after
  Website. Only addresses the business itself publishes are collected —
  nothing is guessed and no third-party lookup service is used. Expect
  roughly half of website-owning businesses to publish one; the rest
  (contact-form-only sites) simply get an empty cell. Found emails are
  remembered in your lead history alongside the lead.

## What's new in v3

- **Persistent dedup database** — every lead ever mined is remembered
  (SQLite, keyed on Google place ID with a name+address fallback). Re-running
  a city never re-buys a lead you already have; each run reports how many
  already-seen leads were skipped.
- **Deep sweep (grid search)** — a single Google text search tops out at ~60
  results. Deep sweep subdivides the map into a grid and searches each cell,
  so a city can return hundreds of leads instead of stopping at 60.
- **First-time setup wizard** — the 5 Google Cloud steps in plain language,
  in the app, with a **Test my key** button that proves your key works before
  you mine.
- **Free trial + license activation** — 25 leads free, no key purchase needed
  to try it. A Gumroad license key unlocks unlimited mining.

## Features

- **Email column** — the address each business publishes on its own
  website, collected during the same run at no extra API cost
- Single search or **bulk mode** (`keyword | location` per line)
- **Thread-safe design** — worker thread + queue-based logging + cooperative
  stop flag; stop anytime and keep every row already mined
- **Streaming CSV** — every record appended + flushed the moment it arrives
- **Robust parsing** — `"1.2K"` reviews → `1200`, phone numbers normalized to
  `+1…` format, every failure logged in plain language

## API setup (about 5 minutes — the in-app wizard walks you through it)

1. [Google Cloud Console](https://console.cloud.google.com/) → create a project
2. Enable **Places API (New)** (APIs & Services → Library)
3. Enable billing on the project (required even for the free usage cap)
4. Create an API key (restrict it to Places API)
5. Paste the key in the app and hit **Test my key**

## Running from source

```bash
pip install -r requirements.txt
python leadminer_x.py
```

## Building the .exe

Pushing to this repo runs the GitHub Actions build (Windows + macOS
artifacts). See [BUILD.md](BUILD.md).

## Usage

1. Paste your Places API key (or set `GOOGLE_PLACES_API_KEY`).
2. Enter a keyword (e.g. `Real estate agents`) and location (e.g. `Troy, MI`),
   or enable **Bulk mode** and paste `keyword | location` lines.
3. Optional: tick **Deep sweep** for full-city coverage.
4. Hit **Start Mining**. **Stop** cancels cleanly; completed rows are saved.
