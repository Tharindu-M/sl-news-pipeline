# sl-news-pipeline

Scheduled job that aggregates Sri Lankan news in English, Sinhala and Tamil
and publishes it as static JSON. No server: GitHub Actions runs it every 20
minutes, GitHub Pages serves it. It is the backend for an Android news app
that has not been built yet.

## The API it produces

```
https://Tharindu-M.github.io/sl-news-pipeline/v1/feed_en.json
https://Tharindu-M.github.io/sl-news-pipeline/v1/feed_si.json
https://Tharindu-M.github.io/sl-news-pipeline/v1/feed_ta.json
https://Tharindu-M.github.io/sl-news-pipeline/v1/feed_{lang}_{category}.json
https://Tharindu-M.github.io/sl-news-pipeline/v1/sources.json
https://Tharindu-M.github.io/sl-news-pipeline/v1/status.json     <- health
```

Article shape (fields are omitted when null):

```json
{
  "id": "a1b2c3d4e5f60718",
  "source_id": "ada-derana-si",
  "source_name": "Ada Derana Sinhala",
  "lang": "si",
  "title": "...",
  "url": "https://sinhala.adaderana.lk/news/251377",
  "published": "2026-09-12T13:04:00Z",
  "excerpt": "First ~220 characters...",
  "image": "https://..."
}
```

## Layout

| file | role |
|---|---|
| `sources.yaml` | source registry — adapters, selectors, feeds. Edit this first. |
| `pipeline.py` | HTTP, `Article` model, five adapters, date parsing, dedupe, enrichment |
| `ingest.py` | orchestration: fetch all sources, merge with last run, write JSON |
| `probe.py` | finds which adapter works for a source. `--diagnose` dumps raw HTTP |
| `inspect_dates.py` | finds where a site publishes its dates (meta/JSON-LD/text) |
| `tests/test_pipeline.py`, `tests/test_e2e.py` | plain scripts, no pytest |
| `.github/workflows/ingest.yml` | every 20 min: lint, test, ingest, deploy Pages |
| `.github/workflows/healthcheck.yml` | daily: opens/closes a GitHub issue on failures |

## Commands

```bash
python ingest.py --out public          # full run
python ingest.py --out public --no-enrich   # skip per-article fetches (fast)
python probe.py                        # which adapter works per source
python probe.py --id divaina --diagnose     # raw HTTP for one source
python inspect_dates.py --id lankadeepa     # locate a site's date markup
python -m pyflakes *.py tests/*.py     # required before commit
python tests/test_pipeline.py && python tests/test_e2e.py
```

On PowerShell `*.py` does not expand — list files explicitly.

## How it works

**Adapter ladder**, cheapest and most durable first:
`rss` → `wordpress` (`/wp-json/wp/v2/posts?_embed`) → `sitemap` (Google News
sitemap) → `html` (CSS selectors from `sources.yaml`) → `google` (Google News
RSS search).

A source declares one adapter. If it returns nothing, `fetch_source` walks the
remaining ladder and records which one took over in `status.json`
(`used_fallback`, `primary_error`). `google` is always last: it works
everywhere, so ranking it higher would stop the real adapter from ever being
fixed.

**Google link resolution.** Google News returns opaque ids (`AU_yqL...`) that
cannot be decoded offline. `_decode_via_batchexecute` asks Google's own
internal endpoint. This is a private API — when it breaks,
`unresolved_google_links` in `status.json` climbs and users land on Google
instead of the publisher. Ten-plus sources depend on it. Requests to
`google.com` are throttled to one per 0.2s across all threads.

**Enrichment** fetches each article page for `og:image` and a real publish
date. Priority order: unresolved google links, then articles with a fabricated
date, then missing images (capped at 250).

**Merging.** Each run merges with the previous output so a source being down
for one cycle doesn't empty the feed. `dedupe` ranks fresh over cached, real
dates over estimated, then metadata richness. A total failure refuses to
overwrite good feeds.

## Constraints that are deliberate

- **No translation.** Sinhala readers get Sinhala originals. An earlier
  candidate backend (brief.lk) machine-translated a story about 735 kg of
  smuggled tobacco into "735 kg of high-grade heroin" while self-reporting
  `verdict: verified`. Do not add MT to the feed.
- **220-character excerpts.** Full text would make this a republisher rather
  than an aggregator. The app must open `url` in a Custom Tab.
- **Sinhala/Tamil dates are parsed explicitly** (`parse_local_date`).
  dateutil silently misreads `2026 සැප්තැම්බර් 12 | ප.ව. 06:34` as 12
  December. Never let dateutil near a localised date string.

## Known problems

**Sinhala is thin** — roughly 49 articles vs 331 English, 120 Tamil.

**Two sources are IP-blocked from CI**, and work fine from a UK home
connection:

| source | block |
|---|---|
| `ada-derana-en` / `ada-derana-si` | 403 CloudFront (whole domain) |
| `divaina` | 403 Cloudflare (whole domain, including `/feed`) |

No adapter change fixes this. The durable options are asking the publishers
for feed access, running ingestion from a residential IP, or accepting
Google's thin coverage. Ada Derana Sinhala gets ~18/run via Google; Divaina
gets 1.

**Dropping `when:7d`** from the Google query returns more articles but many
predate the 14-day retention window and get pruned.

**Recently revived, unverified**: `mawbima`, `ada-lk`, `newsfirst-si`,
`thinakkural`. Check `status.json` for whether they produced anything.
`ada-lk` is the most promising for Sinhala.

## Before committing

`pyflakes` and both test scripts must pass; CI runs them and refuses to
publish otherwise. This exists because a block rewrite once deleted four
functions, passed every local check, and only surfaced as a `NameError`
partway through a deployed run.

Never widen `RETRY_STATUSES` to include 403 — that is a hard block, and
retrying it wastes the whole run's time budget.

## Next

The Android app. One GET per language against a static file, Room cache,
WorkManager refresh, Chrome Custom Tabs for reading. No auth, no pagination,
no key to protect.
