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
  "image": "https://...",
  "category": "sports"
}
```

`category` is optional and sparse — see "Categories" below.

## Layout

| file | role |
|---|---|
| `sources.yaml` | source registry — adapters, selectors, feeds. Edit this first. |
| `pipeline.py` | HTTP, `Article` model, six adapters, date parsing, dedupe, enrichment |
| `ingest.py` | orchestration: fetch all sources, merge with last run, write JSON |
| `probe.py` | finds which adapter works for a source. `--diagnose` dumps raw HTTP |
| `inspect_dates.py` | finds where a site publishes its dates (meta/JSON-LD/text) |
| `tests/test_pipeline.py`, `tests/test_e2e.py` | plain scripts, no pytest |
| `tests/test_reliability.py` | unittest; run via `unittest discover`. Both workflows run it. |
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
python -m unittest discover -s tests -p test_reliability.py
```

On PowerShell `*.py` does not expand — list files explicitly.

## How it works

**Adapter ladder**, cheapest and most durable first:
`rss` → `newsfirst` (vendor JSON API) → `wordpress`
(`/wp-json/wp/v2/posts?_embed`) → `sitemap` (Google News sitemap) → `html`
(CSS selectors from `sources.yaml`) → `google` (Google News RSS search).

`newsfirst` is vendor-specific and gated by `_can_try`: it runs only for a
source that declares an `api:` key, so its ladder position is inert for
everyone else. `rss` stays first because it is the generic path.

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

## Categories

`category` is filled **only from what a publisher states**, never inferred
from the text. Four sources feed it, in precedence order:

1. `wordpress` reads `wp:term` out of the `_embed` response it already
   fetches — no extra request.
2. `rss` reads `<category>`.
3. `newsfirst` maps its API buckets (`NEWSFIRST_CATEGORIES`).
4. `sources.yaml` declares `url_categories:` (a `{path segment: bucket}` map
   against the article's own URL) or `category:` (pins a whole entry).

`CATEGORY_TERMS` in `pipeline.py` normalises site terms across all three
languages. **Only subject sections are mapped.** Placements — "News", "Lead
Story", "latest", "top-story", `ප්‍රධාන පුවත්`, `විගස පුවත්` — are deliberately
absent so they resolve to `None`. An unbucketed article is shown under "All";
a miscategorised one is a bug the reader sees.

`normalise_term` strips U+200B/U+FEFF/U+00AD but **must not strip U+200D**:
ZWJ is load-bearing in Sinhala (`ක්‍ර` is ක + virama + ZWJ + ර). Divaina's own
taxonomy spells sports `ක්‍රී​ඩා` — required ZWJ *plus* a stray ZWSP mid-word —
so a literal `ක්‍රීඩා` never matches it without normalisation.

`url_categories` is matched after Google resolution, decoded: it is the only
signal that survives the Google fallback, because the resolved link is the
publisher's own. Tamil Mirror's sections are Tamil words and arrive
percent-encoded, so write the readable slug in `sources.yaml`.

**Coverage is low and that is the data, not a bug** — 13% `en`, 5% `ta`, 1%
`si` on 2026-09-14. Most outlets file everything under one placement: every
recent Divaina post on `/wp-json` is `විගස පුවත්` (breaking news), and Ada
Derana's sitemap path carries no section at all. `status.json`
(`language_status[lang].categories`) tracks it so a silent decay is visible.

**Dead ends — do not re-attempt** (all probed 2026-09-14):

| tried | result |
|---|---|
| per-category Google queries | keyword form mis-buckets badly (a drug-deaths story and a political meeting returned under "sports"); path form (`site:x/business`) returns 0-2 results. Google matches the page, not the section. |
| BBC topic feeds | `feeds.bbci.co.uk/{sinhala,tamil}/{sport,world,business,…}/rss.xml` all 302 to the root feed — HTTP 200, non-empty, byte-identical. Always verify a section endpoint differs *from root*. |
| `article:section` / JSON-LD `articleSection` | sampled all 18 sources: only newswire (`"News"`, a placement) and divaina. Not worth wiring into `enrich`. |
| `adaderanatamil.lk/rss.xml?cat=sports` | param ignored, returns the root feed. |
| lankadeepa `url_categories` | article paths (`/latest_news/<slug>/1-697107`) are placements, not subjects. Its `/business/26` and `/politics/12` listings are real and could be registered with a pinned `category:` — costs a fetch per section and adds a pseudo-source to `sources.json`. |

Ada Derana's `rss.php` *does* tag every item with its section
(`sports`, `science-and-tech`, `international-news`), but those domains are
CloudFront-blocked from CI, and both sources are pinned to `sitemap`. If that
block ever lifts, switching them to `rss` is the single biggest category win
available.

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

**Sinhala is no longer the thin one** — 180 articles vs 365 English and 127
Tamil (2026-09-13). Tamil remains the weakest language, which is what
reviving `ada-derana-ta` was aimed at.

**Ada Derana Tamil is not a subdomain.** It lives on its own domain,
`adaderanatamil.lk`; `tamil.adaderana.lk` does not resolve, and that wrong
guess is why the source sat parked on a `google` adapter. Its `/rss.xml` is
the freshest option (`/rss.php` 302s to it); `/wp-json` 403s and
`/news-sitemap.xml` lags the feed by ~17h. Unlike `ada-derana-en` / `-si`,
this domain is **not** behind the CloudFront block — whether that holds from
a CI runner still needs confirming from an Actions run.

**Four sources are blocked from CI** and work fine from a UK home
connection. They are blocked in two different ways, confirmed from a live
`main` run on 2026-09-13:

| source | block | would a residential IP help? |
|---|---|---|
| `ada-derana-en` / `-si` / `-ta` | 403 CloudFront, no challenge markers | yes, plausibly |
| `divaina` | 403 Cloudflare, `cf-mitigated: challenge` | **no** |

`divaina` and `mawbima` are interactive JS challenges: the gate is executing
JavaScript, not the source IP, so moving runners cannot fix them. Only the
Ada Derana domains are genuinely IP-based. `ada-derana-ta` is the odd one:
`adaderanatamil.lk` is a separate domain that is clean from a home
connection, but CloudFront still 403s it from a runner.

**Decision (2026-09-13): run with what works.** All four degrade to the
Google fallback rather than failing, so the feeds stay populated. Chasing
publisher feed access or a residential runner is parked, not planned. If it
is ever revisited, the table above says which sources it could actually help.

No adapter change fixes any of this. Ada Derana Sinhala gets ~2-18/run via
Google; Divaina gets 1.

**Tell an IP block apart from a JS challenge before reaching for a fix** —
they look identical (403, `server: cloudflare`) and need opposite responses.
`diagnose_endpoint` now names which one it is: `cf-mitigated: challenge` plus
a "Just a moment..." page means Cloudflare wants JavaScript run, so a browser
passes and this client never will *at any IP* — that is `mawbima`. A plain
403 with no challenge markers is about where the request came from, and a
residential IP may genuinely help — that is `ada-derana-*` and `divaina`.

**Dropping `when:7d`** from the Google query returns more articles but many
predate the 14-day retention window and get pruned.

**The four revived sources were all verified on 2026-09-13.** Only
`newsfirst-si` survived; the other three are parked with the evidence in
`sources.yaml`:

| source | outcome |
|---|---|
| `newsfirst-si` | fixed — see the NewsFirst API note below |
| `mawbima` | Cloudflare **JS challenge** — a residential IP will not fix it |
| `thinakkural` | site is broken for everyone: 308 self-redirect loop |
| `ada-lk` | real sitemap is `/sitemaps`, but ~86s/run and **no dates at all** |

`ada-lk` is the one worth re-examining if it ever gets faster: the endpoints
exist, but its newest chunk carries 999 URLs with no `<lastmod>` or
`news:publication_date`, so nothing it returns can satisfy the
verified-freshness gate. Do not re-enable it without solving the dates.

**The NewsFirst sites are not WordPress from outside.** sinhala/english/tamil
`.newsfirst.lk` are SPAs: `/feed` 403s and `/wp-json` answers 200 with the app
shell rather than JSON, which is why all three used to fall through to Google.
Their front-ends read one endpoint — `{api}/post/sticky` — which returns every
homepage bucket in a single request with real `date_gmt` values, images and
excerpts. That is the `newsfirst` adapter, configured by `api:` in
`sources.yaml`.

**Relative dates.** Lankadeepa stamps anything under a day old as
"5 hours ago" and only older stories get an absolute date. `dateutil` raises
on that form, so half its feed used to get a fabricated scrape-time date and
sort above genuinely newer articles. `parse_relative_date` handles it; it is
tried inside `to_iso` between `parse_local_date` and `dateutil`.

## Before committing

`pyflakes` and all three test suites must pass; CI runs them and refuses to
publish otherwise. This exists because a block rewrite once deleted four
functions, passed every local check, and only surfaced as a `NameError`
partway through a deployed run.

Never widen `RETRY_STATUSES` to include 403 — that is a hard block, and
retrying it wastes the whole run's time budget.

## Next

The Android app. One GET per language against a static file, Room cache,
WorkManager refresh, Chrome Custom Tabs for reading. No auth, no pagination,
no key to protect.
