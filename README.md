# Sri Lanka news pipeline

Scheduled job that pulls from Sri Lankan outlets in English, Sinhala and
Tamil, normalizes everything into one shape, and publishes static JSON.
No server. GitHub Actions runs it; GitHub Pages serves it.

## Your API

    https://<user>.github.io/<repo>/v1/feed_en.json
    https://<user>.github.io/<repo>/v1/feed_si.json
    https://<user>.github.io/<repo>/v1/feed_ta.json
    https://<user>.github.io/<repo>/v1/feed_{lang}_{category}.json
    https://<user>.github.io/<repo>/v1/sources.json
    https://<user>.github.io/<repo>/v1/status.json     <- your health dashboard

Article shape:

```json
{
  "id": "a1b2c3d4e5f60718",
  "source_id": "ada-derana-si",
  "source_name": "Ada Derana Sinhala",
  "lang": "si",
  "title": "...",
  "url": "https://sinhala.adaderana.lk/news/229047",
  "published": "2026-09-12T04:30:00Z",
  "date_estimated": false,
  "collected_at": "2026-09-12T04:40:00Z",
  "excerpt": "First ~220 characters...",
  "image": "https://s3.amazonaws.com/adaderanasinhala/....jpg",
  "category": "sports"
}
```

`published` is UTC. If `date_estimated` is true, it is an observation-time
fallback, **not a verified publication time**; the app should hide relative
publication labels or show "publication time unavailable". Repeated undated
articles retain their original fallback time instead of becoming new each run.
`collected_at` records first observation within retained history (legacy records
without it use their existing timestamp when merged). Existing v1 consumers
should tolerate the two additive fields.

Feeds contain at most 150 articles per language, with a 14-day retention ceiling;
this is not a complete archive or a paginated API.

`category` is **optional and sparse**. It is present only when a publisher
states the section itself — a WordPress `wp:term`, an RSS `<category>`, a
NewsFirst API bucket, or a section slug in the article's own URL mapped in
`sources.yaml`. Nothing is inferred from the headline, so a story is left
unbucketed rather than guessed at. Real coverage on 2026-09-14 was 13% of
`feed_en`, 5% of `feed_ta` and 1% of `feed_si`; most Sri Lankan outlets file
everything under a placement like "Breaking News" and expose no subject at
all. Treat categories as a filter that enriches the feed, never as navigation
the app depends on — build the UI around the all-articles feed and offer
category tabs only where `status.json` shows the counts are worth it.

Categories are one of `politics`, `business`, `sports`, `tech`,
`international`, `entertainment`, `society`.

**All seven `feed_{lang}_{category}.json` endpoints are always published**, so
the client can hardcode the tab set. A category with nothing in it returns 200
with `"count": 0` and `"articles": []` — never a 404. Treat a 404 as a real
error. `language_status[lang].categories` in `status.json` carries the count
for every bucket (zeros included), which is enough to label or grey out tabs
without fetching each feed.

The one exception: if a language fails completely and has no cached snapshot
to fall back on, `feed_{lang}.json` is not published that run and neither are
its category files. In that state the whole language 404s, not just a category.

## Setup

    pip install -r requirements.txt
    python probe.py              # find out which adapter works per outlet
    # pin the winners in sources.yaml, then:
    python ingest.py --out public

Then in the repo: Settings -> Pages -> Source: GitHub Actions. Push, and the
workflow is scheduled every 20 minutes on `main` (execution can be delayed).
Set repository Actions variable `FEED_BASE` to the Pages base URL without a
trailing slash, for example `https://Tharindu-M.github.io/sl-news-pipeline`.
Enable Issues for health alerts. The deployment workflow is
`.github/workflows/ingest.yml`; feature branches run offline checks only.

Offline checks (no publisher requests):

    python -m pyflakes *.py tests/*.py
    python tests/test_pipeline.py
    python tests/test_e2e.py
    python -m unittest discover -s tests -p test_reliability.py

The glob syntax in the static-check command assumes a shell that expands it;
on PowerShell, pass file paths from `Get-ChildItem *.py,tests/*.py` instead.

## When a source breaks

`status.json` tells you which one and why. Re-run `probe.py --id <source>`,
switch the adapter or fix the selectors in `sources.yaml`, push. Live in
minutes — the Android app never changes.

Fetch success (`last_ok`) is separate from content freshness
(`newest_published`, `hours_since_publication`, `stale_content`).
`new_articles` counts IDs absent from the retained feeds, not a lifetime total.
Set `max_age_hours` under `defaults` or an individual source (default: 48).
Missing verified dates count as stale; estimated dates cannot prove freshness.
`google_sources` includes explicitly configured Google adapters as well as
automatic fallbacks.

Every completed ingestion writes `diagnostics/status.json`, even when health
validation rejects the run. Accepted feeds are staged as a complete snapshot
before replacing `public/v1`; rejected runs leave the previous snapshot intact.
CI uploads diagnostics separately and caches feeds only after successful Pages
deployment, using a new cache namespace that excludes older failed outputs.
Local directory promotion supports rollback on rename failure, but direct local
readers can see a short gap and only one writer is supported; Pages publishes
the uploaded artifact as a unit. Actions caches may be evicted and are not backups.

Article links are limited to the configured publisher hostname (plus its `www`
variant) or Google News. Enrichment and Google resolution validate redirect
destinations before following them. This rejects unexpected hosts, credentials,
unsafe schemes and private IP literals, but is not a DNS-rebinding defence.
Source configuration must remain trusted; review legitimate hostname migrations.
Previously saved feeds lack date-confidence metadata: regenerate or audit that
legacy history if it contains known fabricated dates.

## Adapter order

Always prefer the cheapest that works:

1. `rss` — a real feed
2. `wordpress` — `/wp-json/wp/v2/posts?_embed`, works on many .lk sites even
   when the feed is dead
3. `sitemap` — Google News sitemap, listed in robots.txt
4. `html` — CSS selectors against a listing page, most fragile

Last resort for a site with none of the above:

    https://news.google.com/rss/search?q=site:example.lk&hl=si&gl=LK&ceid=LK:si

Links go through a Google redirect and indexing can lag. Resolution uses an
undocumented endpoint and may stop working; it is not a guaranteed service.

## Deliberate non-goals

**No translation.** Sinhala readers get Sinhala originals, Tamil readers get
Tamil. Machine translation of news is how you end up publishing "735 kg of
heroin" when the source said tobacco leaves.

**No summarization.** Headline, optional ~220-char excerpt, and link.
Excerpt length is not a legal safe harbour: verify publisher terms and obtain
permission where needed for headlines, excerpts and images, including hotlinks.

## Costs

Standard GitHub-hosted Actions runners can be free for public repositories,
subject to GitHub's current terms and limits. Check Pages usage restrictions
and quotas before using it as a production/commercial app backend.
Private runs at 72/day x 3 minutes use roughly 6,500 minutes/month, above a
2,000-minute allowance. Measure runtime and bandwidth rather than assuming $0;
moving this Python pipeline to a different scheduler/storage requires adaptation.
