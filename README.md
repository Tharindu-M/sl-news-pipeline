# Sri Lanka news pipeline

Scheduled job that pulls from Sri Lankan outlets in English, Sinhala and
Tamil, normalizes everything into one shape, and publishes static JSON.
No server. GitHub Actions runs it; GitHub Pages serves it.

## Your API

    https://<user>.github.io/<repo>/v1/feed_en.json
    https://<user>.github.io/<repo>/v1/feed_si.json
    https://<user>.github.io/<repo>/v1/feed_ta.json
    https://<user>.github.io/<repo>/v1/feed_si_politics.json
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
  "excerpt": "First ~220 characters...",
  "image": "https://s3.amazonaws.com/adaderanasinhala/....jpg"
}
```

## Setup

    pip install -r requirements.txt
    python probe.py              # find out which adapter works per outlet
    # pin the winners in sources.yaml, then:
    python ingest.py --out public

Then in the repo: Settings -> Pages -> Source: GitHub Actions. Push, and the
workflow runs every 20 minutes.

## When a source breaks

`status.json` tells you which one and why. Re-run `probe.py --id <source>`,
switch the adapter or fix the selectors in `sources.yaml`, push. Live in
minutes — the Android app never changes.

## Adapter order

Always prefer the cheapest that works:

1. `rss` — a real feed
2. `wordpress` — `/wp-json/wp/v2/posts?_embed`, works on many .lk sites even
   when the feed is dead
3. `sitemap` — Google News sitemap, listed in robots.txt
4. `html` — CSS selectors against a listing page, most fragile

Last resort for a site with none of the above:

    https://news.google.com/rss/search?q=site:example.lk&hl=si&gl=LK&ceid=LK:si

Links go through a Google redirect and there's a lag, but it works on almost
anything indexed.

## Deliberate non-goals

**No translation.** Sinhala readers get Sinhala originals, Tamil readers get
Tamil. Machine translation of news is how you end up publishing "735 kg of
heroin" when the source said tobacco leaves.

**No summarization.** Headline, ~220-char excerpt, link. Anything longer
makes you a republisher rather than an aggregator.

## Costs

Free for public repos. GitHub Actions gives unlimited minutes on public
repos; Pages serves 100 GB/month. At ~40 KB per feed that is a lot of
readers. If you go private, ~72 runs/day x ~3 min is roughly 6,500
minutes/month against a 2,000-minute free allowance — move to Cloudflare
Workers Cron + R2 at that point.
