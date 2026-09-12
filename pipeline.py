"""
Core ingestion primitives: HTTP, the normalized Article model, and one
adapter per source type.

Design rule: every adapter returns List[Article] or raises. Nothing here
knows about output formats or scheduling — ingest.py owns that.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone, timedelta
from typing import Any, Iterable
from urllib.parse import (urljoin, urlparse, urlunparse, parse_qsl,
                          urlencode, quote_plus)

import warnings

import feedparser
import httpx
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

# clean_text() is called on short strings that BeautifulSoup mistakes for
# filenames. The warning is noise; the behaviour is correct.
warnings.filterwarnings("ignore", message=".*looks more like a filename.*")

log = logging.getLogger("pipeline")

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)

# Tracking params that create duplicate URLs for the same article.
JUNK_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "ref", "s", "amp",
}


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------

@dataclass
class Article:
    id: str
    source_id: str
    source_name: str
    lang: str                  # en | si | ta
    title: str
    url: str
    published: str             # ISO-8601 UTC
    excerpt: str | None = None
    image: str | None = None
    category: str | None = None
    # Not serialised — used only to verify a resolved google redirect landed
    # on the right publisher.
    source_domain: str | None = None
    # True when `published` is the scrape time rather than the publisher's.
    # Listing pages rarely carry a date; enrich() repairs these from
    # og:article:published_time. Never serialised.
    date_estimated: bool = False
    # Optional CSS selector for the date on this source's article pages, from
    # `selectors.article_date` in sources.yaml. Never serialised.
    date_selector: str | None = None
    # True for records reloaded from the previous run's output. A freshly
    # scraped record always wins a tie, because the old one may predate a
    # parser fix — exactly what happened when Lankadeepa's fabricated
    # timestamps outlived the fix that corrected them. Never serialised.
    stale: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items()
                if v is not None
                and k not in ("source_domain", "date_estimated",
                              "date_selector", "stale")}

    @property
    def published_dt(self) -> datetime:
        return dateparser.parse(self.published)


def canonical_url(url: str, base: str | None = None) -> str:
    """
    Absolute, tracking-free URL — the one we store and open.

    Deliberately keeps `www.`: stripping it made every fetch of a BBC,
    Ada Derana or Divaina article cost an extra 301 round trip.
    """
    if base:
        url = urljoin(base, url)
    p = urlparse(url.strip())
    kept = [(k, v) for k, v in parse_qsl(p.query) if k.lower() not in JUNK_PARAMS]
    return urlunparse((
        "https" if p.scheme in ("", "http", "https") else p.scheme,
        p.netloc.lower(),
        p.path.rstrip("/") or "/",
        "",
        urlencode(kept),
        "",
    ))


def dedupe_key(url: str) -> str:
    """Host-normalised form, used only for identity. Never fetched."""
    p = urlparse(canonical_url(url))
    return urlunparse((p.scheme, p.netloc.removeprefix("www."), p.path, "", p.query, ""))


def article_id(url: str) -> str:
    return hashlib.sha1(dedupe_key(url).encode("utf-8")).hexdigest()[:16]


def clean_text(html_or_text: str | None, limit: int = 220) -> str | None:
    """
    Strip markup and collapse whitespace, then truncate.

    We keep excerpts short on purpose. Republishing a publisher's full text
    turns an aggregator into a competitor and is how you get a takedown.
    Headline plus ~200 chars plus a link is the normal, defensible shape.
    """
    if not html_or_text:
        return None
    soup = BeautifulSoup(html_or_text, "lxml")
    # Nav, share widgets and related-article rails pollute excerpts. Drop them
    # before extracting text — lk_news filters these out line by line instead.
    for junk in soup.select("nav, header, footer, script, style, aside, .share, .related"):
        junk.decompose()
    text = soup.get_text(" ")
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return None
    if len(text) <= limit:
        return text
    cut = text[:limit].rsplit(" ", 1)[0]
    return cut.rstrip(".,;:") + "\u2026"


# ---------------------------------------------------------------------------
# Sinhala / Tamil dates
#
# dateutil silently mis-parses these. Lankadeepa's article pages carry
# "2026 සැප්තැම්බර් 12 | ප.ව. 06:34", which dateutil reads as 12 DECEMBER —
# it ignores the Sinhala month name and takes the day number as the month.
# A wrong date is worse than an obviously-missing one, so these are parsed
# explicitly and dateutil is never allowed near them.
# ---------------------------------------------------------------------------

SI_MONTHS = {
    "ජනවාරි": 1, "පෙබරවාරි": 2, "මාර්තු": 3, "අප්‍රේල්": 4, "අප්රේල්": 4,
    "මැයි": 5, "ජූනි": 6, "ජුනි": 6, "ජූලි": 7, "ජුලි": 7, "අගෝස්තු": 8,
    "සැප්තැම්බර්": 9, "සැප්තැම්": 9, "ඔක්තෝබර්": 10, "නොවැම්බර්": 11,
    "දෙසැම්බර්": 12,
}
TA_MONTHS = {
    "ஜனவரி": 1, "பிப்ரவரி": 2, "மார்ச்": 3, "ஏப்ரல்": 4, "மே": 5,
    "ஜூன்": 6, "ஜூலை": 7, "ஆகஸ்ட்": 8, "செப்டம்பர்": 9, "அக்டோபர்": 10,
    "நவம்பர்": 11, "டிசம்பர்": 12,
}
LOCAL_MONTHS = {**SI_MONTHS, **TA_MONTHS}

# පෙ.ව. = forenoon, ප.ව. = afternoon. Check the AM forms first: "ප.ව." is a
# substring of "පෙ.ව." once dots are stripped in some renderings.
AM_MARKERS = ("පෙ.ව", "පෙව", "முற்பகல்", "காலை", "am")
PM_MARKERS = ("ප.ව", "පව", "பிற்பகல்", "மாலை", "இரவு", "pm")


def parse_local_date(text: str) -> datetime | None:
    """Parse a Sinhala or Tamil date string, or return None."""
    if not text:
        return None
    month = next((v for k, v in LOCAL_MONTHS.items() if k in text), None)
    if month is None:
        return None

    year_m = re.search(r"\b(20\d{2})\b", text)
    if not year_m:
        return None
    year = int(year_m.group(1))

    rest = text[:year_m.start()] + " " + text[year_m.end():]
    time_m = re.search(r"(\d{1,2})[:.](\d{2})", rest)
    hour = minute = 0
    if time_m:
        hour, minute = int(time_m.group(1)), int(time_m.group(2))
        rest = rest[:time_m.start()] + " " + rest[time_m.end():]

    day_m = re.search(r"\b(\d{1,2})\b", rest)
    if not day_m:
        return None
    day = int(day_m.group(1))

    low = text.lower()
    if time_m:
        if any(m in low for m in AM_MARKERS):
            if hour == 12:
                hour = 0
        elif any(m in low for m in PM_MARKERS):
            if hour < 12:
                hour += 12

    try:
        return datetime(year, month, day, hour, minute,
                        tzinfo=timezone(timedelta(hours=5, minutes=30)))
    except ValueError:
        return None


def to_iso(value: Any) -> str | None:
    """Normalize any timestamp to ISO-8601 UTC. Rejects absurd dates."""
    if value is None:
        return None
    try:
        if isinstance(value, (tuple, list)) and len(value) >= 6:
            dt = datetime(*value[:6], tzinfo=timezone.utc)
        elif isinstance(value, datetime):
            dt = value
        else:
            dt = parse_local_date(str(value)) or dateparser.parse(str(value))
    except (ValueError, TypeError, OverflowError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        # Sri Lanka Standard Time. Naive timestamps from .lk sites are local.
        dt = dt.replace(tzinfo=timezone(timedelta(hours=5, minutes=30)))
    dt = dt.astimezone(timezone.utc)

    now = datetime.now(timezone.utc)
    if dt > now + timedelta(days=2) or dt < now - timedelta(days=400):
        return None
    return dt.isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def make_client(user_agent: str | None = None, timeout: int | None = None) -> httpx.Client:
    return httpx.Client(
        headers={
            "User-Agent": user_agent or DEFAULT_UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,si;q=0.8,ta;q=0.8",
        },
        # Without a consent cookie every news.google.com redirect lands on
        # consent.google.com from an EU/UK IP and resolution silently fails.
        cookies={"CONSENT": "YES+cb", "SOCS": "CAISHAgBEhJnd3NfMjAyNDAxMDEtMF9SQzEaAmVuIAEaBgiA_LyaBg"},
        timeout=timeout or 30,
        follow_redirects=True,
    )


# Edge responses that are often transient rather than a hard block.
RETRY_STATUSES = {202, 429, 500, 502, 503, 504}


def get(client: httpx.Client, url: str, quiet: bool = False,
        attempts: int = 3) -> httpx.Response | None:
    """
    Fetch a URL, or None. Failures here are expected and non-fatal: a source
    can be down, rate-limiting us, or serving a bot challenge.

    `quiet` is for per-article enrichment, where a failure costs an image
    rather than an article and would otherwise produce hundreds of log lines.
    """
    last = ""
    for attempt in range(attempts):
        try:
            r = client.get(url)
            if r.status_code in RETRY_STATUSES and attempt < attempts - 1:
                last = f"HTTP {r.status_code}"
                time.sleep(1.5 * (attempt + 1))
                continue
            r.raise_for_status()
            return r
        except httpx.TimeoutException as e:
            last = f"{type(e).__name__}"
            if attempt < attempts - 1:
                time.sleep(1.5 * (attempt + 1))
                continue
        except httpx.HTTPError as e:
            last = str(e).split("\n")[0][:110]
            break
    (log.debug if quiet else log.warning)("GET failed %s: %s", url[:90], last)
    return None


# ---------------------------------------------------------------------------
# Adapters
# ---------------------------------------------------------------------------

def from_rss(client: httpx.Client, src: dict, limit: int = 40) -> list[Article]:
    """Adapter 1: a real RSS/Atom feed. Cheapest when it exists."""
    feeds = src.get("feeds") or ([src["feed"]] if src.get("feed") else [])
    if not feeds:
        raise ValueError(f"{src['id']}: adapter=rss but no `feed`/`feeds` in sources.yaml")

    entries = []
    for feed_url in feeds:
        r = get(client, feed_url)
        if r is None:
            continue
        entries.extend(feedparser.parse(r.content).entries)

    out: list[Article] = []
    for e in entries[:limit * len(feeds)]:
        link = e.get("link")
        title = (e.get("title") or "").strip()
        if not link or not title:
            continue
        published = to_iso(e.get("published_parsed") or e.get("updated_parsed")
                           or e.get("published") or e.get("updated"))
        if not published:
            continue

        image = None
        for mc in (e.get("media_content") or []):
            if mc.get("url"):
                image = mc["url"]
                break
        if not image:
            for enc in (e.get("enclosures") or []):
                if str(enc.get("type", "")).startswith("image/"):
                    image = enc.get("href")
                    break

        url = canonical_url(link, src["site"])
        out.append(Article(
            id=article_id(url),
            source_id=src["id"],
            source_name=src["name"],
            lang=src["lang"],
            title=title,
            url=url,
            published=published,
            excerpt=clean_text(e.get("summary") or e.get("description")),
            image=image,
        ))
    return out


def from_wordpress(client: httpx.Client, src: dict, limit: int = 40) -> list[Article]:
    """
    Adapter 2: the WordPress REST API.

    Many Sri Lankan outlets run WordPress and expose /wp-json even when the
    RSS feed is missing or truncated. `_embed` pulls the featured image in
    the same request, which saves one fetch per article.
    """
    endpoint = f"{src['site'].rstrip('/')}/wp-json/wp/v2/posts"
    r = get(client, f"{endpoint}?_embed=1&per_page={min(limit, 50)}")
    if r is None:
        return []

    try:
        posts = r.json()
    except ValueError:
        log.warning("%s: wp-json returned non-JSON", src["id"])
        return []
    if not isinstance(posts, list):
        return []

    out: list[Article] = []
    for p in posts:
        link = p.get("link")
        title = clean_text((p.get("title") or {}).get("rendered"), limit=300)
        if not link or not title:
            continue
        published = to_iso(p.get("date_gmt") or p.get("date"))
        if not published:
            continue

        image = None
        embedded = (p.get("_embedded") or {}).get("wp:featuredmedia") or []
        if embedded and isinstance(embedded[0], dict):
            image = embedded[0].get("source_url")

        url = canonical_url(link, src["site"])
        out.append(Article(
            id=article_id(url),
            source_id=src["id"],
            source_name=src["name"],
            lang=src["lang"],
            title=title,
            url=url,
            published=published,
            excerpt=clean_text((p.get("excerpt") or {}).get("rendered")),
            image=image,
        ))
    return out


def discover_sitemaps(client: httpx.Client, site: str) -> list[str]:
    """Find news sitemaps via robots.txt, then fall back to common paths."""
    found: list[str] = []
    r = get(client, f"{site.rstrip('/')}/robots.txt")
    if r is not None:
        for line in r.text.splitlines():
            if line.lower().startswith("sitemap:"):
                found.append(line.split(":", 1)[1].strip())
    for guess in ("news-sitemap.xml", "sitemap-news.xml", "sitemap_news.xml",
                  "news.xml", "sitemap.xml"):
        candidate = f"{site.rstrip('/')}/{guess}"
        if candidate not in found:
            found.append(candidate)
    # News sitemaps first — they carry publish dates and titles.
    found.sort(key=lambda u: 0 if "news" in u.lower() else 1)
    return found


def from_sitemap(client: httpx.Client, src: dict, limit: int = 40) -> list[Article]:
    """
    Adapter 3: Google News sitemaps.

    Any outlet that wants to appear in Google News publishes one. It is
    structured XML with title, publication date and language — better than
    scraping and nearly as reliable as RSS.
    """
    urls = [src["sitemap"]] if src.get("sitemap") else discover_sitemaps(client, src["site"])

    for sm_url in urls:
        r = get(client, sm_url)
        if r is None:
            continue
        soup = BeautifulSoup(r.content, "xml")

        # A sitemap index points at other sitemaps; follow the first news-ish one.
        if soup.find("sitemapindex"):
            children = [loc.get_text(strip=True) for loc in soup.find_all("loc")]
            news_children = [c for c in children if "news" in c.lower()] or children[:1]
            urls.extend(c for c in news_children[:2] if c not in urls)
            continue

        out: list[Article] = []
        for node in soup.find_all("url")[:limit]:
            loc = node.find("loc")
            if not loc:
                continue
            news = node.find("news")
            title = None
            published = None
            if news:
                t = news.find("title")
                d = news.find("publication_date")
                title = t.get_text(strip=True) if t else None
                published = to_iso(d.get_text(strip=True)) if d else None
            if not published:
                lm = node.find("lastmod")
                published = to_iso(lm.get_text(strip=True)) if lm else None
            if not title or not published:
                continue

            url = canonical_url(loc.get_text(strip=True), src["site"])
            out.append(Article(
                id=article_id(url),
                source_id=src["id"],
                source_name=src["name"],
                lang=src["lang"],
                title=title,
                url=url,
                published=published,
            ))
        if out:
            return out
    return []


def from_html(client: httpx.Client, src: dict, limit: int = 40) -> list[Article]:
    """
    Adapter 4: scrape a listing page. Last resort, most fragile.

    Selectors live in sources.yaml so a layout change is a config edit and a
    re-run, not a code change and certainly not an app release.
    """
    listings = src.get("listings") or ([src["listing"]] if src.get("listing") else [src["site"]])
    sel = src.get("selectors") or {}

    nodes = []
    for listing in listings:
        r = get(client, listing)
        if r is None:
            continue
        found = BeautifulSoup(r.text, "lxml").select(sel.get("item", "article"))
        if not found:
            log.warning("%s: item selector matched nothing on %s", src["id"], listing)
        nodes.extend(found)
    if not nodes:
        return []

    out: list[Article] = []
    seen: set[str] = set()
    for node in nodes[:limit * 3]:
        a = node.select_one(sel.get("link", "a"))
        href = a.get("href") if a else None
        if not href:
            continue
        url = canonical_url(href, src["site"])
        if url in seen:
            continue
        seen.add(url)

        t_node = node.select_one(sel["title"]) if sel.get("title") else None
        title = clean_text((t_node or a).get_text(), limit=300)
        if not title:
            continue

        img = node.select_one(sel.get("image", "img"))
        image = None
        if img:
            raw = img.get("src") or img.get("data-src") or img.get("data-lazy-src")
            if raw:
                image = urljoin(src["site"], raw)

        published, estimated = None, False
        if sel.get("date"):
            d_node = node.select_one(sel["date"])
            if d_node is not None:
                published = to_iso(d_node.get("datetime")
                                   or d_node.get("content")
                                   or d_node.get_text(strip=True))
        if published is None:
            published, estimated = to_iso(datetime.now(timezone.utc)), True

        out.append(Article(
            id=article_id(url),
            source_id=src["id"],
            source_name=src["name"],
            lang=src["lang"],
            title=title,
            url=url,
            published=published,
            image=image,
            date_estimated=estimated,
            date_selector=sel.get("article_date"),
        ))
        if len(out) >= limit:
            break
    return out


# Google News RSS search. Works on nearly any indexed site, which makes it the
# safety net for outlets that block us or publish nothing machine-readable.
# Two real costs: links are Google redirects (we resolve them below), and
# indexing lags the publisher by minutes to hours.
GOOGLE_HL = {"en": "en-LK", "si": "si", "ta": "ta"}


def _google_feed_url(query: str, hl: str, gl: str, ceid: str) -> str:
    return ("https://news.google.com/rss/search"
            f"?q={quote_plus(query)}&hl={hl}&gl={gl}&ceid={ceid}")


def _google_variants(src: dict) -> list[str]:
    """
    Query forms to try, best first.

    A single `site:x when:7d` with hl=si&gl=LK returns almost nothing for
    Sinhala outlets — Ada Derana English yields 40 results this way while
    Ada Derana Sinhala yields 1. Dropping the recency filter and retrying in
    the regional edition Google actually serves recovers most of them.
    """
    domain = urlparse(src["site"]).netloc.lower().removeprefix("www.")
    lang = src["lang"]
    hl = GOOGLE_HL.get(lang, "en-LK")
    base = src.get("google_query") or f"site:{domain}"

    variants = [
        (f"{base} when:7d", hl, "LK", f"LK:{lang}"),
        (base, hl, "LK", f"LK:{lang}"),
        (base, "en-US", "US", "US:en"),
    ]
    # Tamil content is largely indexed under the Indian edition; Google
    # redirects LK:ta there anyway, so ask for it directly.
    if lang == "ta":
        variants.insert(2, (base, "ta", "IN", "IN:ta"))
    # Some outlets publish a language section under the parent domain.
    parent = ".".join(domain.split(".")[-2:])
    if parent != domain and not src.get("google_query"):
        variants.append((f"site:{parent} {lang}", hl, "LK", f"LK:{lang}"))

    return [_google_feed_url(q, h, g, c) for q, h, g, c in variants]


def _parse_google_feed(content: bytes, src: dict, domain: str,
                       limit: int) -> list[Article]:
    out: list[Article] = []
    for e in feedparser.parse(content).entries[:limit]:
        link, title = e.get("link"), (e.get("title") or "").strip()
        if not link or not title:
            continue
        # Google appends " - Publisher" to every headline.
        title = re.sub(r"\s+-\s+[^-]{2,40}$", "", title).strip()
        published = to_iso(e.get("published_parsed") or e.get("published"))
        if not published:
            continue
        out.append(Article(
            id=article_id(link),
            source_id=src["id"],
            source_name=src["name"],
            lang=src["lang"],
            title=title,
            url=link,
            published=published,
            source_domain=domain,
        ))
    return out


def from_google_news(client: httpx.Client, src: dict, limit: int = 40) -> list[Article]:
    """
    Adapter 5: last resort. Never pin this if a direct adapter works.

    Tries several query forms and keeps the most productive, because a single
    form silently returns near-zero for some outlets.
    """
    domain = urlparse(src["site"]).netloc.lower().removeprefix("www.")
    best: list[Article] = []
    for url in _google_variants(src):
        r = get(client, url, quiet=True)
        if r is None:
            continue
        items = _parse_google_feed(r.content, src, domain, limit)
        if len(items) > len(best):
            best = items
        # Good enough — stop paying for more requests.
        if len(best) >= min(limit, 20):
            break
    return best


def resolve_google_url(
    client: httpx.Client, url: str, expect_domain: str
) -> tuple[str, httpx.Response | None]:
    """
    Turn a news.google.com link into the publisher's real URL.

    Returns (url, response); the response is reused for og:image so we don't
    fetch the same page twice. Falls back to the Google link, which still
    redirects correctly in a real browser even when it fails for us.
    """
    if "news.google.com" not in url:
        return url, None

    decoded = _decode_via_batchexecute(client, url)
    if decoded and (not expect_domain or expect_domain in urlparse(decoded).netloc):
        return canonical_url(decoded), None

    # Strategy 2: follow the redirect, clearing the consent wall if we hit it.
    for attempt in (1, 2):
        try:
            r = client.get(url)
        except httpx.HTTPError:
            return url, None
        host = urlparse(str(r.url)).netloc
        if not any(h in host for h in GOOGLE_HOSTS):
            return canonical_url(str(r.url)), r
        if "consent.google.com" in host and attempt == 1 and _clear_consent(client, r):
            continue
        if expect_domain:
            m = re.search(
                rf'https?://[^"\'\s<>\\]*{re.escape(expect_domain)}[^"\'\s<>\\]*',
                r.text[:200_000], re.I)
            if m:
                return canonical_url(m.group(0).replace("&amp;", "&")), None
        break
    return url, None


# Cheapest and most durable first. google is last on purpose: it always works,
# so anything that ranks it above a direct adapter will quietly stop us from
# ever fixing the direct one.
ADAPTER_PREFERENCE = ["rss", "wordpress", "sitemap", "html", "google"]

ADAPTERS = {
    "rss": from_rss,
    "wordpress": from_wordpress,
    "sitemap": from_sitemap,
    "html": from_html,
    "google": from_google_news,
}


def primary_endpoint(client: httpx.Client, src: dict) -> str:
    """The single URL this source depends on, for diagnostics."""
    adapter = src.get("adapter", "rss")
    if adapter == "rss":
        feeds = src.get("feeds") or ([src["feed"]] if src.get("feed") else [])
        return feeds[0] if feeds else src["site"]
    if adapter == "wordpress":
        return f"{src['site'].rstrip('/')}/wp-json/wp/v2/posts?per_page=1"
    if adapter == "sitemap":
        return src.get("sitemap") or discover_sitemaps(client, src["site"])[0]
    if adapter == "html":
        return (src.get("listings") or [src.get("listing") or src["site"]])[0]
    domain = urlparse(src["site"]).netloc.lower().removeprefix("www.")
    hl = GOOGLE_HL.get(src["lang"], "en-LK")
    return ("https://news.google.com/rss/search"
            f"?q={quote_plus('site:' + domain)}&hl={hl}&gl=LK&ceid=LK:{src['lang']}")


def diagnose_endpoint(client: httpx.Client, src: dict) -> dict[str, Any]:
    """
    Why did this source return nothing?

    "0 articles" is useless in a CI log — a 403 from bot protection, a 404
    from a moved endpoint and a timeout all need different fixes, and the
    answer can differ between a home connection and a datacentre IP.
    """
    try:
        url = primary_endpoint(client, src)
    except Exception as e:
        return {"error": f"could not build endpoint: {e}"}
    info: dict[str, Any] = {"endpoint": url}
    try:
        r = client.get(url)
        info.update(
            status=r.status_code,
            server=r.headers.get("server", "")[:40],
            content_type=r.headers.get("content-type", "").split(";")[0],
            bytes=len(r.content),
            final_url=str(r.url)[:160] if str(r.url) != url else None,
        )
        if r.status_code in (403, 503) and "cloudflare" in info["server"].lower():
            info["hint"] = "Cloudflare is blocking this IP"
        elif r.status_code == 404:
            info["hint"] = "endpoint moved — re-run probe.py"
        elif r.status_code == 200 and info["bytes"] < 500:
            info["hint"] = "200 but almost empty — likely a challenge page"
        elif r.status_code == 200:
            info["hint"] = "200 with content — our parser is the problem"
    except httpx.HTTPError as e:
        info["exception"] = f"{type(e).__name__}: {str(e).splitlines()[0][:90]}"
    return {k: v for k, v in info.items() if v is not None}


# ---------------------------------------------------------------------------
# Enrichment and dedupe
# ---------------------------------------------------------------------------

def enrich(client: httpx.Client, art: Article) -> Article:
    """
    Fetch the article page for og:image / og:description / article:published_time.

    Costs one request per article, so only call it for articles missing an
    image — that is what makes the feed look like a news app instead of a
    list of links.
    """
    r = None
    if "news.google.com" in art.url:
        # The publisher's own domain, derived from the source registry.
        expect = art.source_domain or ""
        resolved, r = resolve_google_url(client, art.url, expect)
        if resolved != art.url:
            art.url = resolved
            art.id = article_id(resolved)

    if r is None:
        r = get(client, art.url, quiet=True)
    if r is None:
        return art
    soup = BeautifulSoup(r.text, "lxml")

    def meta(*names: str) -> str | None:
        for n in names:
            tag = (soup.find("meta", property=n) or soup.find("meta", attrs={"name": n}))
            if tag and tag.get("content"):
                return tag["content"].strip()
        return None

    if not art.image:
        img = meta("og:image", "twitter:image", "twitter:image:src")
        if img:
            art.image = urljoin(art.url, img)
    if not art.excerpt:
        art.excerpt = clean_text(meta("og:description", "description"))

    better = to_iso(meta("article:published_time", "og:article:published_time",
                         "publishdate", "pubdate", "date", "DC.date.issued"))
    if not better:
        for tag in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(tag.string or "{}")
            except (ValueError, TypeError):
                continue
            for obj in (data if isinstance(data, list) else [data]):
                if isinstance(obj, dict) and obj.get("datePublished"):
                    better = to_iso(obj["datePublished"])
                    break
            if better:
                break
    if not better:
        t = soup.find("time")
        if t is not None:
            better = to_iso(t.get("datetime") or t.get_text(strip=True))
    if not better and art.date_selector:
        node = soup.select_one(art.date_selector)
        if node is not None:
            better = to_iso(node.get("datetime") or node.get("content")
                            or node.get_text(strip=True))
    if better:
        art.published = better
        art.date_estimated = False
    return art


def dedupe(articles: Iterable[Article]) -> list[Article]:
    """
    Same URL from two adapters collapses to one. Near-identical headlines
    from the same outlet within an hour also collapse — wire copy gets
    republished under slightly different titles.
    """
    def quality(x: Article) -> tuple[int, int, int]:
        """Higher is better: fresh beats cached, real date beats estimated,
        then richer metadata."""
        return (
            0 if x.stale else 1,
            0 if x.date_estimated else 1,
            sum(bool(v) for v in (x.image, x.excerpt, x.category)),
        )

    by_id: dict[str, Article] = {}
    for a in articles:
        existing = by_id.get(a.id)
        if existing is None or quality(a) > quality(existing):
            by_id[a.id] = a

    out = sorted(by_id.values(), key=lambda x: x.published, reverse=True)

    kept: list[Article] = []
    seen_titles: dict[tuple[str, str], datetime] = {}
    for a in out:
        key = (a.source_id, re.sub(r"\W+", "", a.title.lower())[:60])
        prev = seen_titles.get(key)
        if prev and abs((a.published_dt - prev).total_seconds()) < 3600:
            continue
        seen_titles[key] = a.published_dt
        kept.append(a)
    return kept
