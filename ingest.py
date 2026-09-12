#!/usr/bin/env python3
"""
Build the static JSON API.

Reads sources.yaml, runs each outlet's adapter, merges with whatever was
published last run, and writes:

    public/v1/feed_en.json      latest 150 English articles
    public/v1/feed_si.json      latest 150 Sinhala
    public/v1/feed_ta.json      latest 150 Tamil
    public/v1/feed_en_politics.json   ... per category
    public/v1/sources.json      outlet directory for the app
    public/v1/status.json       per-source health, for you not the app

Merging with the previous run matters: if a site is down for one cycle its
articles stay in the feed instead of vanishing from users' apps.

    python3 ingest.py --out public
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path

import yaml
from dateutil import parser as dateparser

from pipeline import (ADAPTERS, Article, dedupe, diagnose_endpoint, enrich,
                      make_client)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ingest")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

API_VERSION = "v1"
PER_FEED = 150
RETENTION_DAYS = 14
CATEGORIES = ["politics", "business", "sports", "tech", "international",
              "entertainment", "society"]


def load_previous(outdir: Path, lang: str) -> list[dict]:
    path = outdir / API_VERSION / f"feed_{lang}.json"
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8")).get("articles", [])
    except (ValueError, OSError) as e:
        log.warning("could not read previous %s: %s", path, e)
        return []


def fetch_source(client, src: dict, max_items: int) -> tuple[str, list[Article], dict]:
    """
    Fetch one source, falling back to Google News if its own site refuses us.

    This matters because a datacentre IP gets treated very differently from a
    home connection: Ada Derana answers a UK browser but returns 403 from
    CloudFront to a CI runner, and Divaina does the same via Cloudflare. Google
    News reaches all of them, so a blocked source degrades to a slower, less
    complete feed instead of disappearing.

    The fallback is recorded, not silent — `status.json` shows which sources
    are running on it so the primary adapter still gets fixed.
    """
    name = src.get("adapter", "rss")
    adapter = ADAPTERS.get(name)
    if adapter is None:
        return src["id"], [], {"ok": False, "error": f"unknown adapter {name!r}"}

    try:
        items = adapter(client, src, limit=max_items)
    except Exception as e:
        items, primary_err = [], f"{type(e).__name__}: {e}"
    else:
        primary_err = None

    if items:
        return src["id"], items, {"ok": True, "error": None}

    d = diagnose_endpoint(client, src)
    detail = primary_err or " ".join(
        f"{k}={v}" for k, v in d.items() if k != "endpoint")

    # Last resort: Google News indexes nearly every one of these outlets and
    # is not IP-blocked from CI.
    if name != "google" and src.get("fallback", True):
        try:
            items = ADAPTERS["google"](client, src, limit=max_items)
        except Exception:
            items = []
        MIN_USEFUL = 3
        if len(items) >= MIN_USEFUL:
            return src["id"], items, {
                "ok": True, "error": None, "used_fallback": "google",
                "primary_error": f"0 articles ({detail})",
            }
        if items:
            # Better than nothing, but don't let it look healthy.
            return src["id"], items, {
                "ok": False, "used_fallback": "google",
                "primary_error": f"0 articles ({detail})",
                "error": f"primary blocked and google returned only "
                         f"{len(items)} article(s)",
            }

    return src["id"], [], {"ok": False, "error": f"0 articles ({detail})"}


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    tmp.replace(path)  # atomic — readers never see a half-written file


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="public")
    ap.add_argument("--config", default="sources.yaml")
    ap.add_argument("--no-enrich", action="store_true",
                    help="skip per-article og:image fetches (much faster)")
    ap.add_argument("--workers", type=int, default=6)
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config, encoding="utf-8"))
    defaults = cfg.get("defaults", {})
    sources = [s for s in cfg["sources"] if s.get("enabled", True)]
    parked = len(cfg["sources"]) - len(sources)
    outdir = Path(args.out)
    if parked:
        log.info("%d source(s) parked via `enabled: false`", parked)

    client = make_client(
        user_agent=defaults.get("user_agent"),
        timeout=defaults.get("timeout", 20),
    )
    max_items = defaults.get("max_items", 40)

    # ---- fetch every source in parallel -----------------------------------
    fresh: list[Article] = []
    status: dict[str, dict] = {}

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(fetch_source, client, s, max_items): s for s in sources}
        for fut in as_completed(futures):
            src = futures[fut]
            sid, items, outcome = fut.result()
            status[sid] = {
                "name": src["name"],
                "lang": src["lang"],
                "adapter": src.get("adapter"),
                "count": len(items),
                **outcome,
            }
            if not outcome["ok"]:
                log.warning("%-20s FAILED  %s", sid, outcome["error"])
                fresh.extend(items)   # keep whatever little we did get
            elif outcome.get("used_fallback"):
                log.warning("%-20s %3d articles via GOOGLE FALLBACK (%s)",
                            sid, len(items), outcome["primary_error"][:70])
                fresh.extend(items)
            else:
                log.info("%-20s %3d articles", sid, len(items))
                fresh.extend(items)

    if not fresh:
        log.error("Every source failed. Refusing to overwrite good feeds with nothing.")
        return 1

    # ---- fill in missing images -------------------------------------------
    if not args.no_enrich:
        # sitemap and google adapters return no image at all, so most of the
        # feed depends on this step. Prioritise the newest articles — those
        # are the ones users actually see.
        google = [a for a in fresh if "news.google.com" in a.url]
        # A fabricated date is worse than a missing image: it pins the source
        # to the top of the feed forever and shows users the wrong time.
        undated = [a for a in fresh
                   if a.date_estimated and "news.google.com" not in a.url]
        imageless = [a for a in fresh
                     if "news.google.com" not in a.url
                     and not a.date_estimated and not a.image]
        needs = google + undated + sorted(
            imageless, key=lambda a: a.published, reverse=True)[:250]
        log.info("enriching %d articles (%d google links, %d undated, "
                 "%d missing images)", len(needs), len(google), len(undated),
                 len(needs) - len(google) - len(undated))
        with ThreadPoolExecutor(max_workers=max(args.workers, 10)) as pool:
            list(pool.map(lambda a: enrich(client, a), needs))
        resolved = len(google) - sum(
            1 for a in google if "news.google.com" in a.url)
        got_image = sum(1 for a in needs if a.image)
        still_estimated = sum(1 for a in fresh if a.date_estimated)
        log.info("enriched: %d/%d google links resolved, %d dates recovered, "
                 "%d images found", resolved, len(google),
                 len(undated) - sum(1 for a in undated if a.date_estimated),
                 got_image)
        if still_estimated:
            log.warning("%d article(s) still carry a scrape-time date "
                        "(no og:published_time on the page)", still_estimated)
        # enrich() rewrites google redirect URLs, which changes article ids.
        fresh = dedupe(fresh)

    google_total = sum(1 for a in fresh if a.source_id and "news.google.com" in a.url)
    if google_total:
        log.warning("%d article(s) still point at news.google.com "
                    "(consent wall or unresolvable id)", google_total)

    # ---- merge with the previous run, then write ---------------------------
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    generated = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    totals: dict[str, int] = {}

    for lang in ("en", "si", "ta"):
        previous = [
            Article(stale=True,
                    **{k: v for k, v in d.items()
                       if k in Article.__dataclass_fields__ and k != "stale"})
            for d in load_previous(outdir, lang)
        ]
        combined = [a for a in fresh if a.lang == lang] + previous
        merged = [a for a in dedupe(combined) if a.published_dt >= cutoff]
        totals[lang] = len(merged)

        write_json(outdir / API_VERSION / f"feed_{lang}.json", {
            "version": API_VERSION,
            "lang": lang,
            "generated": generated,
            "count": len(merged[:PER_FEED]),
            "articles": [a.to_dict() for a in merged[:PER_FEED]],
        })

        for cat in CATEGORIES:
            subset = [a for a in merged if a.category == cat][:PER_FEED]
            if subset:
                write_json(outdir / API_VERSION / f"feed_{lang}_{cat}.json", {
                    "version": API_VERSION,
                    "lang": lang,
                    "category": cat,
                    "generated": generated,
                    "count": len(subset),
                    "articles": [a.to_dict() for a in subset],
                })

        log.info("feed_%s.json  %d articles", lang, len(merged[:PER_FEED]))

    write_json(outdir / API_VERSION / "sources.json", {
        "version": API_VERSION,
        "generated": generated,
        "sources": [
            {"id": s["id"], "name": s["name"], "lang": s["lang"], "site": s["site"]}
            for s in sources
        ],
    })

    ok = sum(1 for v in status.values() if v["ok"])
    on_fallback = sorted(k for k, v in status.items() if v.get("used_fallback"))
    # Carry forward "last time this source produced anything", so a source
    # that dies quietly shows up as days-since rather than a single red run.
    # lk_news publishes the same idea as custom_summary.json; the difference
    # is that this one has memory, which is what makes alerting possible.
    history_path = outdir / API_VERSION / "status.json"
    previous_detail = {}
    if history_path.exists():
        try:
            previous_detail = json.loads(history_path.read_text(encoding="utf-8")).get("detail", {})
        except (ValueError, OSError):
            pass

    for sid, info in status.items():
        if info["ok"] and info["count"]:
            info["last_ok"] = generated
        else:
            info["last_ok"] = (previous_detail.get(sid) or {}).get("last_ok")
        if info["last_ok"]:
            delta = datetime.now(timezone.utc) - dateparser.parse(info["last_ok"])
            info["hours_since_ok"] = round(delta.total_seconds() / 3600, 1)
        else:
            info["hours_since_ok"] = None

    dead = sorted(
        sid for sid, i in status.items()
        if i["hours_since_ok"] is None or i["hours_since_ok"] > 48
    )

    write_json(history_path, {
        "generated": generated,
        "sources_ok": ok,
        "sources_total": len(sources),
        "totals": totals,
        "unresolved_google_links": google_total,
        "on_fallback": on_fallback,
        "dead": dead,
        "detail": status,
    })

    if dead:
        log.warning("dead >48h (%d): %s", len(dead), ", ".join(dead))

    if on_fallback:
        log.warning("%d source(s) running on the Google fallback: %s",
                    len(on_fallback), ", ".join(on_fallback))
    log.info("done: %d/%d sources healthy (%d on fallback)",
             ok, len(sources), len(on_fallback))

    # Fail the CI job only when the output is genuinely unusable, so you find
    # out from a notification rather than from a user review — but a couple of
    # blocked sources running on the fallback is not a build failure.
    empty_langs = [lang for lang, n in totals.items() if n == 0]
    if empty_langs:
        log.error("FAILING: no articles at all for %s", ", ".join(empty_langs))
        return 1
    if ok < len(sources) * 0.4:
        log.error("FAILING: only %d of %d sources produced articles. "
                  "See status.json `detail` for per-source errors.",
                  ok, len(sources))
        return 1
    if ok < len(sources):
        log.warning("%d of %d sources produced nothing — run continues",
                    len(sources) - ok, len(sources))
    return 0


if __name__ == "__main__":
    sys.exit(main())
