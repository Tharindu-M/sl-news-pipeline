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
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse

import yaml
from dateutil import parser as dateparser

from pipeline import (ADAPTERS, ADAPTER_PREFERENCE, Article, dedupe,
                      diagnose_endpoint, enrich, make_client, is_google_news,
                      valid_web_url, freshest_age_hours, FRESH_WINDOW_HOURS)

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


def load_previous_payload(outdir: Path, name: str) -> dict | None:
    """The last accepted file's full contents, verbatim — used to preserve a
    language's snapshot (and its original `generated` timestamp) unchanged
    when this run's output doesn't clear the quality bar for it."""
    path = outdir / API_VERSION / name
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, OSError) as e:
        log.warning("could not read previous %s: %s", path, e)
        return None


def _can_try(adapter: str, src: dict) -> bool:
    """Whether a source has the config an adapter needs to run at all."""
    if adapter == "rss":
        return bool(src.get("feeds") or src.get("feed"))
    if adapter == "html":
        return bool(src.get("selectors"))
    if adapter == "newsfirst":
        return bool(src.get("api"))
    return True


def fetch_source(client, src: dict, max_items: int) -> tuple[str, list[Article], dict]:
    """
    Fetch one source, walking down the adapter ladder if its site refuses us
    OR if it answered with only stale/unverified content.

    A datacentre IP is treated very differently from a home connection: Ada
    Derana answers a UK browser but returns 403 from CloudFront to a CI
    runner, and Divaina does the same via Cloudflare. Those rules are usually
    per-endpoint rather than per-domain, so a blocked /wp-json often sits
    beside a perfectly open RSS feed — worth trying before resorting to Google.

    A non-empty result is not the same as a *usable* one: a frozen sitemap
    index or an abandoned feed can return HTTP 200 with years-old articles
    every single run. "usable" here means at least one verified (not
    scrape-time-estimated) publish date within the source's freshness
    window; anything else keeps walking the fallback ladder.

    Any fallback is recorded rather than silent: status.json names the adapter
    actually used and keeps the original error, so the primary still gets
    fixed instead of quietly rotting behind a green tick.
    """
    name = src.get("adapter", "rss")
    adapter = ADAPTERS.get(name)
    if adapter is None:
        return src["id"], [], {"ok": False, "error": f"unknown adapter {name!r}"}

    window = src.get("max_age_hours", FRESH_WINDOW_HOURS)

    def usable(items: list[Article]) -> bool:
        age = freshest_age_hours(items)
        return age is not None and age <= window

    try:
        items = adapter(client, src, limit=max_items)
    except Exception as e:
        items, primary_err = [], f"{type(e).__name__}: {e}"
    else:
        primary_err = None

    if items and usable(items):
        return src["id"], items, {"ok": True, "error": None}

    d = diagnose_endpoint(client, src)
    detail = primary_err or " ".join(
        f"{k}={v}" for k, v in d.items() if k != "endpoint")
    if items and not primary_err:
        detail = f"0 usable (verified-fresh) articles of {len(items)} returned"

    # Primary produced something, even if stale — keep it as the floor to beat.
    best_items, best_label = items, None
    if src.get("fallback", True):
        for alt in ADAPTER_PREFERENCE:
            if alt == name or not _can_try(alt, src):
                continue
            try:
                alt_items = ADAPTERS[alt](client, src, limit=max_items)
            except Exception:
                alt_items = []
            if not alt_items:
                continue
            if usable(alt_items):
                best_items, best_label = alt_items, alt
                break
            if len(alt_items) > len(best_items):
                best_items, best_label = alt_items, alt

    if not best_items:
        return src["id"], [], {"ok": False, "error": f"0 articles ({detail})"}

    if best_label is None:
        # Only the primary produced anything, and it was not usable (stale
        # or undated). Report it honestly rather than a silent fallback.
        return src["id"], best_items, {
            "ok": False, "error": f"stale/unverified only ({detail})",
        }

    # A fallback returning one or two articles is not a working source.
    MIN_USEFUL = 3
    if len(best_items) >= MIN_USEFUL:
        return src["id"], best_items, {
            "ok": True, "error": None, "used_fallback": best_label,
            "primary_error": detail,
        }
    return src["id"], best_items, {
        "ok": False, "used_fallback": best_label,
        "primary_error": detail,
        "error": f"primary blocked/stale; {best_label} returned only "
                 f"{len(best_items)} article(s)",
    }


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    tmp.replace(path)  # atomic — readers never see a half-written file


def publish_snapshot(outdir: Path, payloads: dict[str, dict]) -> None:
    """Stage a complete version and roll back if directory promotion fails.

    Only one writer is supported. Pages deploys the accepted artifact as a
    unit; direct local readers may see a brief gap during directory renames.
    """
    outdir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".snapshot-", dir=outdir) as tmp:
        staging = Path(tmp) / API_VERSION
        for name, payload in payloads.items():
            write_json(staging / name, payload)
        target = outdir / API_VERSION
        backup = Path(tmp) / "previous"
        if target.exists():
            target.rename(backup)
        try:
            staging.rename(target)
        except OSError:
            if backup.exists():
                backup.rename(target)
            raise


def source_freshness(items: list[Article], previous: list[Article],
                     now: datetime, max_age_hours: float) -> dict:
    """Fetch success and publisher freshness are independent signals."""
    known = {a.id for a in previous}
    verified = [a.published_dt for a in dedupe(items + previous)
                if not a.date_estimated and a.published_dt <= now]
    newest = max(verified) if verified else None
    hours = (now - newest).total_seconds() / 3600 if newest else None
    return {
        "new_articles": len({a.id for a in items} - known),
        "newest_published": newest.isoformat().replace("+00:00", "Z") if newest else None,
        "hours_since_publication": round(hours, 1) if hours is not None else None,
        "stale_content": hours is None or hours > max_age_hours,
        "estimated_dates": sum(a.date_estimated for a in items),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="public")
    ap.add_argument("--config", default="sources.yaml")
    ap.add_argument("--no-enrich", action="store_true",
                    help="skip per-article og:image fetches (much faster)")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--diagnostics", default="diagnostics/status.json",
                    help="latest run report, written separately from accepted feeds")
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    defaults = cfg.get("defaults", {})
    sources = [s for s in cfg["sources"] if s.get("enabled", True)]
    parked = len(cfg["sources"]) - len(sources)
    outdir = Path(args.out)
    diagnostics = Path(args.diagnostics)
    if diagnostics.resolve().is_relative_to(outdir.resolve()):
        ap.error("--diagnostics must be outside --out")
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
                log.warning("%-20s %3d articles via %s fallback (%s)",
                            sid, len(items), outcome["used_fallback"].upper(),
                            outcome["primary_error"][:70])
                fresh.extend(items)
            else:
                log.info("%-20s %3d articles", sid, len(items))
                fresh.extend(items)

    # Publisher content is untrusted, even when its listing endpoint is trusted.
    registry = {s["id"]: s for s in sources}
    validated = []
    for art in fresh:
        host = urlparse(registry[art.source_id]["site"]).hostname
        if not (valid_web_url(art.url, (host,)) or is_google_news(art.url)):
            log.warning("Rejected unexpected article URL for %s", art.source_id)
            continue
        art.source_domain = host
        if art.image and not valid_web_url(art.image):
            art.image = None
        validated.append(art)
    fresh = validated
    collected = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    for art in fresh:
        art.collected_at = collected

    # ---- fill in missing images -------------------------------------------
    if not args.no_enrich:
        # sitemap and google adapters return no image at all, so most of the
        # feed depends on this step. Prioritise the newest articles — those
        # are the ones users actually see.
        google = [a for a in fresh if is_google_news(a.url)]
        # A fabricated date is worse than a missing image: it pins the source
        # to the top of the feed forever and shows users the wrong time.
        undated = [a for a in fresh
                   if a.date_estimated and not is_google_news(a.url)]
        imageless = [a for a in fresh
                     if not is_google_news(a.url)
                     and not a.date_estimated and not a.image]
        needs = google + undated + sorted(
            imageless, key=lambda a: a.published, reverse=True)[:250]
        log.info("enriching %d articles (%d google links, %d undated, "
                 "%d missing images)", len(needs), len(google), len(undated),
                 len(needs) - len(google) - len(undated))
        with ThreadPoolExecutor(max_workers=max(args.workers, 10)) as pool:
            list(pool.map(lambda a: enrich(client, a), needs))
        resolved = len(google) - sum(
            1 for a in google if is_google_news(a.url))
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

    client.close()
    google_total = sum(1 for a in fresh if is_google_news(a.url))
    if google_total:
        log.warning("%d article(s) still point at news.google.com "
                    "(consent wall or unresolvable id)", google_total)

    # ---- merge with the previous run, then write ---------------------------
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    generated = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    now_dt = dateparser.parse(generated)
    totals: dict[str, int] = {}
    payloads: dict[str, dict] = {}
    previous_all: list[Article] = []
    for art in fresh:
        art.collected_at = art.collected_at or generated

    # A language "passes" when enough *newly observed* articles came in from
    # enough distinct sources — not merely when the merged feed is non-empty.
    # `collected_at` is when an article was first seen (dedupe() preserves
    # this across runs), so a stale source repeatedly re-serving the same
    # old/undated articles cannot count as fresh forever the way a raw
    # published-date check would let it.
    LANG_MIN_FRESH = defaults.get("lang_min_fresh_articles", 3)
    LANG_MIN_SOURCES = defaults.get("lang_min_sources", 1)
    LANG_WARN_SOURCES = defaults.get("lang_warn_sources", 2)
    LANG_FRESH_HOURS = defaults.get("lang_fresh_window_hours", 24)

    def _first_seen_hours(a: Article) -> float:
        try:
            return (now_dt - dateparser.parse(a.collected_at or a.published)).total_seconds() / 3600
        except (ValueError, TypeError, OverflowError):
            return float("inf")

    language_status: dict[str, dict] = {}

    for lang in ("en", "si", "ta"):
        previous = [
            Article(stale=True,
                    **{k: v for k, v in d.items()
                       if k in Article.__dataclass_fields__ and k != "stale"})
            for d in load_previous(outdir, lang)
        ]
        previous = [
            a for a in previous if a.source_id in registry
            and (valid_web_url(a.url, (urlparse(registry[a.source_id]["site"]).hostname,))
                 or is_google_news(a.url))
        ]
        previous_all.extend(previous)
        for art in previous:
            if art.image and not valid_web_url(art.image):
                art.image = None
        combined = [a for a in fresh if a.lang == lang] + previous
        merged = [a for a in dedupe(combined) if a.published_dt >= cutoff]
        totals[lang] = len(merged)

        fresh_candidates = [a for a in merged if _first_seen_hours(a) <= LANG_FRESH_HOURS]
        fresh_sources = {a.source_id for a in fresh_candidates}
        passes = (len(fresh_candidates) >= LANG_MIN_FRESH
                  and len(fresh_sources) >= LANG_MIN_SOURCES)

        prev_payload = load_previous_payload(outdir, f"feed_{lang}.json")

        if passes:
            state = "updated"
            payloads[f"feed_{lang}.json"] = {
                "version": API_VERSION,
                "lang": lang,
                "generated": generated,
                "count": len(merged[:PER_FEED]),
                "articles": [a.to_dict() for a in merged[:PER_FEED]],
            }
            for cat in CATEGORIES:
                subset = [a for a in merged if a.category == cat][:PER_FEED]
                if subset:
                    payloads[f"feed_{lang}_{cat}.json"] = {
                        "version": API_VERSION,
                        "lang": lang,
                        "category": cat,
                        "generated": generated,
                        "count": len(subset),
                        "articles": [a.to_dict() for a in subset],
                    }
            log.info("feed_%s.json  %d articles (updated, %d fresh from %d source(s))",
                     lang, len(merged[:PER_FEED]), len(fresh_candidates), len(fresh_sources))
        elif prev_payload is not None:
            state = "preserved"
            payloads[f"feed_{lang}.json"] = prev_payload
            for cat in CATEGORIES:
                prev_cat = load_previous_payload(outdir, f"feed_{lang}_{cat}.json")
                if prev_cat is not None:
                    payloads[f"feed_{lang}_{cat}.json"] = prev_cat
            log.warning("feed_%s.json  below quality bar (%d fresh article(s) from "
                        "%d source(s), need >=%d from >=%d) -- preserving previous "
                        "snapshot unchanged", lang, len(fresh_candidates),
                        len(fresh_sources), LANG_MIN_FRESH, LANG_MIN_SOURCES)
        else:
            state = "unavailable"
            log.warning("feed_%s.json  below quality bar and no previous snapshot "
                        "to preserve -- no file published this run", lang)

        snapshot_age_hours = None
        prev_generated = (prev_payload or {}).get("generated") if state != "updated" else None
        if state == "preserved" and prev_generated:
            try:
                snapshot_age_hours = round(
                    (datetime.now(timezone.utc) - dateparser.parse(prev_generated))
                    .total_seconds() / 3600, 1)
            except (ValueError, TypeError):
                snapshot_age_hours = None

        language_status[lang] = {
            "state": state,
            "fresh_candidates": len(fresh_candidates),
            "fresh_sources": len(fresh_sources),
            "total_candidates": len(merged),
            "snapshot_generated": generated if state == "updated" else prev_generated,
            "snapshot_age_hours": snapshot_age_hours,
        }
        if state != "unavailable" and len(fresh_sources) < LANG_WARN_SOURCES:
            language_status[lang]["warning"] = (
                f"only {len(fresh_sources)} source(s) contributed fresh articles")

    payloads["sources.json"] = {
        "version": API_VERSION,
        "generated": generated,
        "sources": [
            {"id": s["id"], "name": s["name"], "lang": s["lang"], "site": s["site"]}
            for s in sources
        ],
    }

    for sid, info in status.items():
        items = [a for a in fresh if a.source_id == sid]
        info["count"] = len(items)
        if not items:
            info["ok"] = False
            info["error"] = info.get("error") or "No valid articles"
        info.update(source_freshness(
            items, [a for a in previous_all if a.source_id == sid],
            dateparser.parse(generated),
            registry[sid].get("max_age_hours", defaults.get("max_age_hours", 48)),
        ))
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

    stale_sources = sorted(sid for sid, info in status.items() if info["stale_content"])
    google_sources = sorted(
        sid for sid, info in status.items()
        if info.get("used_fallback", info["adapter"]) == "google"
    )
    report = {
        "generated": generated,
        "sources_ok": ok,
        "sources_total": len(sources),
        "totals": totals,
        "unresolved_google_links": google_total,
        "on_fallback": on_fallback,
        "google_sources": google_sources,
        "stale_sources": stale_sources,
        "dead": dead,
        "language_status": language_status,
        "detail": status,
    }

    if dead:
        log.warning("dead >48h (%d): %s", len(dead), ", ".join(dead))

    if on_fallback:
        log.warning("%d source(s) running on a fallback adapter: %s",
                    len(on_fallback),
                    ", ".join(f"{k}->{status[k]['used_fallback']}"
                              for k in on_fallback))
    by_lang: dict[str, list[str]] = {}
    for sid, info in status.items():
        if info["ok"]:
            by_lang.setdefault(info["lang"], []).append(sid)
    for lang in ("en", "si", "ta"):
        n = len(by_lang.get(lang, []))
        total = sum(1 for x in sources if x["lang"] == lang)
        log.info("  %s: %3d articles from %d/%d sources",
                 lang, totals.get(lang, 0), n, total)

    thin = [l for l in ("en", "si", "ta")
            if totals.get(l, 0) < max(totals.values() or [0]) * 0.35]
    if thin:
        log.warning("thin coverage for %s — worth adding sources or "
                    "unblocking the ones on a fallback", ", ".join(thin))

    log.info("done: %d/%d sources healthy (%d on fallback)",
             ok, len(sources), len(on_fallback))

    # Each language now makes its own accept/preserve decision above, so a
    # weak day for one language no longer blocks the others. The whole run
    # is only unusable when literally nothing can be shown for any language
    # — nothing new, and nothing previously accepted to fall back to.
    publishable = [l for l, s in language_status.items() if s["state"] != "unavailable"]
    unavailable = [l for l, s in language_status.items() if s["state"] == "unavailable"]
    preserved = [l for l, s in language_status.items() if s["state"] == "preserved"]

    errors = []
    # Zero articles from every source is a distinct, stronger signal than any
    # single language falling below its quality bar: it usually means the
    # runner's network is broken, not that publishers went quiet. The
    # per-language grace window (recently-collected articles count as "fresh"
    # for up to LANG_FRESH_HOURS) would otherwise happily re-publish the old
    # snapshot and report success, hiding a total outage. Fail loudly instead;
    # nothing is written below, so the previous accepted snapshot is untouched.
    if not fresh:
        log.error("FAILING: zero articles fetched from any source this run")
        errors.append("No articles were fetched from any source this run")
    if not publishable:
        log.error("FAILING: every language is below the quality bar with no "
                  "previous snapshot to preserve: %s", ", ".join(unavailable))
        errors.append("No language has publishable or preserved content")
    elif unavailable:
        log.warning("%s has no previous snapshot and is below the quality bar "
                    "this run — no file will be published for it",
                    ", ".join(unavailable))
    if preserved:
        log.warning("preserving previous snapshot for: %s (see language_status "
                    "in status.json)", ", ".join(preserved))
    if ok < len(sources) * 0.4:
        log.warning("only %d of %d sources produced anything this run — "
                    "see status.json `detail` for per-source errors",
                    ok, len(sources))
    report["accepted"] = not errors
    report["errors"] = errors
    write_json(diagnostics, report)
    if errors:
        return 1
    payloads["status.json"] = report
    try:
        publish_snapshot(outdir, payloads)
    except OSError as exc:
        report["accepted"] = False
        report["errors"] = [f"Snapshot publication failed: {exc}"]
        write_json(diagnostics, report)
        log.exception("Could not promote staged snapshot")
        return 1
    if ok < len(sources):
        log.warning("%d of %d sources produced nothing — run continues",
                    len(sources) - ok, len(sources))
    return 0


if __name__ == "__main__":
    sys.exit(main())
