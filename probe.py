#!/usr/bin/env python3
"""
Run this FIRST, and run it again whenever a source goes quiet.

For every outlet in sources.yaml it tries all four adapters and reports
which ones actually return articles, how many, and how fresh they are.
Then pin the winner in sources.yaml.

    python3 probe.py                 # all sources
    python3 probe.py --lang si       # just Sinhala
    python3 probe.py --id divaina    # one outlet
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

import yaml

from pipeline import (ADAPTERS, ADAPTER_PREFERENCE, make_client, get,
                      discover_sitemaps)

# Endpoints worth checking by hand when every adapter returns nothing.
# The status code tells you which problem you have:
#   403/503 + Cloudflare  -> blocked, needs a browser-like fetch
#   404 everywhere        -> nothing machine-readable, write selectors
#   200 but 0 articles    -> our parser is wrong, not the site
PROBE_PATHS = [
    ("homepage",  ""),
    ("wp-json",   "/wp-json/wp/v2/posts?per_page=1"),
    ("feed",      "/feed"),
    ("rss.xml",   "/rss.xml"),
    ("rss.php",   "/rss.php"),
    ("robots",    "/robots.txt"),
    ("sitemap",   "/sitemap.xml"),
    ("news-sm",   "/news-sitemap.xml"),
]


def diagnose(client, src: dict) -> None:
    """Raw HTTP reconnaissance for a source where nothing worked."""
    base = src["site"].rstrip("/")
    print(f"  {DIM}--- diagnosis ---{RESET}")
    for label, path in PROBE_PATHS:
        try:
            r = client.get(base + path)
            code = r.status_code
            server = r.headers.get("server", "")
            ctype = r.headers.get("content-type", "").split(";")[0]
            size = len(r.content)
            colour = GREEN if code == 200 else RED
            note = ""
            if "cloudflare" in server.lower() and code in (403, 503):
                note = f"  {RED}<- Cloudflare is blocking us{RESET}"
            elif code == 200 and label == "wp-json" and "json" in ctype:
                note = f"  {GREEN}<- WordPress! use adapter: wordpress{RESET}"
            elif code == 200 and "xml" in ctype and label in ("feed", "rss.xml", "rss.php"):
                note = f"  {GREEN}<- feed exists! use adapter: rss{RESET}"
            print(f"    {colour}{code}{RESET} {label:<10} {ctype:<24} {size:>7}b{note}")
            if label == "robots" and code == 200:
                for line in r.text.splitlines():
                    if line.lower().startswith("sitemap:"):
                        print(f"         {DIM}{line.strip()}{RESET}")
        except Exception as e:
            print(f"    {RED}ERR{RESET} {label:<10} {type(e).__name__}: {str(e)[:50]}")

logging.basicConfig(level=logging.ERROR, format="%(message)s")

GREEN, RED, DIM, BOLD, RESET = "\033[32m", "\033[31m", "\033[2m", "\033[1m", "\033[0m"


def age_hours(iso: str) -> float:
    from dateutil import parser as dp
    return (datetime.now(timezone.utc) - dp.parse(iso)).total_seconds() / 3600


def probe_one(client, src: dict, try_google: bool = True) -> dict:
    results = {}
    for name, fn in ADAPTERS.items():
        # google always works, so probing it first would mask the real answer
        # and waste requests. Run it only as a fallback, after the rest.
        if name == "google":
            continue
        # html needs selectors; skip it unless the outlet defines them.
        if name == "html" and not src.get("selectors"):
            continue
        # google is always available as a fallback — no config needed.
        if name == "rss" and not (src.get("feed") or src.get("feeds")):
            continue
        try:
            items = fn(client, src, limit=10)
        except Exception as e:
            results[name] = {"ok": False, "error": f"{type(e).__name__}: {e}"}
            continue
        if not items:
            results[name] = {"ok": False, "error": "0 articles"}
            continue
        ages = [age_hours(a.published) for a in items]
        results[name] = {
            "ok": True,
            "count": len(items),
            "newest_h": min(ages),
            "with_image": sum(1 for a in items if a.image),
            "sample": items[0].title[:70],
        }

    if try_google and not any(r["ok"] for r in results.values()):
        try:
            items = ADAPTERS["google"](client, src, limit=10)
        except Exception as e:
            results["google"] = {"ok": False, "error": f"{type(e).__name__}: {e}"}
        else:
            if items:
                ages = [age_hours(a.published) for a in items]
                results["google"] = {
                    "ok": True, "count": len(items), "newest_h": min(ages),
                    "with_image": 0, "sample": items[0].title[:70],
                }
            else:
                results["google"] = {"ok": False, "error": "0 articles"}
    return results


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lang", help="filter: en | si | ta")
    ap.add_argument("--id", help="filter: a single source id")
    ap.add_argument("--config", default="sources.yaml")
    ap.add_argument("--diagnose", action="store_true",
                    help="on failure, dump raw HTTP status for common endpoints")
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config, encoding="utf-8"))
    defaults = cfg.get("defaults", {})
    sources = [s for s in cfg["sources"] if s.get("enabled", True)]
    parked = len(cfg["sources"]) - len(sources)
    if args.id:   # --id overrides `enabled: false` so you can debug a parked source
        sources = [s for s in cfg["sources"] if s["id"] == args.id]
    if args.lang:
        sources = [s for s in sources if s["lang"] == args.lang]

    client = make_client(
        user_agent=defaults.get("user_agent"),
        timeout=defaults.get("timeout", 20),
    )

    working, broken = [], []

    for src in sources:
        print(f"\n{BOLD}{src['id']}{RESET} ({src['lang']}) {DIM}{src['site']}{RESET}")

        # Show what sitemaps exist — useful even when the adapter fails.
        if src.get("adapter") == "sitemap" and not src.get("sitemap"):
            found = discover_sitemaps(client, src["site"])[:3]
            print(f"  {DIM}sitemap candidates: {', '.join(found)}{RESET}")

        results = probe_one(client, src)
        any_ok = False
        for name, r in results.items():
            if r["ok"]:
                any_ok = True
                flag = GREEN + "OK " + RESET
                fresh = f"newest {r['newest_h']:.0f}h ago"
                warn = f"  {RED}<- STALE{RESET}" if r["newest_h"] > 48 else ""
                print(f"  {flag} {name:<10} {r['count']:>2} items, "
                      f"{r['with_image']} with image, {fresh}{warn}")
                print(f"      {DIM}{r['sample']}{RESET}")
            else:
                print(f"  {RED}--{RESET} {name:<10} {DIM}{r['error'][:70]}{RESET}")

        if not any_ok and args.diagnose:
            diagnose(client, src)

        if any_ok:
            # Unknown adapter names sort last rather than raising, so adding
            # an adapter can never crash the probe again.
            best = min(
                (n for n, r in results.items() if r["ok"]),
                key=lambda n: (ADAPTER_PREFERENCE.index(n)
                               if n in ADAPTER_PREFERENCE else len(ADAPTER_PREFERENCE)),
            )
            if best == "google":
                print(f"  {DIM}(google is a fallback — keep looking for a direct adapter){RESET}")
            working.append((src["id"], best))
            if best != src.get("adapter"):
                print(f"  {BOLD}-> change adapter to: {best}{RESET}")
        else:
            broken.append(src["id"])

    print(f"\n{BOLD}{'=' * 60}{RESET}")
    print(f"{GREEN}Working ({len(working)}){RESET}")
    for sid, adapter in working:
        print(f"  {sid:<20} {adapter}")
    if broken:
        print(f"\n{RED}No adapter worked ({len(broken)}){RESET}")
        for sid in broken:
            print(f"  {sid}")
        print(f"\n{DIM}For these: open the site, check for /wp-json/wp/v2/posts,{RESET}")
        print(f"{DIM}check robots.txt for a sitemap, then write selectors and{RESET}")
        print(f"{DIM}set adapter: html. Google News RSS is the final fallback:{RESET}")
        print(f"{DIM}  https://news.google.com/rss/search?q=site:example.lk&hl=si&gl=LK{RESET}")
    return 0 if working else 1


if __name__ == "__main__":
    sys.exit(main())
