#!/usr/bin/env python3
"""
Find where a source hides its publish date.

Fetches one listing page and one article page and dumps every element that
could plausibly carry a date: meta tags, <time>, JSON-LD, and any short text
node that parses as a date. Paste the output back and the right selector
goes into sources.yaml.

    python3 inspect_dates.py --id lankadeepa
    python3 inspect_dates.py --url https://www.lankadeepa.lk/news/x/101-697209
"""

from __future__ import annotations

import argparse
import json
import re
import sys

import yaml
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

from pipeline import make_client, get

DIM, BOLD, GREEN, RESET = "\033[2m", "\033[1m", "\033[32m", "\033[0m"

# Sinhala and Tamil month names, so a localised date still registers.
MONTHS = ("jan feb mar apr may jun jul aug sep oct nov dec "
          "ජන පෙබ මාර් අප්‍රේ මැයි ජූනි ජූලි අගෝ සැප් ඔක් නොවැ දෙසැ "
          "ஜன பிப் மார் ஏப் மே ஜூன் ஜூலை ஆக செப் அக் நவ டிச").split()


def looks_like_date(text: str) -> bool:
    t = text.strip()
    if not (6 <= len(t) <= 60):
        return False
    if re.search(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", t):
        return True
    if re.search(r"\d{1,2}[:.]\d{2}", t) and re.search(r"\d", t):
        return True
    low = t.lower()
    if any(m in low for m in MONTHS) and re.search(r"\d", t):
        return True
    return False


def css_path(node) -> str:
    """A short, usable selector for the element."""
    bits = []
    cur = node
    for _ in range(3):
        if cur is None or cur.name in (None, "[document]", "html"):
            break
        seg = cur.name
        cls = cur.get("class")
        if cls:
            seg += "." + ".".join(cls[:2])
        elif cur.get("id"):
            seg += f"#{cur['id']}"
        bits.insert(0, seg)
        cur = cur.parent
    return " > ".join(bits)


def dump(client, url: str, label: str) -> None:
    print(f"\n{BOLD}=== {label} ==={RESET}\n{DIM}{url}{RESET}")
    r = get(client, url)
    if r is None:
        print("  fetch failed")
        return
    soup = BeautifulSoup(r.text, "lxml")

    print(f"\n{BOLD}meta tags mentioning date/time/publish{RESET}")
    hits = 0
    for m in soup.find_all("meta"):
        key = (m.get("property") or m.get("name") or m.get("itemprop") or "")
        if re.search(r"date|time|publish|modif", key, re.I) and m.get("content"):
            print(f"  {GREEN}{key}{RESET} = {m['content'][:60]}")
            hits += 1
    if not hits:
        print(f"  {DIM}none{RESET}")

    print(f"\n{BOLD}<time> elements{RESET}")
    times = soup.find_all("time")
    for t in times[:6]:
        print(f"  {css_path(t)}  datetime={t.get('datetime')!r} "
              f"text={t.get_text(strip=True)[:40]!r}")
    if not times:
        print(f"  {DIM}none{RESET}")

    print(f"\n{BOLD}JSON-LD datePublished{RESET}")
    found_ld = False
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "{}")
        except ValueError:
            continue
        for obj in (data if isinstance(data, list) else [data]):
            if isinstance(obj, dict) and obj.get("datePublished"):
                print(f"  {GREEN}datePublished{RESET} = {obj['datePublished']}")
                found_ld = True
    if not found_ld:
        print(f"  {DIM}none{RESET}")

    print(f"\n{BOLD}text nodes that parse as a date{RESET}")
    seen = set()
    shown = 0
    for node in soup.find_all(string=looks_like_date):
        parent = node.parent
        if parent is None or parent.name in ("script", "style"):
            continue
        path = css_path(parent)
        if path in seen:
            continue
        seen.add(path)
        text = node.strip()
        try:
            parsed = dateparser.parse(text, fuzzy=True)
        except (ValueError, OverflowError):
            parsed = None
        mark = f"{GREEN}-> {parsed}{RESET}" if parsed else f"{DIM}(unparsed){RESET}"
        print(f"  {path}\n      {text[:55]!r} {mark}")
        shown += 1
        if shown >= 10:
            break
    if not shown:
        print(f"  {DIM}none{RESET}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", help="source id from sources.yaml")
    ap.add_argument("--url", help="a single article URL to inspect")
    ap.add_argument("--config", default="sources.yaml")
    args = ap.parse_args()

    client = make_client()

    if args.url:
        dump(client, args.url, "article")
        return 0
    if not args.id:
        return ap.error("pass --id or --url")

    cfg = yaml.safe_load(open(args.config, encoding="utf-8"))
    src = next((s for s in cfg["sources"] if s["id"] == args.id), None)
    if src is None:
        print(f"no source with id {args.id!r}")
        return 1

    listing = (src.get("listings") or [src.get("listing") or src["site"]])[0]
    dump(client, listing, f"{args.id} listing")

    # Grab the first article link off the listing and inspect that too.
    r = get(client, listing)
    if r is not None:
        soup = BeautifulSoup(r.text, "lxml")
        sel = (src.get("selectors") or {}).get("item", "article")
        node = soup.select_one(sel)
        a = node.select_one("a") if node else soup.select_one("a[href*='/']")
        if a and a.get("href"):
            from urllib.parse import urljoin
            dump(client, urljoin(src["site"], a["href"]), f"{args.id} article")
    return 0


if __name__ == "__main__":
    sys.exit(main())
