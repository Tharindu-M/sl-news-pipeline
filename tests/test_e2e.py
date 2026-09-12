import sys, json, shutil
from pathlib import Path
from datetime import datetime, timezone, timedelta
sys.path.insert(0, ".")
import pipeline, ingest
from pipeline import Article, article_id

now = datetime.now(timezone.utc)
def mk(sid, lang, n, cat=None, h=1):
    u = f"https://{sid}.lk/news/{n}"
    return Article(id=article_id(u), source_id=sid, source_name=sid.title(), lang=lang,
                   title=f"{lang} story {n}", url=u,
                   published=(now-timedelta(hours=h)).isoformat().replace("+00:00","Z"),
                   excerpt="x", image="https://i.lk/a.jpg", category=cat)

def fake(client, src, limit=40):
    if src["id"] == "broken": raise RuntimeError("Cloudflare 403")
    return [mk(src["id"], src["lang"], i, cat="politics" if i==0 else None, h=i+1) for i in range(3)]

for k in pipeline.ADAPTERS: pipeline.ADAPTERS[k] = fake
ingest.ADAPTERS = pipeline.ADAPTERS
ingest.enrich = lambda c,a: a
ingest.diagnose_endpoint = lambda c, s: {"error": "mock source unavailable"}

cfg = """
defaults: {timeout: 5, max_items: 10}
sources:
  - {id: a, name: A, lang: en, site: "https://a.lk", adapter: rss, feed: "https://a.lk/rss"}
  - {id: b, name: B, lang: si, site: "https://b.lk", adapter: wordpress}
  - {id: c, name: C, lang: ta, site: "https://c.lk", adapter: sitemap}
  - {id: broken, name: Broken, lang: en, site: "https://d.lk", adapter: rss, feed: "https://d.lk/rss"}
"""
Path("tests/tmp.yaml").write_text(cfg)
shutil.rmtree("tests/out", ignore_errors=True)

sys.argv = ["ingest.py", "--out", "tests/out", "--config", "tests/tmp.yaml", "--no-enrich"]
rc = ingest.main()

fails=[]
def check(n, c, d=""):
    print(("PASS " if c else "FAIL ")+n+("" if c else f"  <- {d}")); 
    if not c: fails.append(n)

check("exit 0 despite 1 broken source", rc == 0, f"rc={rc}")
for lang, cnt in (("en",3),("si",3),("ta",3)):
    p = Path(f"tests/out/v1/feed_{lang}.json")
    check(f"feed_{lang}.json exists", p.exists())
    d = json.loads(p.read_text(encoding="utf-8"))
    check(f"feed_{lang} count", d["count"] == cnt, f"got {d['count']}")
    check(f"feed_{lang} sorted desc", [a["published"] for a in d["articles"]] == sorted([a["published"] for a in d["articles"]], reverse=True))
    check(f"feed_{lang} no nulls", all(None not in a.values() for a in d["articles"]))

st = json.loads(Path("tests/out/v1/status.json").read_text())
check("status flags broken source", st["detail"]["broken"]["ok"] is False and "Cloudflare" in st["detail"]["broken"]["error"])
check("status counts healthy", st["sources_ok"] == 3, st["sources_ok"])
check("category feed written", Path("tests/out/v1/feed_en_politics.json").exists())
check("sources.json written", Path("tests/out/v1/sources.json").exists())

# --- second run must merge, not clobber ---
before = json.loads(Path("tests/out/v1/feed_en.json").read_text())["count"]
def fake2(client, src, limit=40):
    if src["id"] == "broken": raise RuntimeError("down")
    return [mk(src["id"], src["lang"], 99, h=0)]
for k in pipeline.ADAPTERS: pipeline.ADAPTERS[k] = fake2
ingest.ADAPTERS = pipeline.ADAPTERS
ingest.main()
after = json.loads(Path("tests/out/v1/feed_en.json").read_text())
check("second run merges with previous", after["count"] == before + 1, f"{before} -> {after['count']}")
check("newest article first", after["articles"][0]["url"].endswith("/99"))

# --- total failure must not wipe feeds ---
def dead(client, src, limit=40): raise RuntimeError("all down")
for k in pipeline.ADAPTERS: pipeline.ADAPTERS[k] = dead
ingest.ADAPTERS = pipeline.ADAPTERS
rc2 = ingest.main()
survived = json.loads(Path("tests/out/v1/feed_en.json").read_text())["count"]
check("total failure exits nonzero", rc2 == 1, f"rc={rc2}")
check("total failure preserves feed", survived == after["count"], f"{survived}")

print("\n" + ("ALL PASS" if not fails else f"{len(fails)} FAILED: {fails}"))
sys.exit(1 if fails else 0)
