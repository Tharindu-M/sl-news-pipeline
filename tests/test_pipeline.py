import sys, json
from datetime import datetime, timezone, timedelta
sys.path.insert(0, "..")
sys.path.insert(0, ".")
import feedparser
from pipeline import (canonical_url, dedupe_key, article_id, clean_text, to_iso,
                      Article, dedupe, from_rss, from_wordpress, from_sitemap, from_html)

fails = []
def check(name, cond, detail=""):
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  <- {detail}"))
    if not cond: fails.append(name)

# --- canonical_url -------------------------------------------------------
check("strips utm", canonical_url("https://www.adaderana.lk/news/1?utm_source=fb") == "https://www.adaderana.lk/news/1")
check("keeps www (avoids a 301 hop)", canonical_url("https://www.bbc.com/x") == "https://www.bbc.com/x")
check("relative join", canonical_url("/news/2", "https://www.divaina.lk") == "https://www.divaina.lk/news/2")
from pipeline import dedupe_key
check("dedupe ignores www", dedupe_key("https://www.x.lk/a") == dedupe_key("https://x.lk/a"))
check("trailing slash", canonical_url("https://x.lk/a/") == canonical_url("https://x.lk/a"))
check("id stable", article_id("https://www.x.lk/a?utm_medium=x") == article_id("https://x.lk/a/"))

# --- unicode handling (critical for si/ta) -------------------------------
si = "ඩෙංගු රෝගීන්  79,000 පන්නයි"
ta = "டெங்குக் காய்ச்சலால் பாதிக்கப்பட்டவர்கள்"
check("sinhala preserved", clean_text(f"<p>{si}</p>") == "ඩෙංගු රෝගීන් 79,000 පන්නයි")
check("tamil preserved", clean_text(f"<div>{ta}</div>") == ta)
check("unicode url id", len(article_id("https://sinhala.adaderana.lk/news/229047/නීතිවිරෝධීව")) == 16)

# --- excerpt truncation ---------------------------------------------------
long = "word " * 100
ex = clean_text(long, limit=50)
check("truncates", len(ex) <= 51 and ex.endswith("\u2026"), ex)

# --- to_iso ---------------------------------------------------------------
check("iso passthrough", to_iso("2026-09-11T10:00:00Z").startswith("2026-09-11T10:00"))
check("naive treated as SLST", to_iso("2026-09-11 10:00:00") == "2026-09-11T04:30:00Z")
check("rejects far future", to_iso("2031-01-01") is None)
check("rejects ancient", to_iso("1999-01-01") is None)
check("rejects garbage", to_iso("not a date") is None)
check("struct_time", to_iso((2026,9,11,10,0,0,0,0,0)).startswith("2026-09-11T10:00"))

# --- fake HTTP client -----------------------------------------------------
class R:
    def __init__(s, body): s.content = body if isinstance(body, bytes) else body.encode(); s.text = s.content.decode()
    def json(s): return json.loads(s.text)
    def raise_for_status(s): pass
class C:
    def __init__(s, m): s.m = m
    def get(s, url):
        for k, v in s.m.items():
            if k in url: return R(v)
        raise Exception("404 " + url)

now = datetime.now(timezone.utc)
recent = (now - timedelta(hours=3)).strftime("%a, %d %b %Y %H:%M:%S +0000")

rss = f"""<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel>
<item><title>{si}</title><link>https://sinhala.adaderana.lk/news/229047</link>
<description>&lt;p&gt;test body&lt;/p&gt;</description><pubDate>{recent}</pubDate></item>
<item><title>No date</title><link>https://x.lk/2</link></item>
</channel></rss>"""

src = {"id":"ada-derana-si","name":"Ada Derana Sinhala","lang":"si",
       "site":"https://sinhala.adaderana.lk","feed":"https://sinhala.adaderana.lk/rss.php"}
arts = from_rss(C({"rss.php": rss}), src)
check("rss parses", len(arts) == 1, f"got {len(arts)}")
check("rss keeps sinhala title", arts[0].title == si)
check("rss drops dateless item", all(a.title != "No date" for a in arts))
check("rss excerpt cleaned", arts[0].excerpt == "test body", arts[0].excerpt)

# --- wordpress ------------------------------------------------------------
wp = json.dumps([{
  "link":"https://economynext.com/sri-lanka-rupee-123",
  "title":{"rendered":"Sri Lanka rupee at 336.30/40 &amp; steady"},
  "excerpt":{"rendered":"<p>ECONOMYNEXT &#8211; The rupee closed at...</p>"},
  "date_gmt": (now - timedelta(hours=1)).isoformat(),
  "_embedded":{"wp:featuredmedia":[{"source_url":"https://economynext.com/img.png"}]}
}])
wsrc = {"id":"economynext","name":"EconomyNext","lang":"en","site":"https://economynext.com"}
wa = from_wordpress(C({"wp-json": wp}), wsrc)
check("wp parses", len(wa) == 1)
check("wp decodes entities", "&amp;" not in wa[0].title, wa[0].title)
check("wp gets featured image", wa[0].image == "https://economynext.com/img.png")

# --- sitemap --------------------------------------------------------------
sm = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
        xmlns:news="http://www.google.com/schemas/sitemap-news/0.9">
<url><loc>https://www.virakesari.lk/article/999</loc>
<news:news><news:title>{ta}</news:title>
<news:publication_date>{(now - timedelta(hours=2)).isoformat()}</news:publication_date>
</news:news></url></urlset>"""
ssrc = {"id":"virakesari","name":"Virakesari","lang":"ta",
        "site":"https://www.virakesari.lk","sitemap":"https://www.virakesari.lk/news-sitemap.xml"}
sa = from_sitemap(C({"news-sitemap": sm}), ssrc)
check("sitemap parses", len(sa) == 1, f"got {len(sa)}")
check("sitemap tamil title", sa[0].title == ta if sa else False)

# --- html scrape ----------------------------------------------------------
html = """<html><body>
<article><h2 class="title">Fuel prices hiked again</h2>
<a href="/news/551">read</a><img data-src="/img/a.jpg"></article>
<article><h2 class="title">Dup</h2><a href="/news/551">x</a></article>
</body></html>"""
hsrc = {"id":"lankadeepa","name":"Lankadeepa","lang":"si","site":"https://www.lankadeepa.lk",
        "listing":"https://www.lankadeepa.lk/latest_news/1",
        "selectors":{"item":"article","link":"a","title":".title","image":"img"}}
ha = from_html(C({"latest_news": html}), hsrc)
check("html parses", len(ha) == 1, f"got {len(ha)} (dup should collapse)")
check("html lazy img", ha[0].image == "https://www.lankadeepa.lk/img/a.jpg" if ha else False)

# --- dedupe ---------------------------------------------------------------
def mk(u, t, img=None, h=1):
    return Article(id=article_id(u), source_id="s", source_name="S", lang="en",
                   title=t, url=canonical_url(u),
                   published=(now - timedelta(hours=h)).isoformat().replace("+00:00","Z"),
                   image=img)
d = dedupe([mk("https://x.lk/1","A"), mk("https://www.x.lk/1/?utm_source=f","A",img="i.png"),
            mk("https://x.lk/2","B",h=2)])
check("dedupe by url", len(d) == 2, f"got {len(d)}")
check("dedupe prefers richer", any(a.image == "i.png" for a in d))
check("dedupe sorts desc", d[0].published > d[1].published)
n = dedupe([mk("https://x.lk/3","Same Headline Here"), mk("https://x.lk/4","Same  headline, here!")])
check("dedupe near-dup titles", len(n) == 1, f"got {len(n)}")

# --- adapter registry invariants (the bug that crashed probe.py) ----------
from pipeline import ADAPTERS as _A, ADAPTER_PREFERENCE as _P
check("every adapter is ranked", set(_A) == set(_P), f"{set(_A) ^ set(_P)}")
check("google ranked last", _P[-1] == "google", _P)
check("rss ranked first", _P[0] == "rss", _P)

def _best(names):
    return min(names, key=lambda n: _P.index(n) if n in _P else len(_P))
check("prefers rss over google", _best(["google", "rss"]) == "rss")
check("prefers sitemap over html", _best(["html", "sitemap"]) == "sitemap")
check("unknown adapter does not raise", _best(["mystery", "sitemap"]) == "sitemap")

print("\n" + ("ALL PASS" if not fails else f"{len(fails)} FAILED: {fails}"))
sys.exit(1 if fails else 0)
