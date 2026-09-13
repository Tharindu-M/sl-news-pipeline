import sys, json
from datetime import datetime, timezone, timedelta
sys.path.insert(0, "..")
sys.path.insert(0, ".")
from pipeline import (canonical_url, dedupe_key, article_id, clean_text, to_iso,
                      Article, dedupe, from_rss, from_wordpress, from_sitemap, from_html,
                      from_newsfirst, encode_url_path, parse_relative_date)

fails = []
def check(name, cond, detail=""):
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  <- {detail}"))
    if not cond: fails.append(name)

# --- canonical_url -------------------------------------------------------
check("strips utm", canonical_url("https://www.adaderana.lk/news/1?utm_source=fb") == "https://www.adaderana.lk/news/1")
check("keeps www (avoids a 301 hop)", canonical_url("https://www.bbc.com/x") == "https://www.bbc.com/x")
check("relative join", canonical_url("/news/2", "https://www.divaina.lk") == "https://www.divaina.lk/news/2")
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

# --- relative dates -------------------------------------------------------
# Lankadeepa labels everything from the last ~24h as "5 hours ago"; dateutil
# raises on that, so these used to become fabricated scrape-time dates and
# sort above genuinely newer articles.
_ref = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
def _rel(t): return parse_relative_date(t, now=_ref)
check("relative hours", _rel("5 hours ago") == _ref - timedelta(hours=5))
check("relative minutes", _rel("17 minutes ago") == _ref - timedelta(minutes=17))
check("relative singular", _rel("1 hour ago") == _ref - timedelta(hours=1))
check("relative article form", _rel("an hour ago") == _ref - timedelta(hours=1))
check("relative days", _rel("2 days ago") == _ref - timedelta(days=2))
check("relative yesterday", _rel("yesterday") == _ref - timedelta(days=1))
check("relative just now", _rel("just now") == _ref)
check("relative is utc aware", _rel("5 hours ago").tzinfo is not None)
# "ago" is a substring of "Chicago" — the unit word is what makes it safe.
check("relative ignores Chicago", _rel("Chicago bulls win tonight") is None)
check("relative ignores bare ago", _rel("ago") is None)
check("relative ignores unknown unit", _rel("5 bananas ago") is None)
check("relative ignores absolute date", _rel("11 September 2026") is None)
check("to_iso routes relative dates",
      to_iso("5 hours ago") is not None and to_iso("11 September 2026") == "2026-09-10T18:30:00Z")

# --- fake HTTP client -----------------------------------------------------
class R:
    def __init__(s, body, code=200):
        s.content = body if isinstance(body, bytes) else body.encode()
        s.text = s.content.decode()
        s.status_code = code
        s.headers = {}
        s.url = "https://x.lk"
    def json(s): return json.loads(s.text)
    def raise_for_status(s): pass
class C:
    def __init__(s, m): s.m = m
    def get(s, url, **kwargs):
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

# --- newsfirst JSON api ---------------------------------------------------
# Shapes taken from the live response: buckets hold *either* bare posts or a
# {rowCount, postResponseDto} wrapper, and image paths arrive unencoded.
nf_post = {
  "id": "623751", "status": "publish", "type": "post",
  "date": "13-09-2026T12:44 PM",                      # local, dateutil-hostile
  "date_gmt": (now - timedelta(hours=2)).isoformat().replace("+00:00", "Z"),
  "title": {"rendered": si},
  "excerpt": {"rendered": "<p>body text</p>"},
  "post_url": "2026/09/13/%e0%b6%9a%e0%b7%8f%e0%b6%b1",
  "images": {"news_detail_image":
             "https://cdn.newsfirst.lk/sinhala-uploads/New Project (2)-1.jpg"},
}
nf_payload = json.dumps({
  "stickyPost": {"postResponseDto": [{"rowCount": "1", "postResponseDto": [nf_post]}]},
  "sportPost":  {"postResponseDto": [dict(nf_post, id="2", post_url="2026/09/13/sport")]},
  "worldPost":  {"postResponseDto": [dict(nf_post, id="3", post_url="2026/09/13/world")]},
  "draftPost":  {"postResponseDto": [dict(nf_post, id="4", post_url="2026/09/13/d",
                                          status="draft")]},
  "latestPost": {"postResponseDto": [dict(nf_post)]},   # same url as sticky
})
nsrc = {"id": "newsfirst-si", "name": "NewsFirst Sinhala", "lang": "si",
        "site": "https://sinhala.newsfirst.lk", "api": "https://apisinhala.newsfirst.lk"}
na = from_newsfirst(C({"post/sticky": nf_payload}), nsrc)
check("newsfirst parses both envelope shapes", len(na) == 3, f"got {len(na)}")
check("newsfirst dedupes repeated post across buckets",
      len({a.url for a in na}) == len(na))
check("newsfirst drops non-published", all("/d" != a.url[-2:] for a in na))
# clean_text() collapses the doubled space in `si`, same as the assertion above.
check("newsfirst keeps sinhala title", na and na[0].title == clean_text(si, limit=300),
      na[0].title if na else None)
check("newsfirst builds absolute url",
      na and na[0].url.startswith("https://sinhala.newsfirst.lk/2026/09/13/"), na[0].url)
check("newsfirst does not double-encode an encoded path",
      na and "%25" not in na[0].url, na[0].url)
check("newsfirst encodes spaces in image url",
      na and na[0].image == "https://cdn.newsfirst.lk/sinhala-uploads/New%20Project%20%282%29-1.jpg",
      na[0].image)
check("newsfirst uses date_gmt, not the local date field",
      na and not na[0].date_estimated and na[0].published.endswith("Z"))
check("newsfirst maps bucket to category",
      {a.category for a in na} == {None, "sports", "international"},
      {a.category for a in na})
check("newsfirst honours limit", len(from_newsfirst(C({"post/sticky": nf_payload}), nsrc, limit=1)) == 1)
check("newsfirst without api returns nothing",
      from_newsfirst(C({"post/sticky": nf_payload}), {k: v for k, v in nsrc.items() if k != "api"}) == [])
check("newsfirst survives non-JSON", from_newsfirst(C({"post/sticky": "<html>nope"}), nsrc) == [])
check("newsfirst survives unexpected shape",
      from_newsfirst(C({"post/sticky": json.dumps([1, 2, 3])}), nsrc) == [])

check("encode_url_path leaves clean urls alone",
      encode_url_path("https://x.lk/a/b?c=1") == "https://x.lk/a/b?c=1")
check("encode_url_path is idempotent",
      encode_url_path(encode_url_path("https://x.lk/New File (1).jpg"))
      == encode_url_path("https://x.lk/New File (1).jpg"))

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

# --- google link resolution (deleted helpers caused a production NameError)
import json as _json
import pipeline as _p
from pipeline import _parse_batchexecute, _clear_consent, resolve_google_url

# Every name the google path touches. A rewrite once deleted four of these and
# nothing failed until the deployed job hit a NameError mid-run.
for _n in ("_parse_batchexecute", "_decode_via_batchexecute", "_clear_consent",
           "resolve_google_url", "from_google_news", "_google_variants",
           "_parse_google_feed", "_google_feed_url", "_throttle_google",
           "GOOGLE_HOSTS", "_GARTURL_ARGS", "GOOGLE_HL"):
    check(f"pipeline exports {_n}", hasattr(_p, _n))

_vars = _p._google_variants({"id": "x", "name": "X", "lang": "si",
                             "site": "https://sinhala.adaderana.lk"})
check("google variants include a fallback form", len(_vars) >= 3, len(_vars))

_target = "https://www.virakesari.lk/article/224071"
_body = ")]}'\n\n" + _json.dumps([["wrb.fr", "Fbv4je",
        _json.dumps(["garturlres", _target, None]), None, None, None, "generic"]])
check("parses batchexecute body", _parse_batchexecute(_body) == _target)
check("ignores unrelated batchexecute lines",
      _parse_batchexecute('[["wrb.fr","Other","[]"]]') is None)
check("survives malformed batchexecute", _parse_batchexecute("garturlres {{{") is None)

_PAGE = ('<c-wiz><div data-n-a-id="AU_yqLxyz" data-n-a-sg="SIG123" '
         'data-n-a-ts="1757000000">x</div></c-wiz>')
class _GR:
    def __init__(s, t, u="https://news.google.com/x"):
        s.text = t; s.content = t.encode(); s._u = u; s.status_code = 200; s.headers = {}
    @property
    def url(s): return s._u
    def raise_for_status(s): pass
class _GC:
    def __init__(s): s.posts = []
    def get(s, u, **k): return _GR(_PAGE)
    def post(s, u, **k): s.posts.append((u, k)); return _GR(_body)

_c = _GC()
_got, _ = resolve_google_url(_c, "https://news.google.com/rss/articles/AU_yqLxyz",
                             "virakesari.lk")
check("resolves google link via batchexecute", _got == _target, _got)
check("hit the batchexecute endpoint", _c.posts and "batchexecute" in _c.posts[0][0])
_req = _json.loads(_c.posts[0][1]["data"]["f.req"])
check("batchexecute payload carries id/ts/sig",
      all(x in _req[0][0][1] for x in ("AU_yqLxyz", "SIG123", "1757000000")))
check("non-google url passes through",
      resolve_google_url(_GC(), "https://www.divaina.lk/a/1", "divaina.lk")[0]
      == "https://www.divaina.lk/a/1")

_FORM = '<html><form action="/save"><input name="x" value="1"></form></html>'
check("clear_consent posts the form", _clear_consent(_GC(), _GR(_FORM, "https://consent.google.com/m")) is True)
check("clear_consent handles no form", _clear_consent(_GC(), _GR("<html></html>")) is False)

print("\n" + ("ALL PASS" if not fails else f"{len(fails)} FAILED: {fails}"))
sys.exit(1 if fails else 0)
