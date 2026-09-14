"""Offline regressions; run with python -m unittest discover -s tests -p test_reliability.py."""
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import httpx
import yaml

import ingest
import pipeline
from pipeline import Article, article_id


NOW = datetime.now(timezone.utc).replace(microsecond=0)


def article(sid="a", lang="en", age=1, estimated=False, n=1):
    """`n` gives each call a distinct URL/id; several tests need >=1 article
    per language to independently satisfy the LANG_MIN_FRESH quality bar."""
    url = f"https://{sid}.lk/news/{n}"
    return Article(
        id=article_id(url), source_id=sid, source_name=sid, lang=lang,
        title=f"{sid} headline {n}", url=url,
        published=(NOW - timedelta(hours=age)).isoformat().replace("+00:00", "Z"),
        date_estimated=estimated,
    )


class TimestampTests(unittest.TestCase):
    def test_wordpress_naive_gmt_and_local_fallback(self):
        stamp = NOW.strftime("%Y-%m-%dT%H:%M:%S")
        for gmt, expected in ((stamp, NOW), ("bad", NOW - timedelta(hours=5, minutes=30))):
            with self.subTest(gmt=gmt):
                posts = [{
                    "link": "https://a.lk/news/1", "title": {"rendered": "News"},
                    "date_gmt": gmt, "date": stamp,
                }]
                with httpx.Client(transport=httpx.MockTransport(
                    lambda r: httpx.Response(200, json=posts)
                )) as client:
                    result = pipeline.from_wordpress(
                        client, {"id": "a", "name": "A", "lang": "en", "site": "https://a.lk"}
                    )
                self.assertEqual(expected, result[0].published_dt)

    def test_estimated_dates_and_collection_time_are_serialized(self):
        item = article(estimated=True)
        item.collected_at = item.published
        data = item.to_dict()
        self.assertTrue(data["date_estimated"])
        self.assertEqual(item.collected_at, data["collected_at"])
        self.assertNotIn("stale", data)

    def test_repeated_undated_article_keeps_first_seen(self):
        old = article(age=30, estimated=True)
        old.stale = True
        old.collected_at = old.published
        new = article(age=0, estimated=True)
        new.collected_at = new.published
        result = pipeline.dedupe([new, old])[0]
        self.assertEqual(old.published, result.published)
        self.assertEqual(old.collected_at, result.collected_at)

    def test_verified_cached_date_beats_new_estimate(self):
        old, new = article(age=30), article(age=0, estimated=True)
        old.stale = True
        self.assertFalse(pipeline.dedupe([new, old])[0].date_estimated)

    def test_successful_fetch_of_old_articles_is_stale(self):
        old = article(age=72)
        result = ingest.source_freshness([old], [old], NOW, 48)
        self.assertTrue(result["stale_content"])
        self.assertEqual(0, result["new_articles"])
        self.assertEqual(72, result["hours_since_publication"])

    def test_estimated_and_future_dates_do_not_prove_freshness(self):
        for item in (article(estimated=True), article(age=-1)):
            result = ingest.source_freshness([item], [], NOW, 48)
            self.assertTrue(result["stale_content"])
            self.assertIsNone(result["newest_published"])

    def test_freshness_threshold_is_configurable(self):
        self.assertFalse(ingest.source_freshness([article(age=72)], [], NOW, 96)["stale_content"])

    def test_corrected_date_overrides_wrong_cached_date_for_freshness(self):
        cached = article(age=0)
        cached.stale = True
        result = ingest.source_freshness([article(age=72)], [cached], NOW, 48)
        self.assertTrue(result["stale_content"])


class UrlTests(unittest.TestCase):
    def test_rejects_schemes_credentials_private_hosts_and_lookalikes(self):
        for url in (
            "javascript:alert(1)", "file:///etc/passwd",
            "https://a.lk@evil.example/x", "https://a.lk.evil.example/x",
            "https://not-a.lk/x", "https://127.0.0.1/x", "http://[::1]/x",
            "https://a.lk:9999/x", "https://a.lk/\\evil", "https://a.lk/\n",
        ):
            with self.subTest(url=url):
                self.assertFalse(pipeline.valid_web_url(url, ("a.lk",)))
        self.assertTrue(pipeline.valid_web_url("https://www.a.lk/x", ("a.lk",)))
        self.assertFalse(pipeline.is_google_news("https://evil.example/news.google.com"))

    def test_redirect_is_blocked_before_unexpected_host_is_requested(self):
        requested = []

        def handler(request):
            requested.append(str(request.url))
            return httpx.Response(302, headers={"location": "https://evil.example/x"})

        with httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=True) as client:
            self.assertIsNone(pipeline.get(client, "https://a.lk/x", allowed_hosts=("a.lk",)))
        self.assertEqual(["https://a.lk/x"], requested)

    def test_google_rejects_decoded_lookalike(self):
        url = "https://news.google.com/rss/articles/abc"
        with patch.object(pipeline, "_decode_via_batchexecute",
                          return_value="https://a.lk.evil.example/x"), \
                patch.object(pipeline, "checked_get", side_effect=httpx.RequestError("blocked")):
            self.assertEqual(url, pipeline.resolve_google_url(None, url, "a.lk")[0])

    def test_google_accepts_publisher_redirect(self):
        url = "https://news.google.com/rss/articles/abc"

        def handler(request):
            if request.url.host == "news.google.com":
                return httpx.Response(302, headers={"location": "https://www.a.lk/news/1"})
            return httpx.Response(200, text="<html/>")

        with httpx.Client(transport=httpx.MockTransport(handler)) as client, \
                patch.object(pipeline, "_decode_via_batchexecute", return_value=None):
            resolved, _ = pipeline.resolve_google_url(client, url, "a.lk")
        self.assertEqual("https://www.a.lk/news/1", resolved)

    def test_consent_form_cannot_post_to_external_host(self):
        response = httpx.Response(200, text=(
            '<form action="https://evil.example/save"><input name="x" value="1"></form>'
        ), request=httpx.Request("GET", "https://consent.google.com/m"))
        with patch.object(httpx.Client, "post") as post:
            with httpx.Client() as client:
                self.assertFalse(pipeline._clear_consent(client, response))
            post.assert_not_called()

    def test_html_skips_unsafe_link_without_losing_valid_article(self):
        body = ('<article><a href="javascript:alert(1)">Bad</a></article>'
                '<article><a href="/news/1">Good</a></article>')
        with httpx.Client(transport=httpx.MockTransport(
            lambda r: httpx.Response(200, text=body)
        )) as client:
            items = pipeline.from_html(
                client, {"id": "a", "name": "A", "lang": "en", "site": "https://a.lk"}
            )
        self.assertEqual(1, len(items))
        self.assertEqual("Good", items[0].title)


class SnapshotHarness(unittest.TestCase):
    """Config + patched-adapter plumbing. Holds no tests of its own, so
    subclasses do not re-run each other's cases."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.out = self.root / "public"
        self.config = self.root / "sources.yaml"
        self.diag = self.root / "diagnostics.json"
        self.sources = [
            {"id": sid, "name": sid, "lang": lang, "site": f"https://{sid}.lk",
             "adapter": "rss", "feed": f"https://{sid}.lk/rss", "fallback": False}
            for sid, lang in (("a", "en"), ("b", "si"), ("c", "ta"))
        ]
        self.config.write_text(yaml.safe_dump({"sources": self.sources}), encoding="utf-8")

    def run_ingest(self, items):
        def fetch(client, src, limit):
            return items.get(src["id"], [])

        with patch.dict(ingest.ADAPTERS, {"rss": fetch}), \
                patch.object(ingest, "diagnose_endpoint", return_value={"error": "mock"}), \
                patch("sys.argv", [
                    "ingest.py", "--out", str(self.out), "--config", str(self.config),
                    "--diagnostics", str(self.diag), "--no-enrich",
                ]):
            return ingest.main()

    def seed(self):
        # LANG_MIN_FRESH defaults to 3: each single-source language here must
        # supply at least 3 distinct articles to clear the quality bar.
        items = {s["id"]: [article(s["id"], s["lang"], age=i, n=i + 1)
                          for i in range(3)]
                 for s in self.sources}
        self.assertEqual(0, self.run_ingest(items))

    def snapshot(self):
        return {p.name: p.read_bytes() for p in (self.out / "v1").iterdir()}


class SnapshotTests(SnapshotHarness):
    def test_total_failure_is_rejected_and_preserves_snapshot(self):
        """Zero articles from every source is a distinct, harder failure than
        any one language falling below its quality bar: it usually means the
        client/network is broken, not that publishers went quiet. This must
        fail loudly rather than quietly re-publish old content as a "pass"."""
        self.seed()
        before = self.snapshot()
        self.assertEqual(1, self.run_ingest({}))
        self.assertEqual(before, self.snapshot())
        report = json.loads(self.diag.read_text(encoding="utf-8"))
        self.assertFalse(report["accepted"])
        self.assertTrue(report["errors"])

    def test_partial_return_within_grace_window_does_not_hard_fail(self):
        """One source re-confirming an already-known article while others
        return nothing is tolerated: previously-collected content is still
        within its freshness grace window, so this is a soft warning
        (surfaced via language_status/stale_sources), not a CI failure."""
        self.seed()
        self.assertEqual(0, self.run_ingest({"a": [article(age=0, n=1)]}))

    def test_empty_language_does_not_publish_partial_snapshot(self):
        self.assertEqual(1, self.run_ingest({
            "a": [article()], "b": [article("b", "si")],
        }))
        self.assertFalse((self.out / "v1").exists())

    def test_stale_category_file_is_replaced_on_success(self):
        """
        publish_snapshot swaps the whole v1/ directory rather than writing in
        place, so a hand-written leftover cannot survive a run. Every category
        file is published now, so the proof is that the *content* was replaced,
        not that the file disappeared.
        """
        self.seed()
        stale_file = self.out / "v1/feed_en_politics.json"
        stale_file.write_text('{"articles":[]}', encoding="utf-8")
        self.seed()
        published = json.loads(stale_file.read_text(encoding="utf-8"))
        self.assertEqual("politics", published["category"])
        self.assertIn("version", published)
        self.assertIn("generated", published)

    def test_staging_write_failure_leaves_snapshot_untouched(self):
        self.seed()
        before = self.snapshot()
        with patch.object(ingest, "write_json", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                ingest.publish_snapshot(self.out, {"feed_en.json": {}})
        self.assertEqual(before, self.snapshot())

    def test_promotion_failure_rolls_back(self):
        self.seed()
        before = self.snapshot()
        original = Path.rename

        def rename(path, target):
            if path.name == "v1" and path.parent.name.startswith(".snapshot-"):
                raise OSError("promotion failed")
            return original(path, target)

        with patch.object(Path, "rename", rename):
            with self.assertRaises(OSError):
                ingest.publish_snapshot(self.out, {"feed_en.json": {}})
        self.assertEqual(before, self.snapshot())

    def test_diagnostics_cannot_overwrite_accepted_snapshot(self):
        self.diag = self.out / "v1/status.json"
        with self.assertRaises(SystemExit):
            self.run_ingest({})

    def test_publication_error_is_reported_without_replacing_old_feeds(self):
        self.seed()
        before = self.snapshot()
        with patch.object(ingest, "publish_snapshot", side_effect=OSError("disk full")):
            self.assertEqual(1, self.run_ingest({
                s["id"]: [article(s["id"], s["lang"], age=i, n=i + 1)
                         for i in range(3)]
                for s in self.sources
            }))
        self.assertEqual(before, self.snapshot())
        self.assertFalse(json.loads(self.diag.read_text())["accepted"])

    def test_unknown_dates_stay_stable_across_serialization(self):
        # 3 undated articles per source to clear the quality bar; only the
        # `n=1` article is tracked for first-seen stability, the other two
        # are filler so the language-level freshness gate passes.
        def batch(age, estimated=True):
            return {s["id"]: [article(s["id"], s["lang"], age=age, estimated=estimated, n=i + 1)
                             for i in range(3)]
                    for s in self.sources}

        def tracked(payload_path):
            data = json.loads(payload_path.read_text())["articles"]
            return next(a for a in data if a["url"] == "https://a.lk/news/1")

        self.assertEqual(0, self.run_ingest(batch(age=3)))
        old = tracked(self.out / "v1/feed_en.json")
        self.assertEqual(0, self.run_ingest(batch(age=0)))
        new = tracked(self.out / "v1/feed_en.json")
        self.assertEqual(old["published"], new["published"])
        self.assertEqual(old["collected_at"], new["collected_at"])
        self.assertTrue(new["date_estimated"])
        report = json.loads(self.diag.read_text())
        self.assertEqual(["a", "b", "c"], report["stale_sources"])
        self.assertEqual(0, report["detail"]["a"]["new_articles"])


class CategoryTests(unittest.TestCase):
    """sources.yaml-declared categories, and their survival across runs."""

    def apply(self, src, arts):
        ingest.apply_source_categories({src["id"]: src}, arts)
        return arts

    def test_url_categories_bucket_by_path_segment(self):
        src = {"id": "divaina", "url_categories": {"sports-news": "sports"}}
        a = article("divaina")
        a.url = "https://www.divaina.lk/sports-news/match-report"
        self.assertEqual("sports", self.apply(src, [a])[0].category)

    def test_url_categories_ignores_unmapped_segment(self):
        """A placement segment must not borrow some other section's bucket."""
        src = {"id": "divaina", "url_categories": {"sports-news": "sports"}}
        a = article("divaina")
        a.url = "https://www.divaina.lk/main-news/budget"
        self.assertIsNone(self.apply(src, [a])[0].category)

    def test_url_categories_matches_a_percent_encoded_segment(self):
        """
        Tamil Mirror files stories under Tamil section names, so the path
        arrives percent-encoded. sources.yaml carries the readable slug.
        """
        src = {"id": "tamil-mirror",
               "url_categories": {"உலக-செய்திகள்": "international"}}
        a = article("tamil-mirror", lang="ta")
        a.url = ("https://www.tamilmirror.lk/%E0%AE%89%E0%AE%B2%E0%AE%95-"
                 "%E0%AE%9A%E0%AF%86%E0%AE%AF%E0%AF%8D%E0%AE%A4%E0%AE%BF"
                 "%E0%AE%95%E0%AE%B3%E0%AF%8D/123")
        self.assertEqual("international", self.apply(src, [a])[0].category)

    def test_url_categories_skips_unresolved_google_link(self):
        """
        The path of a news.google.com redirect belongs to Google, not the
        publisher -- matching against it would bucket by an opaque id.
        """
        src = {"id": "divaina", "url_categories": {"articles": "sports"}}
        a = article("divaina")
        a.url = "https://news.google.com/rss/articles/CBMiK0FVX3lxTE"
        self.assertIsNone(self.apply(src, [a])[0].category)

    def test_pinned_category_applies_to_every_article(self):
        src = {"id": "lk", "category": "business"}
        self.assertEqual("business", self.apply(src, [article("lk")])[0].category)

    def test_adapter_category_beats_sources_yaml(self):
        """A publisher's own taxonomy is better evidence than our guess."""
        src = {"id": "lk", "category": "business",
               "url_categories": {"news": "politics"}}
        a = article("lk")
        a.category = "sports"
        self.assertEqual("sports", self.apply(src, [a])[0].category)

    def test_url_categories_beats_pinned_category(self):
        src = {"id": "lk", "category": "business",
               "url_categories": {"news": "politics"}}
        self.assertEqual("politics", self.apply(src, [article("lk")])[0].category)

    def test_unknown_bucket_is_dropped_on_load(self):
        """
        ingest filters category feeds with `a.category == cat`, so a typo would
        reach feed_{lang}.json and then never produce a category feed.
        """
        sources = [{"id": "lk", "category": "buisness",
                    "url_categories": {"sport": "sprots", "biz": "business"}}]
        ingest.validate_source_categories(sources)
        self.assertNotIn("category", sources[0])
        self.assertEqual({"biz": "business"}, sources[0]["url_categories"])

    def test_valid_config_survives_validation(self):
        sources = [{"id": "lk", "category": "business",
                    "url_categories": {"sport": "sports"}}]
        ingest.validate_source_categories(sources)
        self.assertEqual("business", sources[0]["category"])
        self.assertEqual({"sport": "sports"}, sources[0]["url_categories"])


class CategoryPersistenceTests(SnapshotHarness):
    """A category must not evaporate when a source degrades for one cycle."""

    def items(self, categorised):
        out = {}
        for s in self.sources:
            arts = [article(s["id"], s["lang"], age=i, n=i + 1) for i in range(3)]
            if categorised:
                arts[0].category = "business"
            out[s["id"]] = arts
        return out

    def category_of(self, lang, n=1):
        feed = json.loads((self.out / f"v1/feed_{lang}.json").read_text(encoding="utf-8"))
        art = next(a for a in feed["articles"] if a["url"].endswith(f"/news/{n}"))
        return art.get("category")

    def test_category_is_written_and_round_trips(self):
        self.assertEqual(0, self.run_ingest(self.items(categorised=True)))
        self.assertEqual("business", self.category_of("en"))
        self.assertTrue((self.out / "v1/feed_en_business.json").exists())

    def test_category_survives_a_categoryless_refetch(self):
        """
        dedupe ranks fresh over cached before it looks at metadata, and then
        replaces the object wholesale. One run of a source falling back to
        Google would otherwise erase the bucket permanently.
        """
        self.assertEqual(0, self.run_ingest(self.items(categorised=True)))
        self.assertEqual(0, self.run_ingest(self.items(categorised=False)))
        self.assertEqual("business", self.category_of("en"))

    def test_status_reports_category_coverage(self):
        self.assertEqual(0, self.run_ingest(self.items(categorised=True)))
        status = json.loads(self.diag.read_text(encoding="utf-8"))
        en = status["language_status"]["en"]
        self.assertEqual(1, en["categorised"])
        self.assertEqual(2, en["uncategorised"])
        # every bucket is reported, so the counts can drive tab labels directly
        self.assertEqual(1, en["categories"]["business"])
        self.assertEqual(0, en["categories"]["politics"])
        self.assertEqual(set(ingest.CATEGORIES), set(en["categories"]))


class CategoryEndpointTests(SnapshotHarness):
    """Every category endpoint exists, so a 404 means a real error."""

    def unreachable_bar(self):
        """
        Rewrite the config so no language can clear the freshness bar.

        Sources still return articles -- returning none instead would trip the
        separate total-failure guard, which is a different code path.
        """
        self.config.write_text(
            yaml.safe_dump({"defaults": {"lang_min_fresh_articles": 99},
                            "sources": self.sources}),
            encoding="utf-8")

    def items(self):
        return {s["id"]: [article(s["id"], s["lang"], age=i, n=i + 1)
                          for i in range(3)]
                for s in self.sources}

    def payload(self, name):
        return json.loads((self.out / "v1" / name).read_text(encoding="utf-8"))

    def test_every_category_is_published_even_when_empty(self):
        self.seed()
        for lang in ("en", "si", "ta"):
            for cat in ingest.CATEGORIES:
                path = self.out / f"v1/feed_{lang}_{cat}.json"
                self.assertTrue(path.exists(), f"missing feed_{lang}_{cat}.json")

    def test_an_empty_category_is_a_valid_empty_payload(self):
        """Not a stub: same envelope as a populated feed, with zero articles."""
        self.seed()
        empty = self.payload("feed_en_politics.json")
        self.assertEqual(0, empty["count"])
        self.assertEqual([], empty["articles"])
        self.assertEqual("politics", empty["category"])
        self.assertEqual("en", empty["lang"])
        self.assertIn("generated", empty)

    def test_preserved_language_still_publishes_every_category(self):
        """
        The preserved branch copies last run's category files. One that has no
        previous file must be synthesised rather than left to 404.
        """
        self.seed()
        (self.out / "v1/feed_en_politics.json").unlink()
        before = self.payload("feed_en.json")["generated"]

        self.unreachable_bar()
        self.assertEqual(0, self.run_ingest(self.items()))

        self.assertEqual("preserved",
                         json.loads(self.diag.read_text(encoding="utf-8"))
                         ["language_status"]["en"]["state"])
        restored = self.payload("feed_en_politics.json")
        self.assertEqual(0, restored["count"])
        # the preserved snapshot's timestamp, not this run's
        self.assertEqual(before, restored["generated"])

    def test_unavailable_language_publishes_no_category_files(self):
        """
        The one documented exception: with no feed_{lang}.json there are no
        feed_{lang}_{cat}.json either, rather than claiming a language exists.
        """
        self.unreachable_bar()
        self.run_ingest(self.items())
        self.assertFalse((self.out / "v1/feed_en.json").exists())
        for cat in ingest.CATEGORIES:
            self.assertFalse((self.out / f"v1/feed_en_{cat}.json").exists())

    def test_status_lists_every_category_including_zeros(self):
        self.seed()
        cats = (json.loads(self.diag.read_text(encoding="utf-8"))
                ["language_status"]["en"]["categories"])
        self.assertEqual(set(ingest.CATEGORIES), set(cats))
        self.assertEqual(0, cats["politics"])


class WorkflowTests(unittest.TestCase):
    def test_only_accepted_deployed_feeds_are_cached(self):
        root = Path(__file__).resolve().parents[1]
        workflow = yaml.load(
            (root / ".github/workflows/ingest.yml").read_text(encoding="utf-8"),
            Loader=yaml.BaseLoader,
        )
        steps = workflow["jobs"]["ingest"]["steps"]
        deploy = next(i for i, s in enumerate(steps) if s.get("id") == "deployment")
        save = next(i for i, s in enumerate(steps)
                    if s.get("uses", "").startswith("actions/cache/save"))
        self.assertGreater(save, deploy)
        self.assertNotIn("if", steps[save])
        self.assertTrue(steps[save]["with"]["key"].startswith("accepted-feeds-v2-"))
        self.assertEqual("github.ref == 'refs/heads/main'", workflow["jobs"]["ingest"]["if"])


if __name__ == "__main__":
    unittest.main()
