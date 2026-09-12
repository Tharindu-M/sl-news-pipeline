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


def article(sid="a", lang="en", age=1, estimated=False):
    url = f"https://{sid}.lk/news/1"
    return Article(
        id=article_id(url), source_id=sid, source_name=sid, lang=lang,
        title=f"{sid} headline", url=url,
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


class SnapshotTests(unittest.TestCase):
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
        items = {s["id"]: [article(s["id"], s["lang"])] for s in self.sources}
        self.assertEqual(0, self.run_ingest(items))

    def snapshot(self):
        return {p.name: p.read_bytes() for p in (self.out / "v1").iterdir()}

    def test_partial_and_total_failures_preserve_all_accepted_files(self):
        self.seed()
        before = self.snapshot()
        for items in ({"a": [article(age=0)]}, {}):
            self.assertEqual(1, self.run_ingest(items))
            self.assertEqual(before, self.snapshot())
            report = json.loads(self.diag.read_text(encoding="utf-8"))
            self.assertFalse(report["accepted"])
            self.assertTrue(report["errors"])

    def test_empty_language_does_not_publish_partial_snapshot(self):
        self.assertEqual(1, self.run_ingest({
            "a": [article()], "b": [article("b", "si")],
        }))
        self.assertFalse((self.out / "v1").exists())

    def test_obsolete_category_file_removed_on_success(self):
        self.seed()
        stale_file = self.out / "v1/feed_en_politics.json"
        stale_file.write_text('{"articles":[]}', encoding="utf-8")
        self.seed()
        self.assertFalse(stale_file.exists())

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
            self.assertEqual(1, self.run_ingest(
                {s["id"]: [article(s["id"], s["lang"])] for s in self.sources}
            ))
        self.assertEqual(before, self.snapshot())
        self.assertFalse(json.loads(self.diag.read_text())["accepted"])

    def test_unknown_dates_stay_stable_across_serialization(self):
        first = {s["id"]: [article(s["id"], s["lang"], age=3, estimated=True)]
                 for s in self.sources}
        self.assertEqual(0, self.run_ingest(first))
        old = json.loads((self.out / "v1/feed_en.json").read_text())["articles"][0]
        second = {s["id"]: [article(s["id"], s["lang"], age=0, estimated=True)]
                  for s in self.sources}
        self.assertEqual(0, self.run_ingest(second))
        new = json.loads((self.out / "v1/feed_en.json").read_text())["articles"][0]
        self.assertEqual(old["published"], new["published"])
        self.assertEqual(old["collected_at"], new["collected_at"])
        self.assertTrue(new["date_estimated"])
        report = json.loads(self.diag.read_text())
        self.assertEqual(["a", "b", "c"], report["stale_sources"])
        self.assertEqual(0, report["detail"]["a"]["new_articles"])


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
