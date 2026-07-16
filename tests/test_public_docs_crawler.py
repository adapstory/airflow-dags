from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from dags.public_docs_crawler import CrawlResponse, crawl_public_docs


@dataclass
class FakeFetcher:
    responses: dict[str, CrawlResponse]
    requests: list[tuple[str, dict[str, str]]]

    def __call__(self, url: str, headers: Mapping[str, str]) -> CrawlResponse:
        self.requests.append((url, dict(headers)))
        return self.responses[url]


def test_crawler_discovers_links_and_uses_robots_sitemap_and_conditional_headers() -> None:
    root = "https://docs.example.com/"
    page = "https://docs.example.com/guide"
    new_page = "https://docs.example.com/new"
    fetcher = FakeFetcher(
        {
            "https://docs.example.com/robots.txt": CrawlResponse(
                200,
                {"content-type": "text/plain"},
                b"User-agent: *\nAllow: /\nSitemap: https://docs.example.com/sitemap.xml\n",
            ),
            "https://docs.example.com/sitemap.xml": CrawlResponse(
                200,
                {"content-type": "application/xml"},
                f"<urlset><url><loc>{root}</loc></url><url><loc>{page}</loc></url></urlset>".encode(),
            ),
            root: CrawlResponse(
                200,
                {"content-type": "text/html", "etag": '"root-v2"'},
                f'<a href="{page}">guide</a><a href="{new_page}">new</a>'.encode(),
            ),
            page: CrawlResponse(304, {"etag": '"page-v1"'}, b""),
            new_page: CrawlResponse(
                200,
                {"content-type": "text/html", "last-modified": "Wed, 10 Jul 2026 10:00:00 GMT"},
                b"new page",
            ),
        },
        [],
    )

    evidence = crawl_public_docs(
        seed_uri=root,
        crawl_policy={
            "allowed_domains": ["docs.example.com"],
            "deny_patterns": ["/admin"],
            "max_depth": 2,
            "max_pages": 10,
            "respect_robots_txt": True,
            "sitemap_discovery": True,
            "user_agent": "serp-test/1",
        },
        previous_state={
            page: {
                "content_hash": "old-page-hash",
                "etag": '"page-v1"',
                "last_modified": None,
                "status": "active",
            }
        },
        fetcher=fetcher,
    )

    assert evidence["status"] == "completed"
    assert evidence["changed_urls"] == [root, new_page]
    assert evidence["unchanged_urls"] == [page]
    assert evidence["deleted_urls"] == []
    page_request = next(headers for url, headers in fetcher.requests if url == page)
    assert page_request["If-None-Match"] == '"page-v1"'
    assert evidence["pages"][page]["status"] == "unchanged"


def test_crawler_marks_sitemap_pages_deleted_and_keeps_deny_and_robots_fail_closed() -> None:
    root = "https://docs.example.com/"
    removed = "https://docs.example.com/removed"
    blocked = "https://docs.example.com/admin"
    fetcher = FakeFetcher(
        {
            "https://docs.example.com/robots.txt": CrawlResponse(
                200,
                {"content-type": "text/plain"},
                b"User-agent: *\nDisallow: /admin\nSitemap: https://docs.example.com/sitemap.xml\n",
            ),
            "https://docs.example.com/sitemap.xml": CrawlResponse(
                200,
                {"content-type": "application/xml"},
                f"<urlset><url><loc>{root}</loc></url></urlset>".encode(),
            ),
            root: CrawlResponse(
                200,
                {"content-type": "text/html"},
                f'<a href="{blocked}">blocked</a>'.encode(),
            ),
        },
        [],
    )

    evidence = crawl_public_docs(
        seed_uri=root,
        crawl_policy={
            "allowed_domains": ["docs.example.com"],
            "deny_patterns": ["/admin"],
            "max_depth": 1,
            "max_pages": 5,
            "respect_robots_txt": True,
            "sitemap_discovery": True,
            "user_agent": "serp-test/1",
        },
        previous_state={
            removed: {
                "content_hash": "removed-hash",
                "etag": None,
                "last_modified": None,
                "status": "active",
            }
        },
        fetcher=fetcher,
    )

    assert evidence["deleted_urls"] == [removed]
    assert evidence["pages"][removed]["status"] == "deleted"
    assert evidence["pages"][blocked]["status"] == "blocked"
    assert all(url != blocked for url, _ in fetcher.requests)


def test_crawler_does_not_delete_state_when_authoritative_discovery_is_unavailable() -> None:
    root = "https://docs.example.com/"
    prior = "https://docs.example.com/prior"
    fetcher = FakeFetcher(
        {
            "https://docs.example.com/robots.txt": CrawlResponse(503, {}, b""),
        },
        [],
    )

    evidence = crawl_public_docs(
        seed_uri=root,
        crawl_policy={
            "allowed_domains": ["docs.example.com"],
            "deny_patterns": [],
            "max_depth": 1,
            "max_pages": 5,
            "respect_robots_txt": True,
            "sitemap_discovery": True,
            "user_agent": "serp-test/1",
        },
        previous_state={
            prior: {
                "content_hash": "prior-hash",
                "etag": None,
                "last_modified": None,
                "status": "active",
            }
        },
        fetcher=fetcher,
    )

    assert evidence["status"] == "blocked"
    assert evidence["deleted_urls"] == []
    assert evidence["failure"]["code"] == "ROBOTS_FETCH_FAILED"


def test_crawler_records_missing_robots_as_implicit_allow() -> None:
    root = "https://docs.example.com/"
    fetcher = FakeFetcher(
        {
            "https://docs.example.com/robots.txt": CrawlResponse(404, {}, b""),
            "https://docs.example.com/sitemap.xml": CrawlResponse(404, {}, b""),
            root: CrawlResponse(
                200, {"content-type": "text/html"}, b"<html><body>docs</body></html>"
            ),
        },
        [],
    )

    evidence = crawl_public_docs(
        seed_uri=root,
        crawl_policy={
            "allowed_domains": ["docs.example.com"],
            "deny_patterns": [],
            "max_depth": 1,
            "max_pages": 5,
            "respect_robots_txt": True,
            "sitemap_discovery": True,
            "user_agent": "serp-test/1",
        },
        previous_state={},
        fetcher=fetcher,
    )

    assert evidence["status"] == "completed"
    assert evidence["robots"] == {
        "http_status": 404,
        "policy": "implicit_allow",
        "url": "https://docs.example.com/robots.txt",
    }
    assert evidence["changed_urls"] == [root]


def test_crawler_preserves_directory_seed_base_and_ignores_non_navigation_hrefs() -> None:
    root = "https://docs.example.com/latest/"
    sitemap = "https://docs.example.com/latest/sitemap.xml"
    guide = "https://docs.example.com/latest/guide"
    static_asset = "https://docs.example.com/latest/site.css"
    fetcher = FakeFetcher(
        {
            "https://docs.example.com/robots.txt": CrawlResponse(404, {}, b""),
            sitemap: CrawlResponse(404, {}, b""),
            root: CrawlResponse(
                200,
                {"content-type": "text/html"},
                b'<link rel="stylesheet" href="site.css"><a href="guide">guide</a>',
            ),
            guide: CrawlResponse(200, {"content-type": "text/html"}, b"guide"),
        },
        [],
    )

    evidence = crawl_public_docs(
        seed_uri=root,
        crawl_policy={
            "allowed_domains": ["docs.example.com"],
            "deny_patterns": [],
            "max_depth": 1,
            "max_pages": 5,
            "respect_robots_txt": True,
            "sitemap_discovery": True,
            "user_agent": "serp-test/1",
        },
        previous_state={},
        fetcher=fetcher,
    )

    assert evidence["status"] == "completed"
    assert evidence["changed_urls"] == [root, guide]
    assert static_asset not in [url for url, _ in fetcher.requests]
    assert [url for url, _ in fetcher.requests] == [
        "https://docs.example.com/robots.txt",
        sitemap,
        root,
        guide,
    ]


def test_crawler_confines_html_and_sitemap_discovery_to_version_seed_path() -> None:
    root = "https://kafka.apache.org/43/"
    current_guide = "https://kafka.apache.org/43/getting-started/"
    current_config = "https://kafka.apache.org/43/configuration/"
    historical_sitemap_page = "https://kafka.apache.org/37/"
    historical_html_page = "https://kafka.apache.org/38/"
    sitemap = "https://kafka.apache.org/sitemap.xml"
    fetcher = FakeFetcher(
        {
            "https://kafka.apache.org/robots.txt": CrawlResponse(
                200,
                {"content-type": "text/plain"},
                f"User-agent: *\nAllow: /\nSitemap: {sitemap}\n".encode(),
            ),
            sitemap: CrawlResponse(
                200,
                {"content-type": "application/xml"},
                (
                    "<urlset>"
                    f"<url><loc>{root}</loc></url>"
                    f"<url><loc>{current_guide}</loc></url>"
                    f"<url><loc>{historical_sitemap_page}</loc></url>"
                    "</urlset>"
                ).encode(),
            ),
            root: CrawlResponse(
                200,
                {"content-type": "text/html"},
                (
                    f'<a href="{current_config}">current</a>'
                    f'<a href="{historical_html_page}">historical</a>'
                ).encode(),
            ),
            current_guide: CrawlResponse(200, {"content-type": "text/html"}, b"guide"),
            current_config: CrawlResponse(200, {"content-type": "text/html"}, b"config"),
        },
        [],
    )

    evidence = crawl_public_docs(
        seed_uri=root,
        crawl_policy={
            "allowed_domains": ["kafka.apache.org"],
            "curated_frontier_urls": [],
            "deny_patterns": [],
            "max_depth": 2,
            "max_pages": 10,
            "respect_robots_txt": True,
            "sitemap_discovery": True,
            "user_agent": "serp-test/1",
        },
        previous_state={},
        fetcher=fetcher,
    )

    requested_urls = [url for url, _ in fetcher.requests]
    assert evidence["changed_urls"] == [root, current_config, current_guide]
    assert evidence["pages"][historical_html_page]["reason"] == "CRAWL_POLICY_DENIED"
    assert historical_sitemap_page not in requested_urls
    assert historical_html_page not in requested_urls
    assert sitemap in requested_urls


def test_crawler_bounds_policy_denied_evidence_to_max_pages_without_losing_total() -> None:
    root = "https://kafka.apache.org/43/"
    sitemap = "https://kafka.apache.org/sitemap.xml"
    denied_urls = [f"https://kafka.apache.org/{version}/" for version in range(10, 20)]
    fetcher = FakeFetcher(
        {
            "https://kafka.apache.org/robots.txt": CrawlResponse(
                200,
                {"content-type": "text/plain"},
                f"User-agent: *\nAllow: /\nSitemap: {sitemap}\n".encode(),
            ),
            sitemap: CrawlResponse(
                200,
                {"content-type": "application/xml"},
                (
                    "<urlset>"
                    f"<url><loc>{root}</loc></url>"
                    + "".join(f"<url><loc>{url}</loc></url>" for url in denied_urls)
                    + "</urlset>"
                ).encode(),
            ),
            root: CrawlResponse(200, {"content-type": "text/html"}, b"root"),
        },
        [],
    )

    evidence = crawl_public_docs(
        seed_uri=root,
        crawl_policy={
            "allowed_domains": ["kafka.apache.org"],
            "curated_frontier_urls": [],
            "deny_patterns": [],
            "max_depth": 1,
            "max_pages": 3,
            "respect_robots_txt": True,
            "sitemap_discovery": True,
            "user_agent": "serp-test/1",
        },
        previous_state={},
        fetcher=fetcher,
    )

    assert evidence["summary"]["blocked"] == len(denied_urls)
    assert len(evidence["blocked_urls"]) == 3
    assert len([page for page in evidence["pages"].values() if page["status"] == "blocked"]) == 3
    assert evidence["evidence_limits"] == {
        "blocked_observations": len(denied_urls),
        "blocked_urls_recorded": 3,
        "blocked_urls_truncated": True,
        "max_pages": 3,
    }


def test_crawler_allows_an_explicit_curated_frontier_without_opening_sibling_paths() -> None:
    root = "https://docs.example.com/latest/"
    curated = "https://docs.example.com/release-notes/"
    unrelated = "https://docs.example.com/legacy/"
    fetcher = FakeFetcher(
        {
            "https://docs.example.com/robots.txt": CrawlResponse(404, {}, b""),
            "https://docs.example.com/latest/sitemap.xml": CrawlResponse(404, {}, b""),
            root: CrawlResponse(
                200,
                {"content-type": "text/html"},
                (
                    f'<a href="{curated}">curated</a>' f'<a href="{unrelated}">unrelated</a>'
                ).encode(),
            ),
            curated: CrawlResponse(200, {"content-type": "text/html"}, b"release notes"),
        },
        [],
    )

    evidence = crawl_public_docs(
        seed_uri=root,
        crawl_policy={
            "allowed_domains": ["docs.example.com"],
            "curated_frontier_urls": [curated],
            "deny_patterns": [],
            "max_depth": 1,
            "max_pages": 5,
            "respect_robots_txt": True,
            "sitemap_discovery": True,
            "user_agent": "serp-test/1",
        },
        previous_state={},
        fetcher=fetcher,
    )

    assert evidence["changed_urls"] == [root, curated]
    assert evidence["pages"][unrelated]["reason"] == "CRAWL_POLICY_DENIED"
    assert unrelated not in [url for url, _ in fetcher.requests]


def test_crawler_ignores_linked_media_assets_for_quarantine() -> None:
    root = "https://neo4j.com/docs/"
    guide = "https://neo4j.com/docs/operations-manual/current/"
    missing_image = "https://neo4j.com/wp-content/uploads/titanic-1.png"
    fetcher = FakeFetcher(
        {
            "https://neo4j.com/robots.txt": CrawlResponse(
                200,
                {"content-type": "text/plain"},
                b"User-agent: *\nAllow: /\nSitemap: https://neo4j.com/sitemap.xml\n",
            ),
            "https://neo4j.com/sitemap.xml": CrawlResponse(
                200,
                {"content-type": "application/xml"},
                f"<urlset><url><loc>{root}</loc></url></urlset>".encode(),
            ),
            root: CrawlResponse(
                200,
                {"content-type": "text/html"},
                (f'<a href="{guide}">operations</a><a href="{missing_image}">diagram</a>').encode(),
            ),
            guide: CrawlResponse(200, {"content-type": "text/html"}, b"guide"),
        },
        [],
    )

    evidence = crawl_public_docs(
        seed_uri=root,
        crawl_policy={
            "allowed_domains": ["neo4j.com"],
            "deny_patterns": ["/login", "/admin"],
            "max_depth": 2,
            "max_pages": 10,
            "respect_robots_txt": True,
            "sitemap_discovery": True,
            "user_agent": "serp-test/1",
        },
        previous_state={},
        fetcher=fetcher,
    )

    assert evidence["status"] == "completed"
    assert evidence["failed_urls"] == []
    assert missing_image not in evidence["pages"]
    assert missing_image not in [url for url, _ in fetcher.requests]


def test_crawler_quarantines_a_recoverable_page_failure_without_tombstoning_state() -> None:
    root = "https://docs.example.com/"
    protected = "https://docs.example.com/protected"
    fetcher = FakeFetcher(
        {
            "https://docs.example.com/robots.txt": CrawlResponse(
                200,
                {"content-type": "text/plain"},
                b"User-agent: *\nAllow: /\nSitemap: https://docs.example.com/sitemap.xml\n",
            ),
            "https://docs.example.com/sitemap.xml": CrawlResponse(
                200,
                {"content-type": "application/xml"},
                f"<urlset><url><loc>{root}</loc></url><url><loc>{protected}</loc></url></urlset>".encode(),
            ),
            root: CrawlResponse(200, {"content-type": "text/html"}, b"root"),
            protected: CrawlResponse(429, {"retry-after": "60"}, b""),
        },
        [],
    )

    evidence = crawl_public_docs(
        seed_uri=root,
        crawl_policy={
            "allowed_domains": ["docs.example.com"],
            "deny_patterns": [],
            "max_depth": 1,
            "max_pages": 5,
            "respect_robots_txt": True,
            "sitemap_discovery": True,
            "user_agent": "serp-test/1",
        },
        previous_state={
            protected: {
                "content_hash": "protected-hash",
                "etag": None,
                "last_modified": None,
                "status": "active",
            }
        },
        fetcher=fetcher,
    )

    assert evidence["status"] == "quarantined"
    assert evidence["failed_urls"] == [protected]
    assert evidence["deleted_urls"] == []
    assert evidence["quarantine"]["reason_codes"] == ["HTTP_429"]
