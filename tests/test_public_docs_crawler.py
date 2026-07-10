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
                (
                    f'<a href="{guide}">operations</a>' f'<a href="{missing_image}">diagram</a>'
                ).encode(),
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
