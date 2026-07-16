from __future__ import annotations

from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urldefrag, urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser
from xml.etree import ElementTree

CRAWLER_CONTRACT_VERSION = "2026.07.2"
_MAX_DISCOVERY_BYTES = 1_000_000
_MAX_SITEMAP_DEPTH = 2
_NON_DOCUMENT_ASSET_SUFFIXES = frozenset(
    {
        ".7z",
        ".avif",
        ".bmp",
        ".css",
        ".eot",
        ".gif",
        ".gz",
        ".ico",
        ".jpeg",
        ".jpg",
        ".js",
        ".map",
        ".mjs",
        ".mov",
        ".mp3",
        ".mp4",
        ".otf",
        ".png",
        ".rar",
        ".svg",
        ".tar",
        ".tgz",
        ".tif",
        ".tiff",
        ".ttf",
        ".wav",
        ".webm",
        ".webp",
        ".woff",
        ".woff2",
        ".zip",
    }
)


@dataclass(frozen=True, slots=True)
class CrawlResponse:
    status_code: int
    headers: Mapping[str, str]
    body: bytes


Fetcher = Callable[[str, Mapping[str, str]], CrawlResponse]


def crawl_public_docs(
    *,
    seed_uri: str,
    crawl_policy: Mapping[str, Any],
    previous_state: Mapping[str, Mapping[str, Any]],
    fetcher: Fetcher,
) -> dict[str, Any]:
    """Crawl a governed public-docs website and produce replayable change evidence."""

    seed_uri = _canonical_url(seed_uri)
    policy = _with_governed_path_scopes(_validate_policy(crawl_policy), seed_uri)
    _require_allowed_url(seed_uri, policy)
    robots_url = _canonical_url(urljoin(seed_uri, "/robots.txt"))
    robots_response = fetcher(robots_url, {"User-Agent": str(policy["user_agent"])})
    if robots_response.status_code in {404, 410}:
        robot_parser = RobotFileParser(robots_url)
        robot_parser.parse(())
        robots_policy = "implicit_allow"
    elif robots_response.status_code != 200:
        return _blocked_evidence("ROBOTS_FETCH_FAILED", f"status={robots_response.status_code}")
    else:
        if len(robots_response.body) > _MAX_DISCOVERY_BYTES:
            return _blocked_evidence(
                "ROBOTS_PAYLOAD_TOO_LARGE", "robots.txt exceeds discovery limit"
            )
        robot_parser = RobotFileParser(robots_url)
        robot_parser.parse(robots_response.body.decode("utf-8", errors="replace").splitlines())
        robots_policy = "parsed"
    if not robot_parser.can_fetch(str(policy["user_agent"]), seed_uri):
        return _blocked_evidence("ROBOTS_DENIED", seed_uri)

    sitemap_urls = _sitemap_urls(seed_uri, robot_parser)
    (
        sitemap_pages,
        authoritative_discovery,
        sitemap_blocked_pages,
        sitemap_blocked_observations,
    ) = _discover_sitemap_pages(
        sitemap_urls=sitemap_urls,
        robot_parser=robot_parser,
        policy=policy,
        fetcher=fetcher,
    )
    max_pages = int(policy["max_pages"])
    max_depth = int(policy["max_depth"])
    queue: deque[tuple[str, int]] = deque(
        (url, 0 if url == seed_uri else 1) for url in [seed_uri, *sitemap_pages]
    )
    queued = set(queue_url for queue_url, _ in queue)
    pages: dict[str, dict[str, Any]] = {
        url: _page_report(url, "blocked", "CRAWL_POLICY_DENIED") for url in sitemap_blocked_pages
    }
    state: dict[str, dict[str, Any]] = {}
    changed_urls: list[str] = []
    unchanged_urls: list[str] = []
    deleted_urls: list[str] = []
    failed_urls: list[str] = []
    blocked_urls: list[str] = list(sitemap_blocked_pages)
    blocked_urls_recorded = set(blocked_urls)
    blocked_observations = sitemap_blocked_observations
    crawled_urls: set[str] = set()
    discovery_complete = True

    def record_blocked_url(url: str, reason: str) -> None:
        nonlocal blocked_observations
        blocked_observations += 1
        if url in blocked_urls_recorded or len(blocked_urls_recorded) >= max_pages:
            return
        blocked_urls_recorded.add(url)
        blocked_urls.append(url)
        pages[url] = _page_report(url, "blocked", reason)

    while queue:
        url, depth = queue.popleft()
        if url in crawled_urls:
            continue
        if len(crawled_urls) >= max_pages:
            discovery_complete = False
            break
        crawled_urls.add(url)
        if not robot_parser.can_fetch(str(policy["user_agent"]), url):
            record_blocked_url(url, "ROBOTS_DENIED")
            continue
        prior = previous_state.get(url)
        request_headers = {"User-Agent": str(policy["user_agent"])}
        _add_conditional_headers(request_headers, prior)
        response = fetcher(url, request_headers)
        status, reason = _classify_response(response, prior)
        headers = _normalized_headers(response.headers)
        content_hash = (
            sha256(response.body).hexdigest() if response.status_code == 200 else _prior_hash(prior)
        )
        pages[url] = _page_report(url, status, reason, response, content_hash)
        if status in {"changed", "new"}:
            changed_urls.append(url)
            state[url] = _active_state(_required_hash(content_hash), headers, response.status_code)
        elif status == "unchanged":
            unchanged_urls.append(url)
            state[url] = _active_state(
                _required_hash(content_hash), headers, response.status_code, prior
            )
        elif status == "deleted":
            deleted_urls.append(url)
            state[url] = {"status": "tombstoned", "content_hash": _prior_hash(prior)}
        elif status == "blocked":
            blocked_urls.append(url)
        else:
            failed_urls.append(url)

        if response.status_code == 200 and status in {"changed", "new"} and depth < max_depth:
            if _is_html(response.headers, response.body):
                for link in _extract_links(url, response.body, max_links=max_pages):
                    if link in queued:
                        continue
                    if not _is_crawl_document_url(link):
                        continue
                    if _is_allowed_url(link, policy) and robot_parser.can_fetch(
                        str(policy["user_agent"]), link
                    ):
                        queue.append((link, depth + 1))
                        queued.add(link)
                    elif _is_same_domain(link, policy):
                        record_blocked_url(link, "CRAWL_POLICY_DENIED")

    if authoritative_discovery and discovery_complete:
        for url, prior in previous_state.items():
            if prior.get("status") != "active" or url in crawled_urls or url in sitemap_pages:
                continue
            if not _is_allowed_url(url, policy):
                continue
            deleted_urls.append(url)
            pages[url] = _page_report(url, "deleted", "URL_REMOVED_FROM_SITEMAP")
            state[url] = {"status": "tombstoned", "content_hash": _prior_hash(prior)}

    quarantine_reason_codes = sorted(
        {
            str(page["reason"])
            for page in pages.values()
            if page.get("status") in {"blocked", "failed"}
            and isinstance(page.get("reason"), str)
            and str(page["reason"]).startswith("HTTP_")
        }
    )
    quarantined = bool(quarantine_reason_codes)
    evidence: dict[str, Any] = {
        "artifact_type": "public_docs_crawler_evidence",
        "contract_version": CRAWLER_CONTRACT_VERSION,
        "status": "quarantined" if quarantined else "completed",
        "seed_uri": seed_uri,
        "robots": {
            "http_status": robots_response.status_code,
            "policy": robots_policy,
            "url": robots_url,
        },
        "crawl_scope": {
            "seed_path_prefix": _crawl_path_prefix(seed_uri),
            "curated_path_prefixes": [
                _crawl_path_prefix(url) for url in policy["curated_frontier_urls"]
            ],
        },
        "discovery_complete": discovery_complete,
        "authoritative_discovery": authoritative_discovery,
        "changed_urls": sorted(set(changed_urls)),
        "unchanged_urls": sorted(set(unchanged_urls)),
        "deleted_urls": sorted(set(deleted_urls)),
        "failed_urls": sorted(set(failed_urls)),
        "blocked_urls": sorted(set(blocked_urls)),
        "pages": pages,
        "state": state,
        "summary": {
            "changed": len(set(changed_urls)),
            "unchanged": len(set(unchanged_urls)),
            "deleted": len(set(deleted_urls)),
            "failed": len(set(failed_urls)),
            "blocked": blocked_observations,
        },
        "evidence_limits": {
            "blocked_observations": blocked_observations,
            "blocked_urls_recorded": len(blocked_urls_recorded),
            "blocked_urls_truncated": blocked_observations > len(blocked_urls_recorded),
            "max_pages": max_pages,
        },
    }
    if quarantined:
        evidence["quarantine"] = {
            "reason_codes": quarantine_reason_codes,
            "replay_required": True,
            "retryable": True,
        }
    return evidence


def _validate_policy(policy: Mapping[str, Any]) -> dict[str, Any]:
    if policy.get("respect_robots_txt") is not True:
        raise ValueError("respect_robots_txt must be true")
    allowed_domains = policy.get("allowed_domains")
    if not isinstance(allowed_domains, list) or not all(
        isinstance(value, str) for value in allowed_domains
    ):
        raise ValueError("allowed_domains must be a list of strings")
    deny_patterns = policy.get("deny_patterns", [])
    if not isinstance(deny_patterns, list) or not all(
        isinstance(value, str) for value in deny_patterns
    ):
        raise ValueError("deny_patterns must be a list of strings")
    user_agent = policy.get("user_agent")
    if not isinstance(user_agent, str) or not user_agent.strip():
        raise ValueError("user_agent is required")
    max_depth = _bounded_int(policy, "max_depth", 0, 5)
    max_pages = _bounded_int(policy, "max_pages", 1, 500)
    curated_frontier_urls = policy.get("curated_frontier_urls", [])
    if not isinstance(curated_frontier_urls, list) or not all(
        isinstance(value, str) and value.strip() for value in curated_frontier_urls
    ):
        raise ValueError("curated_frontier_urls must be a list of non-empty strings")
    return {
        "allowed_domains": allowed_domains,
        "curated_frontier_urls": curated_frontier_urls,
        "deny_patterns": deny_patterns,
        "max_depth": max_depth,
        "max_pages": max_pages,
        "user_agent": user_agent,
    }


def _bounded_int(policy: Mapping[str, Any], key: str, lower: int, upper: int) -> int:
    value = policy.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or not lower <= value <= upper:
        raise ValueError(f"{key} must be between {lower} and {upper}")
    return value


def _sitemap_urls(seed_uri: str, robot_parser: RobotFileParser) -> list[str]:
    return [
        _canonical_url(value)
        for value in (robot_parser.site_maps() or [urljoin(seed_uri, "sitemap.xml")])
        if value
    ]


def _discover_sitemap_pages(
    *,
    sitemap_urls: Sequence[str],
    robot_parser: RobotFileParser,
    policy: Mapping[str, Any],
    fetcher: Fetcher,
) -> tuple[list[str], bool, list[str], int]:
    pages: list[str] = []
    blocked_pages: list[str] = []
    blocked_page_set: set[str] = set()
    blocked_observations = 0
    queue: deque[tuple[str, int]] = deque((url, 0) for url in sitemap_urls)
    seen: set[str] = set()
    authoritative = False
    while queue and len(pages) < int(policy["max_pages"]):
        sitemap_url, depth = queue.popleft()
        if sitemap_url in seen or depth > _MAX_SITEMAP_DEPTH:
            continue
        seen.add(sitemap_url)
        if not _is_allowed_discovery_resource_url(
            sitemap_url, policy
        ) or not robot_parser.can_fetch(str(policy["user_agent"]), sitemap_url):
            continue
        response = fetcher(sitemap_url, {"User-Agent": str(policy["user_agent"])})
        if response.status_code != 200 or len(response.body) > _MAX_DISCOVERY_BYTES:
            continue
        try:
            root = ElementTree.fromstring(response.body)
        except ElementTree.ParseError:
            continue
        authoritative = True
        root_name = root.tag.rsplit("}", 1)[-1].lower()
        locations = [
            child.text.strip()
            for child in root.iter()
            if child.tag.rsplit("}", 1)[-1].lower() == "loc" and child.text and child.text.strip()
        ]
        if root_name == "sitemapindex":
            queue.extend((_canonical_url(location), depth + 1) for location in locations)
        elif root_name == "urlset":
            for location in locations:
                candidate = _canonical_url(location)
                if (
                    _is_crawl_document_url(candidate)
                    and _is_allowed_url(candidate, policy)
                    and robot_parser.can_fetch(str(policy["user_agent"]), candidate)
                ):
                    pages.append(candidate)
                    if len(pages) >= int(policy["max_pages"]):
                        break
                elif _is_same_domain(candidate, policy):
                    blocked_observations += 1
                    if candidate not in blocked_page_set and len(blocked_page_set) < int(
                        policy["max_pages"]
                    ):
                        blocked_page_set.add(candidate)
                        blocked_pages.append(candidate)
    return (
        list(dict.fromkeys(pages)),
        authoritative,
        blocked_pages,
        blocked_observations,
    )


def _classify_response(
    response: CrawlResponse, prior: Mapping[str, Any] | None
) -> tuple[str, str | None]:
    if response.status_code == 304:
        return ("unchanged", "HTTP_NOT_MODIFIED")
    if response.status_code == 200:
        content_hash = sha256(response.body).hexdigest()
        return (
            ("unchanged", "CONTENT_HASH_UNCHANGED")
            if prior and prior.get("content_hash") == content_hash
            else ("changed" if prior else "new", None)
        )
    if response.status_code in {404, 410} and prior and prior.get("status") == "active":
        return "deleted", f"HTTP_{response.status_code}"
    if response.status_code in {401, 403}:
        return "blocked", f"HTTP_{response.status_code}"
    return "failed", f"HTTP_{response.status_code}"


def _extract_links(base_url: str, body: bytes, *, max_links: int) -> list[str]:
    parser = _LinkParser(max_links=max_links)
    parser.feed(body.decode("utf-8", errors="replace"))
    return list(dict.fromkeys(_canonical_url(urljoin(base_url, href)) for href in parser.links))


class _LinkParser(HTMLParser):
    def __init__(self, *, max_links: int) -> None:
        super().__init__(convert_charrefs=True)
        self.max_links = max_links
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() not in {"a", "area"}:
            return
        if len(self.links) >= self.max_links:
            return
        for key, value in attrs:
            if (
                key.lower() == "href"
                and value
                and not value.startswith(("#", "mailto:", "javascript:"))
            ):
                self.links.append(value)
                return


def _add_conditional_headers(headers: dict[str, str], prior: Mapping[str, Any] | None) -> None:
    if not prior:
        return
    etag = prior.get("etag")
    last_modified = prior.get("last_modified")
    if isinstance(etag, str) and etag:
        headers["If-None-Match"] = etag
    if isinstance(last_modified, str) and last_modified:
        headers["If-Modified-Since"] = last_modified


def _active_state(
    content_hash: str,
    headers: Mapping[str, str],
    status_code: int,
    prior: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "content_hash": content_hash,
        "etag": headers.get("etag", prior.get("etag") if prior else None),
        "last_modified": headers.get(
            "last-modified", prior.get("last_modified") if prior else None
        ),
        "http_status": status_code,
        "status": "active",
    }


def _page_report(
    url: str,
    status: str,
    reason: str | None,
    response: CrawlResponse | None = None,
    content_hash: str | None = None,
) -> dict[str, object]:
    return {
        "content_hash": content_hash,
        "etag": None if response is None else _normalized_headers(response.headers).get("etag"),
        "http_status": None if response is None else response.status_code,
        "last_modified": None
        if response is None
        else _normalized_headers(response.headers).get("last-modified"),
        "reason": reason,
        "status": status,
        "url": url,
    }


def _blocked_evidence(code: str, message: str) -> dict[str, object]:
    return {
        "artifact_type": "public_docs_crawler_evidence",
        "contract_version": CRAWLER_CONTRACT_VERSION,
        "status": "blocked",
        "changed_urls": [],
        "unchanged_urls": [],
        "deleted_urls": [],
        "failed_urls": [],
        "blocked_urls": [],
        "pages": {},
        "state": {},
        "summary": {"changed": 0, "unchanged": 0, "deleted": 0, "failed": 0, "blocked": 0},
        "failure": {"code": code, "message": message},
    }


def _normalized_headers(headers: Mapping[str, str]) -> dict[str, str]:
    return {str(key).lower(): str(value) for key, value in headers.items()}


def _prior_hash(prior: Mapping[str, Any] | None) -> str | None:
    value = None if prior is None else prior.get("content_hash")
    return value if isinstance(value, str) else None


def _required_hash(value: str | None) -> str:
    if value is None:
        raise ValueError("changed page must have a content hash")
    return value


def _is_html(headers: Mapping[str, str], body: bytes) -> bool:
    content_type = _normalized_headers(headers).get("content-type", "").lower()
    return "html" in content_type or body.lstrip().startswith((b"<html", b"<!doctype", b"<a "))


def _is_same_domain(url: str, policy: Mapping[str, Any]) -> bool:
    hostname = urlparse(url).hostname
    return (
        hostname in set(policy["allowed_domains"])
        if isinstance(policy["allowed_domains"], list)
        else False
    )


def _is_allowed_url(url: str, policy: Mapping[str, Any]) -> bool:
    if not _is_allowed_discovery_resource_url(url, policy):
        return False
    canonical_url = _canonical_url(url)
    parsed = urlparse(canonical_url)
    scopes = policy.get("allowed_url_scopes")
    if not isinstance(scopes, list):
        return False
    return any(
        isinstance(scope, tuple)
        and len(scope) == 3
        and parsed.hostname == scope[0]
        and (canonical_url == scope[1] or parsed.path.startswith(scope[2]))
        for scope in scopes
    )


def _is_allowed_discovery_resource_url(url: str, policy: Mapping[str, Any]) -> bool:
    if not _is_same_domain(url, policy):
        return False
    parsed = urlparse(url)
    if parsed.scheme != "https":
        return False
    path_and_query = parsed.path + (f"?{parsed.query}" if parsed.query else "")
    return not any(
        pattern in path_and_query or pattern in url for pattern in policy["deny_patterns"]
    )


def _with_governed_path_scopes(policy: dict[str, Any], seed_uri: str) -> dict[str, Any]:
    seed_host = urlparse(seed_uri).hostname
    scope_urls = [seed_uri, *policy["curated_frontier_urls"]]
    scopes: list[tuple[str, str, str]] = []
    normalized_curated_urls: list[str] = []
    for index, raw_url in enumerate(scope_urls):
        url = _canonical_url(raw_url)
        if not _is_allowed_discovery_resource_url(url, policy):
            label = "seed_uri" if index == 0 else "curated_frontier_urls"
            raise ValueError(f"{label} is outside crawl policy")
        hostname = urlparse(url).hostname
        if hostname != seed_host:
            raise ValueError("curated_frontier_urls host must match seed_uri host")
        if hostname is None:
            raise ValueError("governed crawl scope requires a hostname")
        scopes.append((hostname, url, _crawl_path_prefix(url)))
        if index > 0:
            normalized_curated_urls.append(url)
    return {
        **policy,
        "allowed_url_scopes": scopes,
        "curated_frontier_urls": normalized_curated_urls,
    }


def _crawl_path_prefix(url: str) -> str:
    path = urlparse(url).path or "/"
    if path == "/" or path.endswith("/"):
        return path
    return f"{path}/"


def _is_crawl_document_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    return not any(path.endswith(suffix) for suffix in _NON_DOCUMENT_ASSET_SUFFIXES)


def _require_allowed_url(url: str, policy: Mapping[str, Any]) -> None:
    if not _is_allowed_url(url, policy):
        raise ValueError("seed_uri is outside crawl policy")


def _canonical_url(url: str) -> str:
    without_fragment, _ = urldefrag(url)
    parsed = urlparse(without_fragment)
    path = parsed.path or "/"
    return urlunparse(
        (parsed.scheme.lower(), (parsed.hostname or "").lower(), path, "", parsed.query, "")
    )
