from __future__ import annotations

from urllib.parse import urlparse

from dags.serp_public_docs_seed_catalog import (
    GOVERNED_PUBLIC_DOCS_SOURCES,
    PUBLIC_DOCS_SOURCE_REGISTRY_VERSION,
    QUARANTINED_PUBLIC_DOCS_CANDIDATES,
    governed_public_docs_sources,
)


def test_governed_registry_expands_sources_and_connector_coverage() -> None:
    sources = governed_public_docs_sources()

    assert PUBLIC_DOCS_SOURCE_REGISTRY_VERSION == "public-docs-source-registry/v1"
    assert len(sources) >= 45
    assert len(GOVERNED_PUBLIC_DOCS_SOURCES) == len(sources)
    assert {source["source_type"] for source in sources} == {
        "git",
        "openapi",
        "pdf",
        "website",
    }
    assert sum(source["priority"] == "P0" for source in sources) == 26
    assert sum(source["priority"] == "P1" for source in sources) >= 19


def test_active_sources_are_official_policy_complete_and_unique() -> None:
    sources = governed_public_docs_sources()
    seed_ids = [source["seed_id"] for source in sources]
    canonical_uris = [source["docs_url"] for source in sources]

    assert len(seed_ids) == len(set(seed_ids))
    assert len(canonical_uris) == len(set(canonical_uris))
    for source in sources:
        assert source["governance_state"] == "active"
        assert source["official_source_evidence"] == "vendor-docs-or-owned-repository"
        assert source["license_review"] == "reviewed-public-docs"
        assert source["robots_cache_max_hours"] == 24
        assert source["refresh_cadence"] == "daily"
        parsed = urlparse(str(source["docs_url"]))
        assert parsed.scheme in {"https", "git+https"}
        assert parsed.hostname


def test_uncertain_candidates_remain_quarantined_and_non_executable() -> None:
    active_ids = {source["seed_id"] for source in governed_public_docs_sources()}

    assert QUARANTINED_PUBLIC_DOCS_CANDIDATES
    assert all(
        candidate["governance_state"] == "quarantined"
        for candidate in QUARANTINED_PUBLIC_DOCS_CANDIDATES
    )
    assert active_ids.isdisjoint(
        candidate["seed_id"] for candidate in QUARANTINED_PUBLIC_DOCS_CANDIDATES
    )
