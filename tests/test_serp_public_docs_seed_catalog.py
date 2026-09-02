from __future__ import annotations

from urllib.parse import urlparse

import pytest

from dags.serp_public_docs_seed_catalog import (
    GOLDEN_RETRIEVAL_SCOPE_BY_SEED_ID,
    GOVERNED_PUBLIC_DOCS_SOURCES,
    PUBLIC_DOCS_SOURCE_REGISTRY_VERSION,
    QUARANTINED_PUBLIC_DOCS_CANDIDATES,
    governed_public_docs_sources,
)

pytestmark = pytest.mark.unit


def test_governed_registry_expands_sources_and_connector_coverage() -> None:
    sources = governed_public_docs_sources()

    assert PUBLIC_DOCS_SOURCE_REGISTRY_VERSION == "public-docs-source-registry/v2"
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


def test_active_sources_have_ownership_coverage_and_freshness_authority() -> None:
    required_capabilities = {
        "concepts",
        "setup",
        "reference",
        "security",
        "operations",
        "troubleshooting",
        "migrations",
        "release-notes",
    }

    for source in governed_public_docs_sources():
        assert source["knowledge_scope"] == "public"
        assert source["authority_kind"] == "official"
        assert source["original_language"] == "en"
        assert source["version_scope"] in {"current", "supported-version"}
        assert source["source_steward"] == "adapstory-platform-architecture"
        assert source["platform_owner"] == "adapstory-knowledge-platform"
        assert source["security_owner"] == "adapstory-security"
        assert source["privacy_owner"] == "adapstory-privacy"
        assert source["legal_owner"] == "adapstory-legal"
        assert set(source["capability_coverage"]) == required_capabilities
        if source["priority"] == "P0":
            assert source["freshness_warning_hours"] == 24
            assert source["freshness_hard_limit_hours"] == 72
        else:
            assert source["freshness_warning_hours"] == 72
            assert source["freshness_hard_limit_hours"] == 168


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


def test_experience_golden_domains_have_canonical_retrieval_scope_metadata() -> None:
    expected = {
        "apache-airflow-docs": "/apache/airflow",
        "argo-cd-docs": "/argoproj/argo-cd",
        "junit-jupiter-docs": "/junit-team/junit5",
        "k3s-docs": "/k3s-io/docs",
        "keycloak-docs": "/keycloak/keycloak",
        "langfuse-docs": "/langfuse/langfuse",
        "nextjs-docs": "/vercel/next.js",
        "ollama-docs": "/ollama/ollama",
        "opentelemetry-docs": "/open-telemetry/opentelemetry-collector",
        "qdrant-docs": "/qdrant/qdrant-client",
        "ragas-docs": "/explodinggradients/ragas",
        "spring-boot-docs": "/spring-projects/spring-boot",
    }

    assert {
        seed_id: metadata["library_id"]
        for seed_id, metadata in GOLDEN_RETRIEVAL_SCOPE_BY_SEED_ID.items()
    } == expected

    sources = {source["seed_id"]: source for source in governed_public_docs_sources()}
    for seed_id, library_id in expected.items():
        source = sources[seed_id]
        assert source["library_id"] == library_id
        assert source["library_name"]
        assert source["product_versions"]

    junit = sources["junit-jupiter-docs"]
    assert junit["docs_url"] == "https://docs.junit.org/5.11.0/user-guide/"
    assert junit["crawl_max_pages"] == 1


def test_trivy_docs_use_live_canonical_entrypoint() -> None:
    sources = {source["seed_id"]: source for source in governed_public_docs_sources()}

    assert sources["trivy-docs"]["docs_url"] == "https://trivy.dev/docs/"
