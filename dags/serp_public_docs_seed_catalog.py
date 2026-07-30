from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from adapstory_serp_pipeline.contracts.source_policy import (
    PUBLIC_DOCS_CAPABILITY_COVERAGE,
)

PUBLIC_DOCS_NIGHTLY_SOURCE_CATALOG_PATH = "docs/reports/serp/public-docs-seed-catalog-2026-07-08.md"
STACK_INVENTORY_SOURCE_PATH = "docs/reports/serp/stack-inventory-2026-07-02.md"
PUBLIC_DOCS_SOURCE_REGISTRY_VERSION = "public-docs-source-registry/v2"

WEBSITE_GIT_RELEASE_NOTES_INGEST_MODES = ("website", "git", "release-notes")
OPENAPI_INGEST_MODES = ("openapi",)


def _source(
    *,
    component: str,
    docs_url: str,
    repo_url: str,
    releases_url: str,
    seed_id: str,
    catalog_docs_url: str | None = None,
    frontier_urls: Sequence[str] = (),
    source_type: str = "website",
    suggested_ingest_modes: Sequence[str] = WEBSITE_GIT_RELEASE_NOTES_INGEST_MODES,
    version: str | None = None,
    priority: str = "P0",
) -> Mapping[str, Any]:
    freshness_warning_hours = 24 if priority == "P0" else 72
    freshness_hard_limit_hours = 72 if priority == "P0" else 168
    source: dict[str, Any] = {
        "authority_kind": "official",
        "capability_coverage": PUBLIC_DOCS_CAPABILITY_COVERAGE,
        "component": component,
        "docs_url": docs_url,
        "freshness_hard_limit_hours": freshness_hard_limit_hours,
        "freshness_warning_hours": freshness_warning_hours,
        "governance_state": "active",
        "knowledge_scope": "public",
        "legal_owner": "adapstory-legal",
        "license_review": "reviewed-public-docs",
        "official_source_evidence": "vendor-docs-or-owned-repository",
        "original_language": "en",
        "platform_owner": "adapstory-knowledge-platform",
        "priority": priority,
        "privacy_owner": "adapstory-privacy",
        "refresh_cadence": "daily",
        "releases_url": releases_url,
        "repo_url": repo_url,
        "robots_cache_max_hours": 24,
        "security_owner": "adapstory-security",
        "seed_id": seed_id,
        "source_steward": "adapstory-platform-architecture",
        "source_type": source_type,
        "suggested_ingest_modes": tuple(suggested_ingest_modes),
        "version_scope": "supported-version" if version is not None else "current",
    }
    if catalog_docs_url is not None:
        source["catalog_docs_url"] = catalog_docs_url
    if frontier_urls:
        source["frontier_urls"] = tuple(frontier_urls)
    if version is not None:
        source["version"] = version
    return source


GOVERNED_PUBLIC_DOCS_SOURCES: tuple[Mapping[str, Any], ...] = (
    _source(
        component="Proxmox VE",
        docs_url="https://pve.proxmox.com/pve-docs/",
        repo_url="https://git.proxmox.com/",
        releases_url="https://www.proxmox.com/en/downloads",
        seed_id="proxmox-ve-docs",
        suggested_ingest_modes=("website", "release-notes"),
    ),
    _source(
        component="K3s",
        docs_url="https://docs.k3s.io/",
        repo_url="https://github.com/k3s-io/k3s",
        releases_url="https://github.com/k3s-io/k3s/releases",
        seed_id="k3s-docs",
        frontier_urls=(
            "https://docs.k3s.io/quick-start",
            "https://docs.k3s.io/installation/requirements",
        ),
        version="v1.34.3+k3s1",
    ),
    _source(
        component="Kubernetes",
        docs_url="https://kubernetes.io/docs/",
        repo_url="https://github.com/kubernetes/kubernetes",
        releases_url="https://kubernetes.io/releases/",
        seed_id="kubernetes-docs",
        version="v1.34.3",
    ),
    _source(
        component="Kubernetes OpenAPI",
        docs_url=(
            "https://raw.githubusercontent.com/kubernetes/kubernetes/v1.34.3/"
            "api/openapi-spec/v3/api__v1_openapi.json"
        ),
        repo_url="https://github.com/kubernetes/kubernetes",
        releases_url="https://github.com/kubernetes/kubernetes/releases",
        seed_id="kubernetes-openapi-docs",
        source_type="openapi",
        suggested_ingest_modes=OPENAPI_INGEST_MODES,
        version="v1.34.3",
    ),
    _source(
        component="Helm",
        docs_url="https://helm.sh/docs/",
        repo_url="https://github.com/helm/helm",
        releases_url="https://github.com/helm/helm/releases",
        seed_id="helm-docs",
    ),
    _source(
        component="Kustomize",
        docs_url="https://kustomize.io/",
        repo_url="https://github.com/kubernetes-sigs/kustomize",
        releases_url="https://github.com/kubernetes-sigs/kustomize/releases",
        seed_id="kustomize-docs",
    ),
    _source(
        component="Argo CD",
        docs_url="https://argo-cd.readthedocs.io/en/stable/",
        repo_url="https://github.com/argoproj/argo-cd",
        releases_url="https://github.com/argoproj/argo-cd/releases",
        seed_id="argo-cd-docs",
    ),
    _source(
        component="Apache Airflow",
        docs_url="https://airflow.apache.org/docs/",
        repo_url="https://github.com/apache/airflow",
        releases_url="https://github.com/apache/airflow/releases",
        seed_id="apache-airflow-docs",
    ),
    _source(
        component="OpenSearch",
        docs_url="https://docs.opensearch.org/latest/",
        repo_url="https://github.com/opensearch-project/OpenSearch",
        releases_url="https://github.com/opensearch-project/OpenSearch/releases",
        seed_id="opensearch-docs",
    ),
    _source(
        component="Qdrant",
        docs_url="https://qdrant.tech/documentation/",
        repo_url="https://github.com/qdrant/qdrant",
        releases_url="https://github.com/qdrant/qdrant/releases",
        seed_id="qdrant-docs",
    ),
    _source(
        component="Neo4j",
        docs_url="https://neo4j.com/docs/",
        repo_url="https://github.com/neo4j/neo4j",
        releases_url="https://github.com/neo4j/neo4j/releases",
        seed_id="neo4j-docs",
    ),
    _source(
        component="PostgreSQL",
        docs_url="https://www.postgresql.org/docs/16/",
        repo_url="https://github.com/postgres/postgres",
        releases_url="https://www.postgresql.org/docs/release/",
        seed_id="postgresql-reference-docs",
        catalog_docs_url="https://www.postgresql.org/docs/",
        frontier_urls=(
            "https://www.postgresql.org/docs/16/tutorial.html",
            "https://www.postgresql.org/docs/16/sql.html",
            "https://www.postgresql.org/docs/16/index.html",
        ),
        version="16.1.0",
    ),
    _source(
        component="Redis",
        docs_url="https://redis.io/docs/latest/",
        repo_url="https://github.com/redis/redis",
        releases_url="https://github.com/redis/redis/releases",
        seed_id="redis-docs",
    ),
    _source(
        component="Apache Kafka",
        docs_url="https://kafka.apache.org/43/",
        repo_url="https://github.com/apache/kafka",
        releases_url="https://github.com/apache/kafka/releases",
        seed_id="apache-kafka-docs",
        version="4.3",
    ),
    _source(
        component="MinIO",
        docs_url="https://docs.min.io/",
        repo_url="https://github.com/minio/minio",
        releases_url="https://github.com/minio/minio/releases",
        seed_id="minio-docs",
    ),
    _source(
        component="Vault",
        docs_url="https://developer.hashicorp.com/vault/docs",
        repo_url="https://github.com/hashicorp/vault",
        releases_url="https://github.com/hashicorp/vault/releases",
        seed_id="vault-docs",
    ),
    _source(
        component="Keycloak",
        docs_url="https://www.keycloak.org/documentation",
        repo_url="https://github.com/keycloak/keycloak",
        releases_url="https://github.com/keycloak/keycloak/releases",
        seed_id="keycloak-docs",
    ),
    _source(
        component="Jenkins",
        docs_url="https://www.jenkins.io/doc/",
        repo_url="https://github.com/jenkinsci/jenkins",
        releases_url="https://github.com/jenkinsci/jenkins/releases",
        seed_id="jenkins-docs",
    ),
    _source(
        component="Harbor",
        docs_url="https://goharbor.io/docs/",
        repo_url="https://github.com/goharbor/harbor",
        releases_url="https://github.com/goharbor/harbor/releases",
        seed_id="harbor-docs",
    ),
    _source(
        component="cert-manager",
        docs_url="https://cert-manager.io/docs/",
        repo_url="https://github.com/cert-manager/cert-manager",
        releases_url="https://github.com/cert-manager/cert-manager/releases",
        seed_id="cert-manager-docs",
    ),
    _source(
        component="External Secrets Operator",
        docs_url="https://external-secrets.io/latest/",
        repo_url="https://github.com/external-secrets/external-secrets",
        releases_url="https://github.com/external-secrets/external-secrets/releases",
        seed_id="external-secrets-operator-docs",
    ),
    _source(
        component="Cilium",
        docs_url="https://docs.cilium.io/en/stable/",
        repo_url="https://github.com/cilium/cilium",
        releases_url="https://github.com/cilium/cilium/releases",
        seed_id="cilium-docs",
    ),
    _source(
        component="Kyverno",
        docs_url="https://kyverno.io/docs/",
        repo_url="https://github.com/kyverno/kyverno",
        releases_url="https://github.com/kyverno/kyverno/releases",
        seed_id="kyverno-docs",
    ),
    _source(
        component="OpenEBS",
        docs_url="https://openebs.io/docs/",
        repo_url="https://github.com/openebs/openebs",
        releases_url="https://github.com/openebs/openebs/releases",
        seed_id="openebs-docs",
    ),
    _source(
        component="Traefik Proxy",
        docs_url="https://doc.traefik.io/traefik/",
        repo_url="https://github.com/traefik/traefik",
        releases_url="https://github.com/traefik/traefik/releases",
        seed_id="traefik-proxy-docs",
    ),
    _source(
        component="NVIDIA GPU Operator",
        docs_url="https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/latest/",
        repo_url="https://github.com/NVIDIA/gpu-operator",
        releases_url="https://github.com/NVIDIA/gpu-operator/releases",
        seed_id="nvidia-gpu-operator-docs",
    ),
    _source(
        component="Grafana",
        docs_url="https://grafana.com/docs/grafana/latest/",
        repo_url="https://github.com/grafana/grafana",
        releases_url="https://github.com/grafana/grafana/releases",
        seed_id="grafana-docs",
        priority="P1",
    ),
    _source(
        component="Loki",
        docs_url="https://grafana.com/docs/loki/latest/",
        repo_url="https://github.com/grafana/loki",
        releases_url="https://github.com/grafana/loki/releases",
        seed_id="loki-docs",
        priority="P1",
    ),
    _source(
        component="Tempo",
        docs_url="https://grafana.com/docs/tempo/latest/",
        repo_url="https://github.com/grafana/tempo",
        releases_url="https://github.com/grafana/tempo/releases",
        seed_id="tempo-docs",
        priority="P1",
    ),
    _source(
        component="Pyroscope",
        docs_url="https://grafana.com/docs/pyroscope/latest/",
        repo_url="https://github.com/grafana/pyroscope",
        releases_url="https://github.com/grafana/pyroscope/releases",
        seed_id="pyroscope-docs",
        priority="P1",
    ),
    _source(
        component="OpenTelemetry",
        docs_url="https://opentelemetry.io/docs/",
        repo_url="https://github.com/open-telemetry/opentelemetry.io",
        releases_url="https://github.com/open-telemetry/opentelemetry.io/releases",
        seed_id="opentelemetry-docs",
        priority="P1",
    ),
    _source(
        component="ClickHouse",
        docs_url="https://clickhouse.com/docs/",
        repo_url="https://github.com/ClickHouse/ClickHouse",
        releases_url="https://github.com/ClickHouse/ClickHouse/releases",
        seed_id="clickhouse-docs",
        priority="P1",
    ),
    _source(
        component="Dify",
        docs_url="https://docs.dify.ai/en/introduction",
        repo_url="https://github.com/langgenius/dify",
        releases_url="https://github.com/langgenius/dify/releases",
        seed_id="dify-docs",
        priority="P1",
    ),
    _source(
        component="n8n",
        docs_url="https://docs.n8n.io/",
        repo_url="https://github.com/n8n-io/n8n",
        releases_url="https://github.com/n8n-io/n8n/releases",
        seed_id="n8n-docs",
        priority="P1",
    ),
    _source(
        component="Ollama",
        docs_url="https://docs.ollama.com/",
        repo_url="https://github.com/ollama/ollama",
        releases_url="https://github.com/ollama/ollama/releases",
        seed_id="ollama-docs",
        priority="P1",
    ),
    _source(
        component="SonarQube Server",
        docs_url="https://docs.sonarsource.com/sonarqube-server/",
        repo_url="https://github.com/SonarSource/sonarqube",
        releases_url="https://github.com/SonarSource/sonarqube/releases",
        seed_id="sonarqube-server-docs",
        priority="P1",
    ),
    _source(
        component="Trivy",
        docs_url="https://trivy.dev/latest/",
        repo_url="https://github.com/aquasecurity/trivy",
        releases_url="https://github.com/aquasecurity/trivy/releases",
        seed_id="trivy-docs",
        priority="P1",
    ),
    _source(
        component="Nextcloud",
        docs_url="https://docs.nextcloud.com/",
        repo_url="https://github.com/nextcloud/server",
        releases_url="https://github.com/nextcloud/server/releases",
        seed_id="nextcloud-docs",
        priority="P1",
    ),
    _source(
        component="Spring Boot",
        docs_url="https://docs.spring.io/spring-boot/",
        repo_url="https://github.com/spring-projects/spring-boot",
        releases_url="https://github.com/spring-projects/spring-boot/releases",
        seed_id="spring-boot-docs",
        priority="P1",
    ),
    _source(
        component="Spring Security",
        docs_url="https://docs.spring.io/spring-security/reference/",
        repo_url="https://github.com/spring-projects/spring-security",
        releases_url="https://github.com/spring-projects/spring-security/releases",
        seed_id="spring-security-docs",
        priority="P1",
    ),
    _source(
        component="React",
        docs_url="https://react.dev/",
        repo_url="https://github.com/facebook/react",
        releases_url="https://github.com/facebook/react/releases",
        seed_id="react-docs",
        priority="P1",
    ),
    _source(
        component="Vite",
        docs_url="https://vite.dev/guide/",
        repo_url="https://github.com/vitejs/vite",
        releases_url="https://github.com/vitejs/vite/releases",
        seed_id="vite-docs",
        priority="P1",
    ),
    _source(
        component="Playwright",
        docs_url="https://playwright.dev/docs/intro",
        repo_url="https://github.com/microsoft/playwright",
        releases_url="https://github.com/microsoft/playwright/releases",
        seed_id="playwright-docs",
        priority="P1",
    ),
    _source(
        component="FastAPI",
        docs_url="https://fastapi.tiangolo.com/",
        repo_url="https://github.com/fastapi/fastapi",
        releases_url="https://github.com/fastapi/fastapi/releases",
        seed_id="fastapi-docs",
        priority="P1",
    ),
    _source(
        component="Pydantic",
        docs_url="https://docs.pydantic.dev/latest/",
        repo_url="https://github.com/pydantic/pydantic",
        releases_url="https://github.com/pydantic/pydantic/releases",
        seed_id="pydantic-docs",
        priority="P1",
    ),
    _source(
        component="Proxmox VE Administration Guide",
        docs_url="https://pve.proxmox.com/pve-docs/pve-admin-guide.pdf",
        repo_url="https://git.proxmox.com/",
        releases_url="https://www.proxmox.com/en/downloads",
        seed_id="proxmox-ve-admin-guide-pdf",
        source_type="pdf",
        suggested_ingest_modes=("pdf",),
        priority="P1",
    ),
    _source(
        component="OpenTelemetry Docs Repository",
        docs_url=(
            "git+https://github.com/open-telemetry/opentelemetry.io" "?ref=main&path=README.md"
        ),
        catalog_docs_url="https://opentelemetry.io/docs/",
        repo_url="https://github.com/open-telemetry/opentelemetry.io",
        releases_url="https://github.com/open-telemetry/opentelemetry.io/releases",
        seed_id="opentelemetry-docs-git-readme",
        source_type="git",
        suggested_ingest_modes=("git",),
        priority="P1",
    ),
)

QUARANTINED_PUBLIC_DOCS_CANDIDATES: tuple[Mapping[str, Any], ...] = (
    {
        "component": "Nexus Repository",
        "docs_url": "https://help.sonatype.com/en/sonatype-nexus-repository.html",
        "governance_state": "quarantined",
        "quarantine_reason": "repository-ownership-and-release-surface-incomplete",
        "seed_id": "nexus-repository-docs",
    },
    {
        "component": "LangGraph",
        "docs_url": "https://docs.langchain.com/oss/python/langgraph/",
        "governance_state": "quarantined",
        "quarantine_reason": "canonical-docs-migration-needs-review",
        "seed_id": "langgraph-docs",
    },
)


def governed_public_docs_sources() -> Sequence[Mapping[str, Any]]:
    return tuple(dict(source) for source in GOVERNED_PUBLIC_DOCS_SOURCES)
