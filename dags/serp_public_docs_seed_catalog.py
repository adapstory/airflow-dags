from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

PUBLIC_DOCS_NIGHTLY_SOURCE_CATALOG_PATH = (
    "tmp/serp-public-docs-nightly-source-catalog-2026-07-08.md"
)
STACK_INVENTORY_SOURCE_PATH = "tmp/stack-inventory-2026-07-02.md"

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
) -> Mapping[str, Any]:
    source: dict[str, Any] = {
        "component": component,
        "docs_url": docs_url,
        "priority": "P0",
        "releases_url": releases_url,
        "repo_url": repo_url,
        "seed_id": seed_id,
        "source_type": source_type,
        "suggested_ingest_modes": tuple(suggested_ingest_modes),
    }
    if catalog_docs_url is not None:
        source["catalog_docs_url"] = catalog_docs_url
    if frontier_urls:
        source["frontier_urls"] = tuple(frontier_urls)
    if version is not None:
        source["version"] = version
    return source


P0_PUBLIC_DOCS_SOURCES: tuple[Mapping[str, Any], ...] = (
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
)


def p0_public_docs_sources() -> Sequence[Mapping[str, Any]]:
    return tuple(dict(source) for source in P0_PUBLIC_DOCS_SOURCES)
