from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

PUBLIC_DOCS_NIGHTLY_SOURCE_CATALOG_PATH = (
    "tmp/serp-public-docs-nightly-source-catalog-2026-07-08.md"
)
STACK_INVENTORY_SOURCE_PATH = "tmp/stack-inventory-2026-07-02.md"

P0_PUBLIC_DOCS_SOURCES: tuple[Mapping[str, Any], ...] = (
    {
        "component": "Proxmox VE",
        "docs_url": "https://pve.proxmox.com/pve-docs/",
        "priority": "P0",
        "seed_id": "proxmox-ve-docs",
    },
    {
        "component": "K3s",
        "docs_url": "https://docs.k3s.io/",
        "frontier_urls": (
            "https://docs.k3s.io/quick-start",
            "https://docs.k3s.io/installation/requirements",
        ),
        "priority": "P0",
        "seed_id": "k3s-docs",
        "version": "v1.34.3+k3s1",
    },
    {
        "component": "Kubernetes",
        "docs_url": "https://kubernetes.io/docs/",
        "priority": "P0",
        "seed_id": "kubernetes-docs",
        "version": "v1.34.3",
    },
    {
        "component": "Kubernetes OpenAPI",
        "docs_url": (
            "https://raw.githubusercontent.com/kubernetes/kubernetes/v1.34.3/"
            "api/openapi-spec/v3/api__v1_openapi.json"
        ),
        "priority": "P0",
        "seed_id": "kubernetes-openapi-docs",
        "source_type": "openapi",
        "version": "v1.34.3",
    },
    {
        "component": "Helm",
        "docs_url": "https://helm.sh/docs/",
        "priority": "P0",
        "seed_id": "helm-docs",
    },
    {
        "component": "Kustomize",
        "docs_url": "https://kustomize.io/",
        "priority": "P0",
        "seed_id": "kustomize-docs",
    },
    {
        "component": "Argo CD",
        "docs_url": "https://argo-cd.readthedocs.io/en/stable/",
        "priority": "P0",
        "seed_id": "argo-cd-docs",
    },
    {
        "component": "Apache Airflow",
        "docs_url": "https://airflow.apache.org/docs/",
        "priority": "P0",
        "seed_id": "apache-airflow-docs",
    },
    {
        "component": "OpenSearch",
        "docs_url": "https://docs.opensearch.org/latest/",
        "priority": "P0",
        "seed_id": "opensearch-docs",
    },
    {
        "component": "Qdrant",
        "docs_url": (
            "https://raw.githubusercontent.com/qdrant/landing_page/master/"
            "qdrant-landing/content/documentation/_index.md"
        ),
        "priority": "P0",
        "seed_id": "qdrant-docs",
    },
    {
        "component": "Neo4j",
        "docs_url": "https://raw.githubusercontent.com/neo4j/docs-operations/dev/README.adoc",
        "priority": "P0",
        "seed_id": "neo4j-docs",
    },
    {
        "component": "PostgreSQL",
        "docs_url": "https://www.postgresql.org/docs/16/",
        "frontier_urls": (
            "https://www.postgresql.org/docs/16/tutorial.html",
            "https://www.postgresql.org/docs/16/sql.html",
            "https://www.postgresql.org/docs/16/index.html",
        ),
        "priority": "P0",
        "seed_id": "postgresql-reference-docs",
        "version": "16.1.0",
    },
    {
        "component": "Redis",
        "docs_url": "https://raw.githubusercontent.com/redis/redis/unstable/README.md",
        "priority": "P0",
        "seed_id": "redis-docs",
    },
    {
        "component": "Apache Kafka",
        "docs_url": "https://kafka.apache.org/documentation/",
        "priority": "P0",
        "seed_id": "apache-kafka-docs",
    },
    {
        "component": "MinIO",
        "docs_url": "https://docs.min.io/",
        "priority": "P0",
        "seed_id": "minio-docs",
    },
    {
        "component": "Vault",
        "docs_url": "https://developer.hashicorp.com/vault/docs",
        "priority": "P0",
        "seed_id": "vault-docs",
    },
    {
        "component": "Keycloak",
        "docs_url": "https://www.keycloak.org/documentation",
        "priority": "P0",
        "seed_id": "keycloak-docs",
    },
    {
        "component": "Jenkins",
        "docs_url": (
            "https://raw.githubusercontent.com/jenkins-infra/docs.jenkins.io/main/"
            "docs/user-docs/modules/ROOT/pages/index.adoc"
        ),
        "priority": "P0",
        "seed_id": "jenkins-docs",
    },
    {
        "component": "Harbor",
        "docs_url": "https://raw.githubusercontent.com/goharbor/website/main/docs/_index.md",
        "priority": "P0",
        "seed_id": "harbor-docs",
    },
    {
        "component": "cert-manager",
        "docs_url": (
            "https://raw.githubusercontent.com/cert-manager/website/master/"
            "content/docs/installation/README.md"
        ),
        "priority": "P0",
        "seed_id": "cert-manager-docs",
    },
    {
        "component": "External Secrets Operator",
        "docs_url": "https://external-secrets.io/latest/",
        "priority": "P0",
        "seed_id": "external-secrets-operator-docs",
    },
    {
        "component": "Cilium",
        "docs_url": (
            "https://raw.githubusercontent.com/cilium/cilium/master/"
            "Documentation/gettingstarted/k8s-install-default.rst"
        ),
        "priority": "P0",
        "seed_id": "cilium-docs",
    },
    {
        "component": "Kyverno",
        "docs_url": (
            "https://raw.githubusercontent.com/kyverno/website/main/"
            "src/content/docs/docs/installation/installation.mdx"
        ),
        "priority": "P0",
        "seed_id": "kyverno-docs",
    },
    {
        "component": "OpenEBS",
        "docs_url": (
            "https://raw.githubusercontent.com/openebs/website/main/"
            "docs/main/quickstart-guide/installation.md"
        ),
        "priority": "P0",
        "seed_id": "openebs-docs",
    },
    {
        "component": "Traefik Proxy",
        "docs_url": "https://doc.traefik.io/traefik/",
        "priority": "P0",
        "seed_id": "traefik-proxy-docs",
    },
    {
        "component": "NVIDIA GPU Operator",
        "docs_url": "https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/latest/",
        "priority": "P0",
        "seed_id": "nvidia-gpu-operator-docs",
    },
)


def p0_public_docs_sources() -> Sequence[Mapping[str, Any]]:
    return tuple(dict(source) for source in P0_PUBLIC_DOCS_SOURCES)
