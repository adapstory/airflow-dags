"""Canonical legal/provenance catalog for SERP's mandatory benchmark suites.

The catalog deliberately distinguishes a runnable dataset from a benchmark
harness.  A repository's source-code license never attests the rights to
redistribute or evaluate its dataset.  Each scheduled run snapshots the live
upstream dataset and licensing evidence before an adapter is allowed to run.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256

from dags.serp_eval_contracts import MANDATORY_SERP_BENCHMARK_SUITES

BENCHMARK_CATALOG_CONTRACT_VERSION = "serp-benchmark-catalog/v2"
_READY = "ready"
_RIGHTS_ATTESTED = "attested"
_RIGHTS_UNVERIFIED = "rights-unverified"


@dataclass(frozen=True, slots=True)
class BenchmarkSuiteCatalogEntry:
    suite_id: str
    dataset_id: str
    dataset_revision: str
    dataset_source_url: str
    license_evidence_url: str
    adapter_source_url: str
    dataset_license_id: str
    distribution_rule: str
    execution_status: str
    rights_status: str
    legal_boundary: str


# Revisions are upstream content revisions, not mutable branch labels.  Entries
# without an upstream dataset license remain executable only under the explicit
# rights-unverified internal-only policy.  That policy does not claim a license
# or permit redistribution; it makes the governance boundary durable evidence.
MANDATORY_BENCHMARK_SUITE_CATALOG = (
    BenchmarkSuiteCatalogEntry(
        suite_id="APIBench",
        dataset_id="gorilla-llm/APIBench",
        dataset_revision="ac21e1892e634dfa25f8ad75f16cbdbfb0a5736d",
        dataset_source_url=(
            "https://huggingface.co/datasets/gorilla-llm/APIBench/raw/"
            "ac21e1892e634dfa25f8ad75f16cbdbfb0a5736d/README.md"
        ),
        license_evidence_url=(
            "https://huggingface.co/api/datasets/gorilla-llm/APIBench/revision/"
            "ac21e1892e634dfa25f8ad75f16cbdbfb0a5736d"
        ),
        adapter_source_url="https://github.com/adapstory/airflow-dags",
        dataset_license_id="Apache-2.0",
        distribution_rule="public-share-allowed",
        execution_status=_READY,
        rights_status=_RIGHTS_ATTESTED,
        legal_boundary="The Apache-2.0 dataset card governs the captured APIBench snapshot.",
    ),
    BenchmarkSuiteCatalogEntry(
        suite_id="ARES",
        dataset_id="stanford-futuredata/ARES",
        dataset_revision="c7c9018a755faf8347c4da415632bae1593ef104",
        dataset_source_url=(
            "https://raw.githubusercontent.com/stanford-futuredata/ARES/"
            "c7c9018a755faf8347c4da415632bae1593ef104/README.md"
        ),
        license_evidence_url=(
            "https://raw.githubusercontent.com/stanford-futuredata/ARES/"
            "c7c9018a755faf8347c4da415632bae1593ef104/LICENSE"
        ),
        adapter_source_url="https://github.com/adapstory/airflow-dags",
        dataset_license_id="Apache-2.0",
        distribution_rule="internal-only",
        execution_status=_READY,
        rights_status=_RIGHTS_ATTESTED,
        legal_boundary=(
            "ARES is a licensed synthetic-data generator; every generated run must also "
            "retain its generation manifest and model/provider policy evidence."
        ),
    ),
    BenchmarkSuiteCatalogEntry(
        suite_id="BEIR",
        dataset_id="BeIR/scifact",
        dataset_revision="b3b5335604bf5ee3c4447671af975ea25143d4f5",
        dataset_source_url=(
            "https://huggingface.co/datasets/BeIR/scifact/raw/"
            "b3b5335604bf5ee3c4447671af975ea25143d4f5/README.md"
        ),
        license_evidence_url=(
            "https://huggingface.co/api/datasets/BeIR/scifact/revision/"
            "b3b5335604bf5ee3c4447671af975ea25143d4f5"
        ),
        adapter_source_url="https://github.com/adapstory/airflow-dags",
        dataset_license_id="CC-BY-SA-4.0",
        distribution_rule="internal-only",
        execution_status=_READY,
        rights_status=_RIGHTS_ATTESTED,
        legal_boundary=(
            "SciFact corpus, queries, and qrels are retained internally with attribution."
        ),
    ),
    BenchmarkSuiteCatalogEntry(
        suite_id="CodeRAG-Bench",
        dataset_id="code-rag-bench/code-rag-bench",
        dataset_revision="f9e100ca9ed94b8f1983b356ae81966e30210cf4",
        dataset_source_url=(
            "https://raw.githubusercontent.com/code-rag-bench/code-rag-bench/"
            "f9e100ca9ed94b8f1983b356ae81966e30210cf4/README.md"
        ),
        license_evidence_url=(
            "https://api.github.com/repos/code-rag-bench/code-rag-bench/commits/"
            "f9e100ca9ed94b8f1983b356ae81966e30210cf4"
        ),
        adapter_source_url="https://github.com/adapstory/airflow-dags",
        dataset_license_id="LicenseRef-CodeRAG-Bench-Rights-Unverified",
        distribution_rule="internal-only-no-redistribution",
        execution_status=_READY,
        rights_status=_RIGHTS_UNVERIFIED,
        legal_boundary=(
            "Rights are unverified: execution is internal-only, evidence is retained, and "
            "the snapshot must never be redistributed or represented as licensed."
        ),
    ),
    BenchmarkSuiteCatalogEntry(
        suite_id="RAGBench",
        dataset_id="galileo-ai/ragbench",
        dataset_revision="97808f3e5fd16ede40bbff6c2949af8139b2eb7b",
        dataset_source_url=(
            "https://huggingface.co/datasets/galileo-ai/ragbench/raw/"
            "97808f3e5fd16ede40bbff6c2949af8139b2eb7b/README.md"
        ),
        license_evidence_url=(
            "https://huggingface.co/api/datasets/galileo-ai/ragbench/revision/"
            "97808f3e5fd16ede40bbff6c2949af8139b2eb7b"
        ),
        adapter_source_url="https://github.com/adapstory/airflow-dags",
        dataset_license_id="CC-BY-4.0",
        distribution_rule="public-share-allowed",
        execution_status=_READY,
        rights_status=_RIGHTS_ATTESTED,
        legal_boundary="RAGBench dataset card's CC-BY-4.0 terms apply to the retained snapshot.",
    ),
    BenchmarkSuiteCatalogEntry(
        suite_id="RepoQA",
        dataset_id="evalplus/repoqa",
        dataset_revision="ae876deb1365dbf5a15b0533723c8ed123eee586",
        dataset_source_url=(
            "https://raw.githubusercontent.com/evalplus/repoqa/"
            "ae876deb1365dbf5a15b0533723c8ed123eee586/README.md"
        ),
        license_evidence_url=(
            "https://raw.githubusercontent.com/evalplus/repoqa/"
            "ae876deb1365dbf5a15b0533723c8ed123eee586/LICENSE"
        ),
        adapter_source_url="https://github.com/adapstory/airflow-dags",
        dataset_license_id="Apache-2.0",
        distribution_rule="internal-only",
        execution_status=_READY,
        rights_status=_RIGHTS_ATTESTED,
        legal_boundary=(
            "The RepoQA project is Apache-2.0; source repositories selected by a run must "
            "be individually recorded in that run's dataset manifest."
        ),
    ),
    BenchmarkSuiteCatalogEntry(
        suite_id="SWE-bench Verified",
        dataset_id="SWE-bench/SWE-bench_Verified",
        dataset_revision="91aa3ed51b709be6457e12d00300a6a596d4c6a3",
        dataset_source_url=(
            "https://huggingface.co/datasets/SWE-bench/SWE-bench_Verified/raw/"
            "91aa3ed51b709be6457e12d00300a6a596d4c6a3/README.md"
        ),
        license_evidence_url=(
            "https://huggingface.co/api/datasets/SWE-bench/SWE-bench_Verified/revision/"
            "91aa3ed51b709be6457e12d00300a6a596d4c6a3"
        ),
        adapter_source_url="https://github.com/adapstory/airflow-dags",
        dataset_license_id="LicenseRef-SWE-Bench-Verified-Rights-Unverified",
        distribution_rule="internal-only-no-redistribution",
        execution_status=_READY,
        rights_status=_RIGHTS_UNVERIFIED,
        legal_boundary=(
            "Rights are unverified: the MIT harness license does not license the instances; "
            "execution is internal-only and evidence must not be redistributed."
        ),
    ),
    BenchmarkSuiteCatalogEntry(
        suite_id="cwd-benchmark-data",
        dataset_id="datadotworld/cwd-benchmark-data",
        dataset_revision="0b75eb62eaf7ea315a863cd7611ebc908149f7e0",
        dataset_source_url=(
            "https://raw.githubusercontent.com/datadotworld/cwd-benchmark-data/"
            "0b75eb62eaf7ea315a863cd7611ebc908149f7e0/README.md"
        ),
        license_evidence_url=(
            "https://raw.githubusercontent.com/datadotworld/cwd-benchmark-data/"
            "0b75eb62eaf7ea315a863cd7611ebc908149f7e0/LICENSE.txt"
        ),
        adapter_source_url="https://github.com/adapstory/airflow-dags",
        dataset_license_id="Apache-2.0",
        distribution_rule="public-share-allowed",
        execution_status=_READY,
        rights_status=_RIGHTS_ATTESTED,
        legal_boundary=(
            "The repository's Apache-2.0 license governs the captured CWD dataset snapshot."
        ),
    ),
    BenchmarkSuiteCatalogEntry(
        suite_id="rusBEIR",
        dataset_id="kaengreg/rusBEIR",
        dataset_revision="06c4607129ab801885f14ee721a4013d23795272",
        dataset_source_url=(
            "https://raw.githubusercontent.com/kaengreg/rusBEIR/"
            "06c4607129ab801885f14ee721a4013d23795272/README.md"
        ),
        license_evidence_url=(
            "https://api.github.com/repos/kaengreg/rusBEIR/commits/"
            "06c4607129ab801885f14ee721a4013d23795272"
        ),
        adapter_source_url="https://github.com/adapstory/airflow-dags",
        dataset_license_id="LicenseRef-rusBEIR-Rights-Unverified",
        distribution_rule="internal-only-no-redistribution",
        execution_status=_READY,
        rights_status=_RIGHTS_UNVERIFIED,
        legal_boundary=(
            "Rights are unverified: execution is internal-only, evidence is retained, and "
            "the upstream corpus snapshot must not be redistributed as licensed."
        ),
    ),
)


def build_live_benchmark_catalog_evidence(
    *,
    observed_at: str,
    fetch_bytes: Callable[[str], bytes],
    snapshot_bytes: Callable[[str, str, str, bytes], Mapping[str, object]] | None = None,
) -> dict[str, object]:
    """Fetch and content-address the current upstream legal evidence for every suite.

    Network failures are intentionally propagated: a stale or partial catalog cannot
    be used as legal provenance for a scheduled evaluation.
    """

    _validate_observed_at(observed_at)
    suites: list[dict[str, object]] = []
    for entry in MANDATORY_BENCHMARK_SUITE_CATALOG:
        source_payload = _fetch(entry.dataset_source_url, fetch_bytes)
        license_payload = _fetch(entry.license_evidence_url, fetch_bytes)
        suites.append(
            {
                "adapter_source_url": entry.adapter_source_url,
                "dataset_id": entry.dataset_id,
                "dataset_license_id": entry.dataset_license_id,
                "dataset_revision": entry.dataset_revision,
                "distribution_rule": entry.distribution_rule,
                "execution_status": entry.execution_status,
                "legal_boundary": entry.legal_boundary,
                "license_snapshot": _snapshot(
                    entry.suite_id,
                    "license",
                    entry.license_evidence_url,
                    license_payload,
                    snapshot_bytes,
                ),
                "source_snapshot": _snapshot(
                    entry.suite_id,
                    "source",
                    entry.dataset_source_url,
                    source_payload,
                    snapshot_bytes,
                ),
                "rights_status": entry.rights_status,
                "suite_id": entry.suite_id,
            }
        )
    return {
        "catalog_status": "ready",
        "contract_version": BENCHMARK_CATALOG_CONTRACT_VERSION,
        "observed_at": observed_at,
        "suites": suites,
    }


def _fetch(url: str, fetch_bytes: Callable[[str], bytes]) -> bytes:
    payload = fetch_bytes(url)
    if not isinstance(payload, bytes) or not payload:
        raise ValueError(f"upstream evidence fetch returned no bytes: {url}")
    return payload


def _snapshot(
    suite_id: str,
    evidence_type: str,
    url: str,
    payload: bytes,
    snapshot_bytes: Callable[[str, str, str, bytes], Mapping[str, object]] | None,
) -> dict[str, object]:
    snapshot: dict[str, object] = {
        "byte_length": len(payload),
        "sha256": "sha256:" + sha256(payload).hexdigest(),
        "url": url,
    }
    if snapshot_bytes is None:
        return snapshot
    immutable_artifact = dict(snapshot_bytes(suite_id, evidence_type, url, payload))
    _validate_immutable_artifact(immutable_artifact, payload)
    snapshot["immutable_artifact"] = immutable_artifact
    return snapshot


def _validate_immutable_artifact(artifact: Mapping[str, object], payload: bytes) -> None:
    artifact_path = artifact.get("artifactPath")
    if not isinstance(artifact_path, str) or not artifact_path.startswith("s3://"):
        raise ValueError("immutable catalog evidence must be stored at an s3:// artifact path")
    artifact_version_id = artifact.get("artifactVersionId")
    if not isinstance(artifact_version_id, str) or not artifact_version_id.strip():
        raise ValueError("immutable catalog evidence must include artifactVersionId")
    if artifact.get("objectLockMode") != "COMPLIANCE":
        raise ValueError("immutable catalog evidence must use COMPLIANCE object lock")
    if artifact.get("artifactSha256") != sha256(payload).hexdigest():
        raise ValueError("immutable catalog evidence SHA-256 does not match fetched payload")


def _validate_observed_at(value: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("observed_at must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError("observed_at must include timezone")


if (
    tuple(entry.suite_id for entry in MANDATORY_BENCHMARK_SUITE_CATALOG)
    != MANDATORY_SERP_BENCHMARK_SUITES
):
    raise RuntimeError("benchmark catalog must use the mandatory suite order")
