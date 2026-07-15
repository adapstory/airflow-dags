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

BENCHMARK_CATALOG_CONTRACT_VERSION = "serp-benchmark-catalog/v3"
_READY = "ready"
_RIGHTS_ATTESTED = "attested"
_RIGHTS_UNVERIFIED = "rights-unverified"


@dataclass(frozen=True, slots=True)
class BenchmarkSuiteCatalogEntry:
    suite_id: str
    dataset_id: str
    dataset_revision: str
    dataset_source_url: str
    dataset_artifact_source_id: str
    dataset_artifact_url: str
    supplemental_dataset_artifacts: tuple[tuple[str, str], ...]
    license_evidence_url: str
    adapter_source_url: str
    dataset_license_id: str
    distribution_rule: str
    rights_status: str
    legal_boundary: str


# Revisions are upstream content revisions, not mutable branch labels.  Entries
# without an upstream dataset license remain executable only under the explicit
# rights-unverified internal-only policy.  That policy does not claim a license
# or permit redistribution; it makes the governance boundary durable evidence.
#
# Every entry is materialized through a native adapter.  ``dataset_artifact_url``
# is the primary dataset object; ``supplemental_dataset_artifacts`` are required
# first-class inputs for suites whose corpus, queries, and qrels are published
# separately.  A suite is ready only after every object is WORM-snapshotted and
# the native adapter emits deterministic case evidence from those snapshots.
MANDATORY_BENCHMARK_SUITE_CATALOG = (
    BenchmarkSuiteCatalogEntry(
        suite_id="APIBench",
        dataset_id="gorilla-llm/APIBench",
        dataset_revision="ac21e1892e634dfa25f8ad75f16cbdbfb0a5736d",
        dataset_source_url=(
            "https://huggingface.co/datasets/gorilla-llm/APIBench/raw/"
            "ac21e1892e634dfa25f8ad75f16cbdbfb0a5736d/README.md"
        ),
        dataset_artifact_source_id="dataset",
        dataset_artifact_url=(
            "https://huggingface.co/datasets/gorilla-llm/APIBench/resolve/"
            "ac21e1892e634dfa25f8ad75f16cbdbfb0a5736d/huggingface_eval.json"
        ),
        supplemental_dataset_artifacts=(),
        license_evidence_url=(
            "https://huggingface.co/api/datasets/gorilla-llm/APIBench/revision/"
            "ac21e1892e634dfa25f8ad75f16cbdbfb0a5736d"
        ),
        adapter_source_url="https://github.com/adapstory/airflow-dags",
        dataset_license_id="Apache-2.0",
        distribution_rule="public-share-allowed",
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
        dataset_artifact_source_id="dataset",
        dataset_artifact_url=(
            "https://api.github.com/repos/stanford-futuredata/ARES/tarball/"
            "c7c9018a755faf8347c4da415632bae1593ef104"
        ),
        supplemental_dataset_artifacts=(),
        license_evidence_url=(
            "https://raw.githubusercontent.com/stanford-futuredata/ARES/"
            "c7c9018a755faf8347c4da415632bae1593ef104/LICENSE"
        ),
        adapter_source_url="https://github.com/adapstory/airflow-dags",
        dataset_license_id="Apache-2.0",
        distribution_rule="internal-only",
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
        dataset_artifact_source_id="dataset",
        dataset_artifact_url=(
            "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/scifact.zip"
        ),
        supplemental_dataset_artifacts=(),
        license_evidence_url=(
            "https://huggingface.co/api/datasets/BeIR/scifact/revision/"
            "b3b5335604bf5ee3c4447671af975ea25143d4f5"
        ),
        adapter_source_url="https://github.com/adapstory/airflow-dags",
        dataset_license_id="CC-BY-SA-4.0",
        distribution_rule="internal-only",
        rights_status=_RIGHTS_ATTESTED,
        legal_boundary=(
            "SciFact corpus, queries, and qrels are retained internally with attribution."
        ),
    ),
    BenchmarkSuiteCatalogEntry(
        suite_id="CodeRAG-Bench",
        dataset_id="code-rag-bench/ds1000",
        dataset_revision="7a5933733e549d11b75b74d3eb52bb056ffd986c",
        dataset_source_url=(
            "https://huggingface.co/datasets/code-rag-bench/ds1000/raw/"
            "7a5933733e549d11b75b74d3eb52bb056ffd986c/README.md"
        ),
        dataset_artifact_source_id="dataset",
        dataset_artifact_url=(
            "https://huggingface.co/datasets/code-rag-bench/ds1000/resolve/"
            "7a5933733e549d11b75b74d3eb52bb056ffd986c/ds1000.json"
        ),
        supplemental_dataset_artifacts=(),
        license_evidence_url=(
            "https://huggingface.co/api/datasets/code-rag-bench/ds1000/revision/"
            "7a5933733e549d11b75b74d3eb52bb056ffd986c"
        ),
        adapter_source_url="https://github.com/adapstory/airflow-dags",
        dataset_license_id="LicenseRef-CodeRAG-Bench-Rights-Unverified",
        distribution_rule="internal-only-no-redistribution",
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
        dataset_artifact_source_id="dataset",
        dataset_artifact_url=(
            "https://huggingface.co/datasets/galileo-ai/ragbench/resolve/"
            "97808f3e5fd16ede40bbff6c2949af8139b2eb7b/"
            "covidqa/test-00000-of-00001.parquet"
        ),
        supplemental_dataset_artifacts=(),
        license_evidence_url=(
            "https://huggingface.co/api/datasets/galileo-ai/ragbench/revision/"
            "97808f3e5fd16ede40bbff6c2949af8139b2eb7b"
        ),
        adapter_source_url="https://github.com/adapstory/airflow-dags",
        dataset_license_id="CC-BY-4.0",
        distribution_rule="public-share-allowed",
        rights_status=_RIGHTS_ATTESTED,
        legal_boundary="RAGBench dataset card's CC-BY-4.0 terms apply to the retained snapshot.",
    ),
    BenchmarkSuiteCatalogEntry(
        suite_id="RepoQA",
        dataset_id="evalplus/repoqa_release",
        dataset_revision="ae876deb1365dbf5a15b0533723c8ed123eee586",
        dataset_source_url=(
            "https://raw.githubusercontent.com/evalplus/repoqa/"
            "ae876deb1365dbf5a15b0533723c8ed123eee586/README.md"
        ),
        dataset_artifact_source_id="dataset",
        dataset_artifact_url=(
            "https://github.com/evalplus/repoqa_release/releases/download/2024-06-23/"
            "repoqa-2024-06-23.json.gz"
        ),
        supplemental_dataset_artifacts=(),
        license_evidence_url=(
            "https://raw.githubusercontent.com/evalplus/repoqa/"
            "ae876deb1365dbf5a15b0533723c8ed123eee586/LICENSE"
        ),
        adapter_source_url="https://github.com/adapstory/airflow-dags",
        dataset_license_id="Apache-2.0",
        distribution_rule="internal-only",
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
        dataset_artifact_source_id="dataset",
        dataset_artifact_url=(
            "https://huggingface.co/datasets/SWE-bench/SWE-bench_Verified/resolve/"
            "91aa3ed51b709be6457e12d00300a6a596d4c6a3/"
            "data/test-00000-of-00001.parquet"
        ),
        supplemental_dataset_artifacts=(),
        license_evidence_url=(
            "https://huggingface.co/api/datasets/SWE-bench/SWE-bench_Verified/revision/"
            "91aa3ed51b709be6457e12d00300a6a596d4c6a3"
        ),
        adapter_source_url="https://github.com/adapstory/airflow-dags",
        dataset_license_id="LicenseRef-SWE-Bench-Verified-Rights-Unverified",
        distribution_rule="internal-only-no-redistribution",
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
        dataset_artifact_source_id="dataset",
        dataset_artifact_url=(
            "https://api.github.com/repos/datadotworld/cwd-benchmark-data/tarball/"
            "0b75eb62eaf7ea315a863cd7611ebc908149f7e0"
        ),
        supplemental_dataset_artifacts=(),
        license_evidence_url=(
            "https://raw.githubusercontent.com/datadotworld/cwd-benchmark-data/"
            "0b75eb62eaf7ea315a863cd7611ebc908149f7e0/LICENSE.txt"
        ),
        adapter_source_url="https://github.com/adapstory/airflow-dags",
        dataset_license_id="Apache-2.0",
        distribution_rule="public-share-allowed",
        rights_status=_RIGHTS_ATTESTED,
        legal_boundary=(
            "The repository's Apache-2.0 license governs the captured CWD dataset snapshot."
        ),
    ),
    BenchmarkSuiteCatalogEntry(
        suite_id="rusBEIR",
        dataset_id="kngrg/rus-scifact+kngrg/rus-scifact-qrels",
        dataset_revision=(
            "75b33d32f2f13f058d0598d6d78f0c3d3afc03d9+5e0c312c9fb7304a2dc91ec7fd648b3ace5c329f"
        ),
        dataset_source_url=(
            "https://huggingface.co/datasets/kngrg/rus-scifact/raw/"
            "75b33d32f2f13f058d0598d6d78f0c3d3afc03d9/README.md"
        ),
        dataset_artifact_source_id="corpus",
        dataset_artifact_url=(
            "https://huggingface.co/datasets/kngrg/rus-scifact/resolve/"
            "75b33d32f2f13f058d0598d6d78f0c3d3afc03d9/corpus.jsonl"
        ),
        supplemental_dataset_artifacts=(
            (
                "queries",
                "https://huggingface.co/datasets/kngrg/rus-scifact/resolve/"
                "75b33d32f2f13f058d0598d6d78f0c3d3afc03d9/queries.jsonl",
            ),
            (
                "qrels",
                "https://huggingface.co/datasets/kngrg/rus-scifact-qrels/resolve/"
                "5e0c312c9fb7304a2dc91ec7fd648b3ace5c329f/test.tsv",
            ),
        ),
        license_evidence_url=(
            "https://huggingface.co/api/datasets/kngrg/rus-scifact/revision/"
            "75b33d32f2f13f058d0598d6d78f0c3d3afc03d9"
        ),
        adapter_source_url="https://github.com/adapstory/airflow-dags",
        dataset_license_id="LicenseRef-rusBEIR-Rights-Unverified",
        distribution_rule="internal-only-no-redistribution",
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
    native_adapter_materializer: Callable[
        [str, Mapping[str, bytes], Mapping[str, Mapping[str, object]]], Mapping[str, object]
    ]
    | None = None,
) -> dict[str, object]:
    """Fetch and content-address dataset bytes plus legal evidence for every suite.

    Network failures are intentionally propagated: a stale, partial, or legal-only
    catalog cannot be used as provenance for a scheduled evaluation.
    """

    _validate_observed_at(observed_at)
    if snapshot_bytes is None:
        raise ValueError("immutable dataset snapshot writer is required")
    if native_adapter_materializer is None:
        raise ValueError("native adapter materializer is required")
    suites: list[dict[str, object]] = []
    for entry in MANDATORY_BENCHMARK_SUITE_CATALOG:
        source_payload = _fetch(entry.dataset_source_url, fetch_bytes)
        license_payload = _fetch(entry.license_evidence_url, fetch_bytes)
        source_urls = (
            (entry.dataset_artifact_source_id, entry.dataset_artifact_url),
            *entry.supplemental_dataset_artifacts,
        )
        source_ids = [source_id for source_id, _ in source_urls]
        if len(source_ids) != len(set(source_ids)) or any(
            not source_id.strip() for source_id in source_ids
        ):
            raise ValueError(f"benchmark catalog has invalid dataset source ids: {entry.suite_id}")
        dataset_payloads = {source_id: _fetch(url, fetch_bytes) for source_id, url in source_urls}
        dataset_snapshots = {
            source_id: _snapshot(
                entry.suite_id,
                f"dataset-{source_id}",
                _dataset_source_url(entry, source_id),
                payload,
                snapshot_bytes,
            )
            for source_id, payload in dataset_payloads.items()
        }
        immutable_dataset_snapshots = {
            source_id: _immutable_dataset_snapshot(snapshot, entry.suite_id, source_id)
            for source_id, snapshot in dataset_snapshots.items()
        }
        native_manifest = dict(
            native_adapter_materializer(
                entry.suite_id,
                dataset_payloads,
                immutable_dataset_snapshots,
            )
        )
        _validate_native_adapter_manifest(native_manifest, entry.suite_id)
        suites.append(
            {
                "adapter_source_url": entry.adapter_source_url,
                "dataset_snapshots": dataset_snapshots,
                "dataset_id": entry.dataset_id,
                "dataset_license_id": entry.dataset_license_id,
                "dataset_revision": entry.dataset_revision,
                "distribution_rule": entry.distribution_rule,
                "execution_status": _READY,
                "legal_boundary": entry.legal_boundary,
                "license_snapshot": _snapshot(
                    entry.suite_id,
                    "license",
                    entry.license_evidence_url,
                    license_payload,
                    snapshot_bytes,
                ),
                "native_adapter_manifest": native_manifest,
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


def mandatory_benchmark_adapters_ready() -> bool:
    """Return whether D6 can run without a synthetic or missing suite adapter.

    Scheduling is a production promise.  The nightly DAG stays unscheduled
    until every mandatory suite has a canonical executable adapter; legal
    catalog snapshots continue independently while that prerequisite is not
    satisfied.
    """

    return tuple(entry.suite_id for entry in MANDATORY_BENCHMARK_SUITE_CATALOG) == (
        MANDATORY_SERP_BENCHMARK_SUITES
    )


def _dataset_source_url(entry: BenchmarkSuiteCatalogEntry, source_id: str) -> str:
    if source_id == entry.dataset_artifact_source_id:
        return entry.dataset_artifact_url
    for supplemental_source_id, url in entry.supplemental_dataset_artifacts:
        if supplemental_source_id == source_id:
            return url
    raise ValueError(f"unknown dataset source id for {entry.suite_id}: {source_id}")


def _immutable_dataset_snapshot(
    snapshot: Mapping[str, object], suite_id: str, source_id: str
) -> Mapping[str, object]:
    artifact = snapshot.get("immutable_artifact")
    if not isinstance(artifact, Mapping):
        raise ValueError(
            f"native adapter requires immutable dataset snapshot: {suite_id}/{source_id}"
        )
    return artifact


def _validate_native_adapter_manifest(manifest: Mapping[str, object], suite_id: str) -> None:
    if manifest.get("suiteId") != suite_id:
        raise ValueError(f"native adapter manifest suite mismatch: {suite_id}")
    if manifest.get("status") != "materialized":
        raise ValueError(f"native adapter did not materialize: {suite_id}")
    adapter_id = manifest.get("adapterId")
    if not isinstance(adapter_id, str) or not adapter_id.startswith("native/"):
        raise ValueError(f"native adapter manifest is missing adapterId: {suite_id}")
    case_count = manifest.get("caseCount")
    if not isinstance(case_count, int) or case_count <= 0:
        raise ValueError(f"native adapter manifest has no cases: {suite_id}")
    case_manifest_sha256 = manifest.get("caseManifestSha256")
    if (
        not isinstance(case_manifest_sha256, str)
        or len(case_manifest_sha256) != len("sha256:") + 64
        or not case_manifest_sha256.startswith("sha256:")
    ):
        raise ValueError(f"native adapter manifest has invalid digest: {suite_id}")


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
