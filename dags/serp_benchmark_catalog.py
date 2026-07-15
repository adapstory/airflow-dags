"""Canonical legal/provenance catalog for SERP's mandatory benchmark suites.

The catalog deliberately distinguishes a runnable dataset from a benchmark
harness.  A repository's source-code license never attests the rights to
redistribute or evaluate its dataset.  Each scheduled run snapshots the live
upstream dataset and licensing evidence before an adapter is allowed to run.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256

from dags.serp_eval_contracts import MANDATORY_SERP_BENCHMARK_SUITES

BENCHMARK_CATALOG_CONTRACT_VERSION = "serp-benchmark-catalog/v4"
_READY = "ready"
_RIGHTS_ATTESTED = "attested"
_RIGHTS_UNVERIFIED = "rights-unverified"
_HARNESS_LICENSE_ATTESTED = "ATTESTED"
_HARNESS_LICENSE_UNDECLARED = "UNDECLARED"
_CORPUS_ROLE_BY_SUITE = {
    "APIBench": "api-documentation",
    "ARES": "context-corpus",
    "BEIR": "beir-corpus",
    "CodeRAG-Bench": "documentation-corpus",
    "RAGBench": "source-context",
    "RepoQA": "repository-code",
    "SWE-bench Verified": "base-commit-repository",
    "cwd-benchmark-data": "reference-graph",
    "rusBEIR": "beir-corpus",
}


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
    harness_repository_url: str
    harness_revision: str
    harness_entrypoint: str
    harness_source_archive_url: str
    harness_license_url: str
    harness_license_id: str
    harness_license_status: str
    harness_distribution_rule: str
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
        harness_repository_url="https://github.com/ShishirPatil/gorilla",
        harness_revision="6ea57973c7a6097fd7c5915698c54c17c5b1b6c8",
        harness_entrypoint="gorilla/eval/eval-scripts/ast_eval_hf.py",
        harness_source_archive_url=(
            "https://api.github.com/repos/ShishirPatil/gorilla/tarball/"
            "6ea57973c7a6097fd7c5915698c54c17c5b1b6c8"
        ),
        harness_license_url=(
            "https://raw.githubusercontent.com/ShishirPatil/gorilla/"
            "6ea57973c7a6097fd7c5915698c54c17c5b1b6c8/LICENSE"
        ),
        harness_license_id="Apache-2.0",
        harness_license_status=_HARNESS_LICENSE_ATTESTED,
        harness_distribution_rule="public-share-allowed",
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
        harness_repository_url="https://github.com/stanford-futuredata/ARES",
        harness_revision="c7c9018a755faf8347c4da415632bae1593ef104",
        harness_entrypoint="ares/RAG_Automatic_Evaluation/ppi.py",
        harness_source_archive_url=(
            "https://api.github.com/repos/stanford-futuredata/ARES/tarball/"
            "c7c9018a755faf8347c4da415632bae1593ef104"
        ),
        harness_license_url=(
            "https://raw.githubusercontent.com/stanford-futuredata/ARES/"
            "c7c9018a755faf8347c4da415632bae1593ef104/LICENSE"
        ),
        harness_license_id="Apache-2.0",
        harness_license_status=_HARNESS_LICENSE_ATTESTED,
        harness_distribution_rule="public-share-allowed",
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
        harness_repository_url="https://github.com/beir-cellar/beir",
        harness_revision="ef83d29307061c65d04b035b4f4e7c18bd8374af",
        harness_entrypoint="beir/retrieval/evaluation.py",
        harness_source_archive_url=(
            "https://api.github.com/repos/beir-cellar/beir/tarball/"
            "ef83d29307061c65d04b035b4f4e7c18bd8374af"
        ),
        harness_license_url=(
            "https://raw.githubusercontent.com/beir-cellar/beir/"
            "ef83d29307061c65d04b035b4f4e7c18bd8374af/LICENSE"
        ),
        harness_license_id="Apache-2.0",
        harness_license_status=_HARNESS_LICENSE_ATTESTED,
        harness_distribution_rule="public-share-allowed",
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
        supplemental_dataset_artifacts=(
            (
                "documentation-corpus",
                "https://huggingface.co/datasets/neulab/docprompting-conala/resolve/"
                "48df7abf0f64f9279b4ee04386272eb9dc89ef89/conala-docs.jsonl",
            ),
        ),
        license_evidence_url=(
            "https://huggingface.co/api/datasets/code-rag-bench/ds1000/revision/"
            "7a5933733e549d11b75b74d3eb52bb056ffd986c"
        ),
        harness_repository_url="https://github.com/code-rag-bench/code-rag-bench",
        harness_revision="f9e100ca9ed94b8f1983b356ae81966e30210cf4",
        harness_entrypoint="generation/eval/evaluator.py",
        harness_source_archive_url=(
            "https://api.github.com/repos/code-rag-bench/code-rag-bench/tarball/"
            "f9e100ca9ed94b8f1983b356ae81966e30210cf4"
        ),
        harness_license_url=(
            "https://api.github.com/repos/code-rag-bench/code-rag-bench/git/trees/"
            "f9e100ca9ed94b8f1983b356ae81966e30210cf4?recursive=1"
        ),
        harness_license_id="LicenseRef-CodeRAG-Bench-Harness-Undeclared",
        harness_license_status=_HARNESS_LICENSE_UNDECLARED,
        harness_distribution_rule="internal-only-no-redistribution",
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
        harness_repository_url="https://github.com/rungalileo/ragbench",
        harness_revision="c28e6c22fc858086468eabb274250e27b5a8e9d8",
        harness_entrypoint="ragbench/calculate_metrics.py",
        harness_source_archive_url=(
            "https://api.github.com/repos/rungalileo/ragbench/tarball/"
            "c28e6c22fc858086468eabb274250e27b5a8e9d8"
        ),
        harness_license_url=(
            "https://api.github.com/repos/rungalileo/ragbench/git/trees/"
            "c28e6c22fc858086468eabb274250e27b5a8e9d8?recursive=1"
        ),
        harness_license_id="LicenseRef-RAGBench-Harness-Undeclared",
        harness_license_status=_HARNESS_LICENSE_UNDECLARED,
        harness_distribution_rule="internal-only-no-redistribution",
        dataset_license_id="CC-BY-4.0",
        distribution_rule="internal-only-no-redistribution",
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
        harness_repository_url="https://github.com/evalplus/repoqa",
        harness_revision="ae876deb1365dbf5a15b0533723c8ed123eee586",
        harness_entrypoint="repoqa/compute_score.py",
        harness_source_archive_url=(
            "https://api.github.com/repos/evalplus/repoqa/tarball/"
            "ae876deb1365dbf5a15b0533723c8ed123eee586"
        ),
        harness_license_url=(
            "https://raw.githubusercontent.com/evalplus/repoqa/"
            "ae876deb1365dbf5a15b0533723c8ed123eee586/LICENSE"
        ),
        harness_license_id="Apache-2.0",
        harness_license_status=_HARNESS_LICENSE_ATTESTED,
        harness_distribution_rule="public-share-allowed",
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
        harness_repository_url="https://github.com/SWE-bench/SWE-bench",
        harness_revision="f7bbbb2ccdf479001d6467c9e34af59e44a840f9",
        harness_entrypoint="swebench/harness/run_evaluation.py",
        harness_source_archive_url=(
            "https://api.github.com/repos/SWE-bench/SWE-bench/tarball/"
            "f7bbbb2ccdf479001d6467c9e34af59e44a840f9"
        ),
        harness_license_url=(
            "https://raw.githubusercontent.com/SWE-bench/SWE-bench/"
            "f7bbbb2ccdf479001d6467c9e34af59e44a840f9/LICENSE"
        ),
        harness_license_id="MIT",
        harness_license_status=_HARNESS_LICENSE_ATTESTED,
        harness_distribution_rule="public-share-allowed",
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
        harness_repository_url="https://github.com/datadotworld/cwd-benchmark-data",
        harness_revision="0b75eb62eaf7ea315a863cd7611ebc908149f7e0",
        harness_entrypoint="ACME_Insurance/investigation/acme-benchmark.ttl",
        harness_source_archive_url=(
            "https://api.github.com/repos/datadotworld/cwd-benchmark-data/tarball/"
            "0b75eb62eaf7ea315a863cd7611ebc908149f7e0"
        ),
        harness_license_url=(
            "https://raw.githubusercontent.com/datadotworld/cwd-benchmark-data/"
            "0b75eb62eaf7ea315a863cd7611ebc908149f7e0/LICENSE.txt"
        ),
        harness_license_id="Apache-2.0",
        harness_license_status=_HARNESS_LICENSE_ATTESTED,
        harness_distribution_rule="public-share-allowed",
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
        harness_repository_url="https://github.com/beir-cellar/beir",
        harness_revision="ef83d29307061c65d04b035b4f4e7c18bd8374af",
        harness_entrypoint="beir/retrieval/evaluation.py",
        harness_source_archive_url=(
            "https://api.github.com/repos/beir-cellar/beir/tarball/"
            "ef83d29307061c65d04b035b4f4e7c18bd8374af"
        ),
        harness_license_url=(
            "https://raw.githubusercontent.com/beir-cellar/beir/"
            "ef83d29307061c65d04b035b4f4e7c18bd8374af/LICENSE"
        ),
        harness_license_id="Apache-2.0",
        harness_license_status=_HARNESS_LICENSE_ATTESTED,
        harness_distribution_rule="public-share-allowed",
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
    native_corpus_materializer: Callable[
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
    if native_corpus_materializer is None:
        raise ValueError("native corpus materializer is required")
    suites: list[dict[str, object]] = []
    for entry in MANDATORY_BENCHMARK_SUITE_CATALOG:
        source_payload = _fetch(entry.dataset_source_url, fetch_bytes)
        license_payload = _fetch(entry.license_evidence_url, fetch_bytes)
        harness_source_archive_payload = _fetch(entry.harness_source_archive_url, fetch_bytes)
        harness_license_payload = _fetch(entry.harness_license_url, fetch_bytes)
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
        harness_source_archive_snapshot = _snapshot(
            entry.suite_id,
            "harness-source-archive",
            entry.harness_source_archive_url,
            harness_source_archive_payload,
            snapshot_bytes,
        )
        harness_license_snapshot = _snapshot(
            entry.suite_id,
            "harness-license",
            entry.harness_license_url,
            harness_license_payload,
            snapshot_bytes,
        )
        official_harness = {
            "distribution_rule": entry.harness_distribution_rule,
            "entrypoint": entry.harness_entrypoint,
            "license_id": entry.harness_license_id,
            "license_snapshot": harness_license_snapshot,
            "license_status": entry.harness_license_status,
            "repository_url": entry.harness_repository_url,
            "revision": entry.harness_revision,
            "source_archive_snapshot": harness_source_archive_snapshot,
        }
        native_manifest = dict(
            native_adapter_materializer(
                entry.suite_id,
                dataset_payloads,
                immutable_dataset_snapshots,
            )
        )
        corpus_blocking_reason: str | None = None
        try:
            corpus_materialization = native_corpus_materializer(
                entry.suite_id,
                dataset_payloads,
                immutable_dataset_snapshots,
            )
            corpus_manifest, corpus_payloads = _validated_native_corpus_materialization(
                corpus_materialization,
                entry.suite_id,
                dataset_payloads,
            )
        except ValueError as exc:
            corpus_manifest = None
            corpus_payloads = {}
            corpus_blocking_reason = f"query-independent-corpus-unavailable: {exc}"
        corpus_snapshots = {
            source_id: _corpus_snapshot(
                entry.suite_id,
                source_id,
                _CORPUS_ROLE_BY_SUITE[entry.suite_id],
                payload,
                snapshot_bytes,
            )
            for source_id, payload in corpus_payloads.items()
        }
        if corpus_manifest is not None:
            native_manifest["corpusManifest"] = corpus_manifest
            native_manifest["corpusEvidence"] = [
                _native_corpus_evidence(source_id, snapshot)
                for source_id, snapshot in corpus_snapshots.items()
            ]
        native_manifest["officialHarness"] = {
            "entrypoint": entry.harness_entrypoint,
            "licenseEvidence": _immutable_snapshot_artifact(
                harness_license_snapshot,
                entry.suite_id,
                "harness license",
            ),
            "licenseId": entry.harness_license_id,
            "licenseStatus": entry.harness_license_status,
            "repositoryUrl": entry.harness_repository_url,
            "revision": entry.harness_revision,
            "sourceArchiveEvidence": _immutable_snapshot_artifact(
                harness_source_archive_snapshot,
                entry.suite_id,
                "harness source archive",
            ),
        }
        _validate_native_adapter_manifest(native_manifest, entry.suite_id)
        suites.append(
            {
                "dataset_snapshots": dataset_snapshots,
                "corpus_snapshots": corpus_snapshots,
                "dataset_id": entry.dataset_id,
                "dataset_license_id": entry.dataset_license_id,
                "dataset_revision": entry.dataset_revision,
                "distribution_rule": entry.distribution_rule,
                "execution_status": (
                    _READY if corpus_blocking_reason is None else "corpus-evidence-blocked"
                ),
                "legal_boundary": entry.legal_boundary,
                "license_snapshot": _snapshot(
                    entry.suite_id,
                    "license",
                    entry.license_evidence_url,
                    license_payload,
                    snapshot_bytes,
                ),
                "native_adapter_manifest": native_manifest,
                "official_harness": official_harness,
                "source_snapshot": _snapshot(
                    entry.suite_id,
                    "source",
                    entry.dataset_source_url,
                    source_payload,
                    snapshot_bytes,
                ),
                "rights_status": entry.rights_status,
                "suite_id": entry.suite_id,
                **(
                    {}
                    if corpus_blocking_reason is None
                    else {"blocking_reason": corpus_blocking_reason}
                ),
            }
        )
    blocking_suites = [suite for suite in suites if suite["execution_status"] != _READY]
    return {
        "catalog_status": "ready" if not blocking_suites else "blocked",
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


def _immutable_snapshot_artifact(
    snapshot: Mapping[str, object], suite_id: str, evidence_type: str
) -> Mapping[str, object]:
    artifact = snapshot.get("immutable_artifact")
    if not isinstance(artifact, Mapping):
        raise ValueError(f"{suite_id} requires immutable {evidence_type} evidence")
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
    official_harness = manifest.get("officialHarness")
    if not isinstance(official_harness, Mapping):
        raise ValueError(f"native adapter manifest has no official harness: {suite_id}")
    if official_harness.get("licenseStatus") not in {
        _HARNESS_LICENSE_ATTESTED,
        _HARNESS_LICENSE_UNDECLARED,
    }:
        raise ValueError(f"native adapter manifest has invalid harness license status: {suite_id}")
    for field_name in ("licenseEvidence", "sourceArchiveEvidence"):
        evidence = official_harness.get(field_name)
        if not isinstance(evidence, Mapping) or evidence.get("objectLockMode") != "COMPLIANCE":
            raise ValueError(f"native adapter manifest has invalid {field_name}: {suite_id}")


def _validated_native_corpus_materialization(
    materialization: Mapping[str, object],
    suite_id: str,
    dataset_payloads: Mapping[str, bytes],
) -> tuple[dict[str, object], dict[str, bytes]]:
    if not isinstance(materialization, Mapping) or set(materialization) != {
        "manifest",
        "payloads",
    }:
        raise ValueError(f"native corpus materialization has an invalid shape: {suite_id}")
    manifest_value = materialization.get("manifest")
    payloads_value = materialization.get("payloads")
    if not isinstance(manifest_value, Mapping) or not isinstance(payloads_value, Mapping):
        raise ValueError(f"native corpus materialization is incomplete: {suite_id}")
    manifest = dict(manifest_value)
    if set(manifest) != {
        "datasetSha256BySource",
        "schema",
        "sources",
        "status",
        "suiteId",
    }:
        raise ValueError(f"native corpus manifest has an invalid shape: {suite_id}")
    if manifest.get("schema") != "NativeBenchmarkCorpusManifest/v1":
        raise ValueError(f"native corpus manifest schema is unsupported: {suite_id}")
    if manifest.get("suiteId") != suite_id or manifest.get("status") != "materialized":
        raise ValueError(f"native corpus manifest identity/status is invalid: {suite_id}")
    expected_dataset_digests = {
        source_id: "sha256:" + sha256(payload).hexdigest()
        for source_id, payload in dataset_payloads.items()
    }
    if manifest.get("datasetSha256BySource") != expected_dataset_digests:
        raise ValueError(f"native corpus manifest dataset lineage is invalid: {suite_id}")
    payloads: dict[str, bytes] = {}
    for source_id, payload in payloads_value.items():
        if not isinstance(source_id, str) or not source_id.strip():
            raise ValueError(f"native corpus payload sourceId is invalid: {suite_id}")
        if not isinstance(payload, bytes) or not payload:
            raise ValueError(f"native corpus payload is empty: {suite_id}/{source_id}")
        _validate_canonical_corpus_jsonl(payload, suite_id, source_id)
        payloads[source_id] = payload
    sources = manifest.get("sources")
    if not isinstance(sources, list) or len(sources) != 1:
        raise ValueError(f"native corpus manifest must expose one canonical source: {suite_id}")
    source = sources[0]
    if not isinstance(source, Mapping) or set(source) != {
        "corpusRole",
        "documentCount",
        "payloadSha256",
        "sourceId",
    }:
        raise ValueError(f"native corpus source has an invalid shape: {suite_id}")
    source_id = source.get("sourceId")
    if not isinstance(source_id, str) or set(payloads) != {source_id}:
        raise ValueError(f"native corpus source/payload identity mismatch: {suite_id}")
    if source.get("corpusRole") != _CORPUS_ROLE_BY_SUITE[suite_id]:
        raise ValueError(f"native corpus role is invalid: {suite_id}")
    document_count = source.get("documentCount")
    if not isinstance(document_count, int) or document_count <= 0:
        raise ValueError(f"native corpus document count is invalid: {suite_id}")
    if document_count != len(payloads[source_id].splitlines()):
        raise ValueError(f"native corpus document count does not match payload: {suite_id}")
    if source.get("payloadSha256") != "sha256:" + sha256(payloads[source_id]).hexdigest():
        raise ValueError(f"native corpus payload digest is invalid: {suite_id}")
    return manifest, payloads


def _validate_canonical_corpus_jsonl(payload: bytes, suite_id: str, source_id: str) -> None:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"native corpus is not UTF-8: {suite_id}/{source_id}") from exc
    if not text.endswith("\n") or not text.strip():
        raise ValueError(f"native corpus must be newline-terminated JSONL: {suite_id}/{source_id}")
    documents: list[dict[str, str]] = []
    for line in text.splitlines():
        try:
            document = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"native corpus JSONL is invalid: {suite_id}/{source_id}") from exc
        if (
            not isinstance(document, dict)
            or set(document) != {"documentId", "text"}
            or not isinstance(document.get("documentId"), str)
            or not document["documentId"].strip()
            or not isinstance(document.get("text"), str)
            or not document["text"].strip()
        ):
            raise ValueError(
                f"native corpus document must contain only documentId/text: {suite_id}/{source_id}"
            )
        documents.append(document)
    document_ids = [document["documentId"] for document in documents]
    if document_ids != sorted(set(document_ids)):
        raise ValueError(
            f"native corpus documents must be unique and sorted: {suite_id}/{source_id}"
        )
    canonical = b"".join(
        json.dumps(document, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )
        + b"\n"
        for document in documents
    )
    if canonical != payload:
        raise ValueError(f"native corpus JSONL is not canonical: {suite_id}/{source_id}")


def _corpus_snapshot(
    suite_id: str,
    source_id: str,
    corpus_role: str,
    payload: bytes,
    snapshot_bytes: Callable[[str, str, str, bytes], Mapping[str, object]],
) -> dict[str, object]:
    url = f"derived://native-corpus/{suite_id}/{source_id}"
    snapshot = _snapshot(
        suite_id,
        f"corpus-{source_id}",
        url,
        payload,
        snapshot_bytes,
    )
    return {
        "corpus_role": corpus_role,
        "immutable_artifact": snapshot["immutable_artifact"],
        "sha256": snapshot["sha256"],
        "url": url,
    }


def _native_corpus_evidence(
    source_id: str,
    snapshot: Mapping[str, object],
) -> dict[str, str]:
    artifact = snapshot.get("immutable_artifact")
    if not isinstance(artifact, Mapping):
        raise ValueError(f"native corpus evidence is not immutable: {source_id}")
    return {
        "artifactPath": _required_catalog_str(artifact, "artifactPath"),
        "artifactSha256": _required_catalog_str(artifact, "artifactSha256"),
        "artifactVersionId": _required_catalog_str(artifact, "artifactVersionId"),
        "corpusRole": _required_catalog_str(snapshot, "corpus_role"),
        "objectLockMode": _required_catalog_str(artifact, "objectLockMode"),
        "sourceId": source_id,
    }


def _required_catalog_str(value: Mapping[str, object], field_name: str) -> str:
    field_value = value.get(field_name)
    if not isinstance(field_value, str) or not field_value.strip():
        raise ValueError(f"benchmark catalog {field_name} is required")
    return field_value.strip()


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
