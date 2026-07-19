"""Canonical DS-1000 supply/runtime contract shared by Airflow consumers.

The CodeRAG benchmark profile invokes the official DS-1000 evaluator.  Those
are distinct domain identities: the benchmark suite remains CodeRAG-Bench,
while the immutable sandbox inventory is identified as DS-1000.  Consumers
must validate both identities explicitly and must never accept a legacy alias.
"""

from __future__ import annotations

DS1000_REVISION = "b39aab71da6d23ef8d3cac59a7c5f834516ab334"
DS1000_PLATFORM = "linux/amd64"
DS1000_PYTHON_VERSION = "3.10"
DS1000_IMAGE_PURPOSE = "ds1000-simplified-official-execution"
DS1000_OFFICIAL_DATASET_PATH = "data/ds1000.jsonl.gz"
DS1000_INVENTORY_SUITE_ID = "DS-1000"
DS1000_EXECUTOR_COMMAND = "/opt/ds1000-venv/bin/python"
DS1000_PYTORCH_VARIANT = "cpuonly"
DS1000_SANDBOX_IMAGE_INVENTORY_SCHEMA = "Ds1000SandboxImageInventory/v2"
DS1000_WHEELHOUSE_MANIFEST_SCHEMA = "Ds1000WheelhouseManifest/v3"
DS1000_BASE_IMAGE_PROVENANCE_SCHEMA = "Ds1000BaseImageProvenance/v1"
DS1000_DATASET_PROVENANCE_SCHEMA = "Ds1000SimplifiedDatasetProvenance/v1"
DS1000_SUPPLY_ATTESTATION_SCHEMA = "Ds1000SandboxSupplyAttestation/v3"
DS1000_BASE_IMAGE_SOURCE_REFERENCE = (
    "harbor.adapstory.com/dockerhub-cache/library/python:3.10-slim-bookworm"
)
DS1000_BASE_IMAGE_REPOSITORY = "harbor.adapstory.com/dockerhub-cache/library/python"
DS1000_SANDBOX_IMAGE_REPOSITORY = "harbor.adapstory.com/benchmark-sandboxes/ds1000"
DS1000_DATASET_FIELD_NAMES = (
    "code_context",
    "metadata",
    "prompt",
    "reference_code",
)
DS1000_DATASET_ROW_COUNT = 1000
DS1000_LIBRARY_VERSIONS = (
    ("datasets", "2.19.1"),
    ("gensim", "4.3.2"),
    ("matplotlib", "3.8.4"),
    ("numpy", "1.26.4"),
    ("pandas", "1.5.3"),
    ("scikit-learn", "1.4.0"),
    ("scipy", "1.12.0"),
    ("seaborn", "0.13.2"),
    ("statsmodels", "0.14.1"),
    ("tensorflow-cpu", "2.16.1"),
    ("torch", "2.2.0+cpu"),
    ("tqdm", "4.66.4"),
    ("xgboost", "2.0.3"),
)
