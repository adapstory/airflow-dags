Airflow DAGs for Adapstory production-like infrastructure.

Runtime contract:

- DAG files must parse cleanly on the deployed Airflow image.
- Do not import optional business dependencies at module import time.
- Do not use local-only endpoints such as `host.docker.internal`.
- Add service endpoints, credentials, and runtime packages through GitOps before
  adding a DAG that depends on them.
- Keep at least one dependency-free platform smoke DAG available for runtime
  health verification.

SERP eval DAG contracts:

- `serp_nightly_regression_suite` is the D6 contract DAG. Its `dag_run.conf`
  must provide tenant id, pack version ids, retrieval/reranker profile versions,
  registry resource identity, approved actor id, generated timestamp, and every
  mandatory SERP benchmark suite id. Missing or partial suite lists fail closed.
- `serp_tenant_golden_set_regression` is the D13 contract DAG. Its
  `dag_run.conf` must provide tenant id, workflow id, golden set id/version,
  changed pack version ids, registry resource identity, approved actor id, and
  generated timestamp. Missing workflow or golden-set provenance fails closed.
- These DAGs intentionally emit local handoff artifacts only. Live runner
  images, BC-21 submission endpoints, connections, and credentials must be added
  through GitOps before replacing the pending external-runner/submission tasks
  with networked operators.
