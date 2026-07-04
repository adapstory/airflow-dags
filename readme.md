Airflow DAGs for Adapstory production-like infrastructure.

Runtime contract:

- DAG files must parse cleanly on the deployed Airflow image.
- Do not import optional business dependencies at module import time.
- Do not use local-only endpoints such as `host.docker.internal`.
- Add service endpoints, credentials, and runtime packages through GitOps before
  adding a DAG that depends on them.
- Keep at least one dependency-free platform smoke DAG available for runtime
  health verification.
1
