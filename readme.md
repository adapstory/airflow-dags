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
  mandatory SERP benchmark suite id. It must also provide an absolute local
  `artifact_root_path` plus the reviewed `bc21_base_url`; the DAG derives
  `airflow-plan.json`, `suite-plan.json`, `nightly-report.json`,
  `benchmark-gate-export.json`, `nightly-registry-submissions.json`, and
  `nightly-registry-receipts.json` under a deterministic operation
  directory. Missing or partial suite lists fail closed. D6 writes
  `suite-plan.json`, runs the packaged
  `python -m dags.serp_eval_contracts` runner without shell
  expansion, persists each CLI stdout artifact, and submits
  `nightly-registry-submissions.json` to BC-21 through the reviewed
  `bc21_base_url`. Local dry-run receipt writers are explicit dev/test
  fallback helpers only and are not the default DAG runtime path.
- `serp_tenant_golden_set_regression` is the D13 contract DAG. Its
  `dag_run.conf` must provide tenant id, workflow id, golden set id/version,
  changed pack version ids, registry resource identity, approved actor id, and
  generated timestamp. It must also provide an absolute local
  `artifact_root_path`; the DAG derives `airflow-plan.json`, `golden-set.json`,
  `tenant-golden-report.json`, and `tenant-golden-registry-submissions.json`
  under a deterministic operation directory. Missing workflow or golden-set
  provenance fails closed.
- `serp_benchmark_improvement_wave` is the D19 contract DAG. Its
  `dag_run.conf` must provide tenant id, improvement spec id, baseline run id,
  candidate id, registry resource identity, approved actor id, generated
  timestamp, rollback policy ref, positive max benchmark run budget, every
  mandatory SERP benchmark suite id, replay profile versions, judge
  model/template versions, feature flags, policy/guardrail bundle versions, and
  provider/model-catalog route ids. It must also provide an absolute local
  `artifact_root_path`; the DAG derives `airflow-plan.json`,
  `improvement-spec.json`, `candidate-eval-report.json`,
  `keep-discard-decision.json`, and `improvement-scoreboard.json` under a
  deterministic operation directory. D19 writes deterministic dry-run
  improvement artifacts in-task and passes the same payloads through XCom:
  `improvement-spec.json`, `candidate-eval-report.json`,
  `keep-discard-decision.json`, and `improvement-scoreboard.json`. Each
  downstream artifact consumer verifies the upstream wrapper contract version
  and recomputes `artifactSha256` over the nested payload before accepting XCom
  input. Missing suites, unbounded benchmark budgets, missing replay/model
  governance metadata, raw secrets, malformed or tampered artifacts, or
  below-floor candidate scores fail closed.
- D13 intentionally emits local handoff artifacts and gateway CLI argv specs
  only. Its runner/export/submission tasks return deterministic
  `python -m dags.serp_eval_contracts ...` arguments plus a
  `stdout_path`; the executor must run the argv without shell expansion and
  write stdout to that path. Live runner images, service endpoints, and network
  policy must be added through GitOps before replacing those file-based handoff
  tasks with native networked operators. D6 now executes the same CLI bridge in
  the DAG default path and fails closed when BC-21 submission is not configured.
  D19 still keeps native deterministic artifact writers until its live
  improvement runner is wired through GitOps.
- `artifact_root_path` must be a local absolute path. URLs, parent traversal,
  multiline values, and raw secret material are rejected before any runner
  handoff is emitted.
