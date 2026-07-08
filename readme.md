Airflow DAGs for Adapstory production-like infrastructure.

Runtime contract:

- DAG files must parse cleanly on the deployed Airflow image.
- Do not import optional business dependencies at module import time.
- Do not use local-only endpoints such as `host.docker.internal`.
- Add service endpoints, credentials, and runtime packages through GitOps before
  adding a DAG that depends on them. The production Airflow image must install
  this repository as a package instead of relying on dag-processor-only
  `gitSync` visibility.
- Keep at least one dependency-free platform smoke DAG available for runtime
  health verification.

SERP eval DAG contracts:

- D1-D20 runtime coverage checkpoint:

  | DAG ID | Runtime status | Contract note |
  | --- | --- | --- |
  | D1 `serp_ingest_source_once` | Planned gap | One-shot source ingest DAG is not implemented yet. |
  | D2 `serp_refresh_due_sources` | Planned gap | Scheduled freshness/popularity refresh DAG is not implemented yet. |
  | D3 `serp_reparse_pack_version` | Planned gap | Pack-version reparse DAG is not implemented yet. |
  | D4 `serp_scan_parse_index` | Planned gap | Scan/parse/enrich/index child DAG or task group is not implemented yet. |
  | D5 `serp_publish_signed_pack` | Implemented live-submit contract in current source | Builds the governed public-docs BC-21 publish activation request from indexed D20 batch evidence, submits it to configured BC-21, and records an active receipt after approval, evidence bundle, evidence seal, and benchmark gate inputs are supplied. BC-21 remains the authority for approval/seal validation, idempotency, and active-version mutation. |
  | D6 `serp_nightly_regression_suite` | Implemented, runtime-backed in current source | Production GitOps image/DAG refs must be refreshed before claiming deployed-current runtime. |
  | D7 `serp_online_eval_rollup` | Implemented, runtime-backed in current source | DAG is manual/event-triggered today; backlog frequent scheduling remains planned. Production GitOps refs must be refreshed before claiming deployed-current runtime. |
  | D8 `serp_expire_revoke_packs` | Planned gap | Freshness expiration/revocation DAG is not implemented yet. |
  | D9 `serp_usage_cost_rollup` | Planned gap | CostOps usage rollup DAG is not implemented yet. |
  | D10 `serp_public_catalog_refresh` | Planned gap | Public catalog refresh DAG is not implemented yet. |
  | D11 `serp_tenant_offboarding_purge` | Planned gap | Tenant export/revoke/purge DAG is not implemented yet. |
  | D12 `serp_evidence_seal_verify` | Planned gap | Evidence sealing/verification DAG is not implemented yet. |
  | D13 `serp_tenant_golden_set_regression` | Scaffolded handoff contract | Emits deterministic gateway CLI handoff artifacts; native runtime execution is still planned. |
  | D14 `serp_break_glass_expiry_reconcile` | Planned gap | Emergency override expiry/reconcile DAG is not implemented yet. |
  | D15 `serp_offline_bundle_build_publish` | Planned gap | Offline bundle build/publish DAG is not implemented yet. |
  | D16 `serp_policy_rollout_canary` | Planned gap | Policy rollout canary DAG is not implemented yet. |
  | D17 `serp_model_catalog_promotion` | Planned gap | Model promotion/deprecation DAG is not implemented yet. |
  | D18 `serp_chaos_restore_game_day` | Planned gap | Restore/game-day DAG is not implemented yet. |
  | D19 `serp_benchmark_improvement_wave` | Scaffolded deterministic contract | Writes deterministic improvement artifacts in-task; live improvement runner wiring is still planned. |
  | D20 `serp_web_seed_crawl_refresh` | Implemented scheduled pipeline CLI bridge in current source | Uses a default stack-inventory anchored seed registry when no `dag_run.conf` is supplied, expands approved website `frontier_urls` into deterministic per-page fetch requests, selects due seeds from `refresh_policy` and optional `freshness_state`, writes deterministic seed/refresh artifacts, runs the packaged SERP pipeline CLI bridge only when sources are due, and emits a governed D5 trigger-conf artifact once indexed D20 evidence exists. The D5 trigger-conf carries `ADAPSTORY_SERP_BC21_BASE_URL` when the runtime env provides it, but approvals, evidence seal, benchmark gate, and idempotency remain required. Live robots/sitemap discovery, D4 child dispatch, and deployed GitOps image refresh remain planned; publish activation is handled by D5. |

- `serp_nightly_regression_suite` is the D6 contract DAG. Its `dag_run.conf`
  must provide tenant id, pack version ids, retrieval/reranker profile versions,
  registry resource identity, approved actor id, generated timestamp, and every
  mandatory SERP benchmark suite id. It must also provide `bc21_base_url` plus
  either `artifact_root_path` or the runtime default
  `ADAPSTORY_AIRFLOW_ARTIFACT_ROOT`; artifact locations may be absolute local
  paths or `s3://bucket/prefix` URIs. The DAG derives
  `airflow-plan.json`, `suite-plan.json`, `nightly-report.json`,
  `benchmark-gate-export.json`, `nightly-registry-submissions.json`, and
  `nightly-registry-receipts.json` under a deterministic operation
  directory. Missing or partial suite lists fail closed. D6 writes
  `suite-plan.json`, runs the packaged
  `python -m adapstory_serp_mcp_gateway.airflow_eval_cli` runner without shell
  expansion, persists each CLI stdout artifact, and submits
  `nightly-registry-submissions.json` to BC-21 through the reviewed
  `bc21_base_url`. Local dry-run receipt writers are explicit dev/test
  fallback helpers only and are not the default DAG runtime path.
- `serp_tenant_golden_set_regression` is the D13 scaffolded handoff contract
  DAG, not a native runtime-backed runner yet. Its
  `dag_run.conf` must provide tenant id, workflow id, golden set id/version,
  changed pack version ids, registry resource identity, approved actor id, and
  generated timestamp. It must also provide `artifact_root_path` or rely on the
  runtime default `ADAPSTORY_AIRFLOW_ARTIFACT_ROOT`; artifact locations may be
  absolute local paths or `s3://bucket/prefix` URIs. The DAG derives
  `airflow-plan.json`, `golden-set.json`, `tenant-golden-report.json`, and
  `tenant-golden-registry-submissions.json`
  under a deterministic operation directory. Missing workflow or golden-set
  provenance fails closed.
- `serp_online_eval_rollup` is the D7 sampled online-eval contract DAG. Its
  `dag_run.conf` must provide tenant id, registry resource identity, approved
  actor id, generated timestamp, and one or more online-eval reports produced
  from sampled real requests. It must also provide `artifact_root_path` or rely
  on `ADAPSTORY_AIRFLOW_ARTIFACT_ROOT`; artifact locations may be absolute
  local paths or `s3://bucket/prefix` URIs. The DAG derives
  `airflow-plan.json`, `online-eval-rollup-plan.json`,
  `online-eval-rollup.json`, and
  `online-eval-registry-submissions.json` under a deterministic operation
  directory. D7 writes the rollup plan artifact, runs the packaged
  `python -m adapstory_serp_mcp_gateway.airflow_eval_cli online-eval-rollup`
  runner without shell expansion, persists stdout, then builds BC-21 registry
  submissions for the same rollup. Its plan state is
  `ready_for_po_capacity_approval`; it is not a 1M production approval.
- `serp_benchmark_improvement_wave` is the D19 scaffolded deterministic
  contract DAG, not a live runtime-backed improvement runner yet. Its
  `dag_run.conf` must provide tenant id, improvement spec id, baseline run id,
  candidate id, registry resource identity, approved actor id, generated
  timestamp, rollback policy ref, positive max benchmark run budget, every
  mandatory SERP benchmark suite id, replay profile versions, judge
  model/template versions, feature flags, policy/guardrail bundle versions, and
  provider/model-catalog route ids. It must also provide `artifact_root_path`
  or rely on the runtime default `ADAPSTORY_AIRFLOW_ARTIFACT_ROOT`; artifact
  locations may be absolute local paths or `s3://bucket/prefix` URIs. The DAG
  derives `airflow-plan.json`,
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
- `serp_web_seed_crawl_refresh` is the D20 scheduled public-docs seed refresh
  DAG. When `dag_run.conf` is empty, it builds the default public-docs seed
  registry from the stack-inventory anchored source set. Override
  `dag_run.conf` may still provide tenant id, pack id/version, registry
  resource identity, approved actor id, generated timestamp, a governed
  `seed_registry`, `index_mode` (`evidence-only` or `live`), `embedding_mode`
  (`deterministic-dev` or `live-gateway`), target store names
  (`qdrant_collection`, `opensearch_index`, `neo4j_database`), and either
  `artifact_root_path` or
  `ADAPSTORY_AIRFLOW_ARTIFACT_ROOT`. The seed registry is intentionally limited
  to currently executable connector types: `git`, `website`, `openapi`, and
  `pdf`. Markdown/file-upload intake, Confluence, Notion, and Google Docs are
  taxonomy or planned adapters until their connectors exist in the SERP
  pipeline. Each seed must be approved, public or external-ok, reference the
  `tmp/stack-inventory-2026-07-02.md` inventory evidence, include official-docs
  URI, license/distribution state, daily/nightly refresh policy, and a bounded
  crawl policy with robots enforcement, sitemap intent, allowlist, denylist,
  optional governed `frontier_urls`, max depth, max pages, and user agent.
  Website seeds with approved `frontier_urls` are expanded into deterministic
  per-page `source_fetch_requests` before the packaged pipeline CLI runs, so
  D20 evidence records the exact pages selected for fetch/parse/chunk/embed/
  index. Optional `freshness_state` is accepted per seed; seeds without a
  previous `last_success_at` are due, and indexed seeds are refreshed only after
  `refresh_policy.max_age_hours`. The DAG derives `airflow-plan.json`,
  `public-docs-seed-registry.json`, `public-docs-seed-refresh-plan.json`, and
  after successful indexed D20 evidence,
  `public-docs-publish-activation-trigger-conf.json` with the exact D5
  `public_docs_seed_refresh_result_path` and canonical tenant/pack/version
  identity. The trigger-conf deliberately carries no approval/seal/benchmark
  secrets; it marks the governance inputs that D5 must still receive before
  publish activation can run. The refresh plan contains due
  `source_fetch_requests` plus skipped-seed evidence. If no seed is due, the
  pipeline bridge writes a
  deterministic `no_due_sources` result without spawning the CLI process.
  Otherwise it runs
  `python -m adapstory_serp_pipeline.orchestration.seed_refresh_cli` without
  shell expansion, passes the selected `--embedding-mode`, `--index-mode`, and
  store target names, then persists `public-docs-seed-refresh-result.json`.
  The packaged CLI executes the current fetch/parse/chunk/embed/index path
  through the SERP pipeline ports and writes deterministic batch evidence.
  `index_mode=live` requires `embedding_mode=live-gateway`; evidence-only
  mode defaults to `deterministic-dev`. The Airflow source contract supports
  env defaults through `ADAPSTORY_SERP_PUBLIC_DOCS_INDEX_MODE`,
  `ADAPSTORY_SERP_PUBLIC_DOCS_EMBEDDING_MODE`,
  `ADAPSTORY_SERP_PUBLIC_DOCS_QDRANT_COLLECTION`,
  `ADAPSTORY_SERP_PUBLIC_DOCS_OPENSEARCH_INDEX`, and
  `ADAPSTORY_SERP_PUBLIC_DOCS_NEO4J_DATABASE`.
  Live index mode is implemented in the packaged pipeline via HTTP embedding
  and store adapters, and governed seed-frontier expansion is implemented in
  the Airflow D20 handoff. Deployed-current GitOps image/package refs, runtime
  env wiring, OpenSearch/Neo4j/Qdrant network-policy allowances, live
  robots/sitemap traversal, and changed-page discovery beyond seed freshness
  remain planned runtime work.
- Runtime status in this document means current source-level contract unless a
  deployed runtime is explicitly named. The production Airflow image and
  `gitSync` revision are pinned in GitOps by `Adapstory-GitOps/infra/airflow`;
  if those refs lag the submodule HEADs, D6/D7/D20 must not be described as
  deployed-current until the runtime image, pinned DAG ref, and package refs are
  refreshed and verified.
- D13 intentionally emits local handoff artifacts and gateway CLI argv specs
  only. Its runner/export/submission tasks return deterministic
  `python -m adapstory_serp_mcp_gateway.airflow_eval_cli ...` arguments plus a
  `stdout_path`; the executor must run the argv without shell expansion and
  write stdout to that path. When `stdout_path` or input artifacts are S3
  locations, the executor materializes inputs to a temp file inside the task
  pod and uploads stdout as the resulting artifact; the CLI module itself stays
  local-file only. Live runner images, service endpoints, and network policy
  must be added through GitOps before replacing those file-based handoff tasks
  with native networked operators. D6 now executes the same CLI bridge in the
  DAG default path and fails closed when BC-21 submission is not configured.
  D19 still keeps native deterministic artifact writers until its live
  improvement runner is wired through GitOps.
- `serp_publish_signed_pack` is the D5 public-docs publish activation handoff
  DAG. Its `dag_run.conf` must provide tenant id, pack id/version, registry
  resource identity, approved actor id, generated timestamp,
  `public_docs_seed_refresh_result_path`, `approval_run_id`,
  `evidence_bundle_id`, `evidence_seal_hash`,
  `activation_idempotency_key`, `activation_reason_code`,
  `benchmark_gate_export_sha256`, and `bc21_base_url`. The DAG writes
  `airflow-plan.json`, builds a packaged pipeline CLI spec, and runs
  `python -m adapstory_serp_pipeline.registry.publish_activation_cli` without
  shell expansion. The CLI verifies the D20 batch evidence hash, requires every
  source to be indexed with `activation_pending`, and writes
  `public-docs-publish-activation-request.json`. The follow-up submit task runs
  `python -m adapstory_serp_pipeline.registry.publish_activation_cli submit`,
  posts that request to BC-21, requires the response to mark both activation and
  pack version as `active`, and writes
  `public-docs-publish-activation-receipt.json`. BC-21 remains responsible for
  signature validation, approval/seal validation, idempotent submission
  acceptance, and active-pack mutation.
- D20-to-D5 continuation is artifact-governed, not an approval bypass. The
  D20 trigger-conf can be used as the base `dag_run.conf` for
  `serp_publish_signed_pack`, but D5 still fails closed until
  `approval_run_id`, `evidence_bundle_id`, `evidence_seal_hash`,
  `activation_idempotency_key`, and `benchmark_gate_export_sha256` are supplied
  by the governance/evidence seal flow. `bc21_base_url` can be supplied by the
  caller or by the non-secret runtime default `ADAPSTORY_SERP_BC21_BASE_URL`;
  unsafe public HTTP URLs and raw-secret-looking values are rejected before the
  trigger-conf artifact is written.
- `artifact_root_path` must be an absolute local path or `s3://bucket/prefix`
  URI. Unsupported URL schemes, parent traversal, multiline values, and raw
  secret material are rejected before any runner handoff is emitted.
