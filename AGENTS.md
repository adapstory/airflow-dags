# Adapstory Submodule Agent Contract

This repository is an Adapstory platform submodule. The goal is reliable,
long-term product development, not saving agent tokens or shaving local
validation time. Agents working here must optimize for correctness,
maintainability, observability, and future evolution.

## Read Order

1. This `AGENTS.md`.
2. Local `README.md`, `CONTRIBUTING.md`, and service docs when present.
3. If checked out inside the `adapstory-ai-lms` umbrella, also read the parent
   `../AGENTS.md`, `../docs/standards.md`, `../docs/maven-workflow.md`, and
   relevant regulations under `../docs/`.
4. For infrastructure, observability, GitOps, CI/CD, or deployment work, read
   `../docs/monitoring-observability-regulation.md` when available.

## Mandatory BMAD Delivery Cycle

Every AI agent must self-manage the BMAD-style cycle for non-trivial work:

1. Start from a tracked beads issue or create one before changing files.
2. Clarify product intent and acceptance criteria with the PO when ambiguity
   would change behavior, data contracts, security, cost, or operations.
3. Investigate root cause before proposing or implementing a fix.
4. Write or update a failing test first for production behavior changes.
5. Design the durable solution, including data model, migrations, dependency
   wiring, configuration, telemetry, and rollback when those are part of the
   real problem.
6. Implement production code and tests together. Keep changes in the owning
   module unless the contract is intentionally cross-cutting.
7. Run the honest validation gates for the changed scope and fix failures.
8. Finish with `agent-finish-protocol`: stage exact files, commit, close/update
   beads, push beads, push Git, and report evidence.

## Engineering Standard

### Root Cause

- Prefer root-cause fixes over symptom patches.
- Prefer production paths, real integration contracts, migrations, and durable
  configuration over temporary branches or local-only behavior.
- Be proactive when long-term reliability requires adjacent work: schema
  migration, dedicated artifact tables, indexes, backfills, idempotency,
  dependency configuration, infrastructure values, telemetry, or runbooks.
- Use Context7 before implementing with unfamiliar libraries, frameworks, SDKs,
  APIs, CLIs, or cloud services.
- Keep tests deterministic and meaningful. Mocks are allowed only as explicit
  unit-test doubles at architectural boundaries. They must not replace real
  integration, contract, migration, security, or E2E validation.
- Runtime mock modes, mock-only API paths, fake persistence, and auth bypasses
  are forbidden unless the code is isolated under test fixtures and cannot ship.

## Forbidden Shortcuts

Agents must not use commands, flags, annotations, or code changes that weaken
quality gates or hide failures. Forbidden examples include:

- `--no-cov`, `--cov-fail-under=0`, deleting coverage config, or lowering
  coverage thresholds to pass a run.
- `-DskipTests`, `-Dmaven.test.skip=true`, `skip-test`, `skip-tests`,
  `--skip-tests`, `--skipTests`, or any equivalent test bypass.
- `-Djacoco.skip=true`, `-Dcheckstyle.skip=true`, `-Dpmd.skip=true`,
  `-Dspotbugs.skip=true`, `-Denforcer.skip=true`, or equivalent quality-gate
  bypasses for completion evidence.
- `pytest -k 'not ...'`, `test.skip`, `describe.skip`, `it.skip`,
  `@Disabled`, `@Ignore`, or tag exclusions used to avoid fixing failures.
- `--no-verify`, bypassing hooks, deleting tests, weakening assertions, or
  replacing product behavior with superficial mocks to make checks pass.

If a gate is genuinely impossible to run because of a missing external service,
document the blocker, keep the strongest local gate enabled, and add or update
an issue that tracks the real recovery. Do not present that as a completed
verification.

## Validation Expectations

- Java: run the module's Maven verification path with tests, static analysis,
  and coverage enabled.
- Python: run `ruff check`, `ruff format --check`, `mypy --strict`, and pytest
  with the configured coverage gate enabled for completion evidence.
- Frontend: run lint, typecheck, format check, build, and relevant Playwright or
  component tests.
- GitOps/infra: render Helm/Kustomize, validate ArgoCD or compose contracts, and
  verify health/readiness assumptions.
- Database changes: include migrations, rollback/backfill thinking, and tests
  proving schema behavior.

## Completion Contract

Work is not complete until:

- acceptance criteria are met,
- relevant tests and quality gates pass without forbidden shortcuts,
- operational/documentation changes are updated in the same patch,
- beads status is closed or accurately updated,
- commits are pushed to the tracked remote,
- the handoff names exact validation commands and results.
