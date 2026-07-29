# Adapstory Submodule Agent Contract

This is an Adapstory-owned module. Keep always-on context short; correctness,
security, maintainability, and fresh evidence remain mandatory.

## Read Order

1. This `AGENTS.md`.
2. Run `../scripts/agent_context.sh --summary --task-class <class>
   --module <module>` from the umbrella checkout.
3. Load only routed local README, contract, tests, ADR, or runbook sections.

## Mandatory Delivery Cycle

For non-trivial work: claim a Beads issue; confirm acceptance criteria; diagnose
Root Cause; write the failing test first; implement the canonical contract and
migrate owned callers; run the owning gate; finish with
`agent-finish-protocol`. Use Context7 for unfamiliar current libraries.
Runtime mock modes, mock-only paths, fake persistence, and auth bypasses must
remain isolated test fixtures and cannot ship.

## Forbidden Shortcuts

Never hide a failure with `--no-cov`, `-DskipTests`,
`-Dmaven.test.skip=true`, `-Djacoco.skip=true`, `--no-verify`, disabled tests,
weakened assertions, or superficial mocks. Report an external blocker exactly.

## Validation Expectations

Run the module's honest build, tests, static analysis, coverage, and relevant
integration/E2E checks. Infra changes require rendered config and runtime proof;
database changes require migration and schema-behavior evidence.

## Completion Contract

Completion requires acceptance criteria, synchronized docs/operations, fresh
checks, accurate Beads state, exact commits, tracked remote publication, and
the evidence layer actually reached.
