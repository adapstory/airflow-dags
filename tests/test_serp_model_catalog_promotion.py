"""Focused D17/D19 EvaluationRelease/v2 regression entrypoint."""

from test_serp_evaluation_release_v2 import (  # noqa: F401
    test_d17_consumes_ci_v3_bundle_and_seals_governed_v2_promotion,
    test_d17_rejects_blocked_activation_and_missing_treatment,
    test_d17_rejects_component_evidence_tampering,
    test_d17_uses_a_read_only_session_for_the_exact_ci_release_operation,
    test_d19_builds_scoreless_reference_only_paired_request_v2,
    test_d19_rejects_inline_selection_or_scoring_fields,
    test_d19_rereads_the_v2_promotion_and_both_release_manifests,
)
