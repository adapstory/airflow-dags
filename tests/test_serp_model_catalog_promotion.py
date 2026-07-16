"""Focused D17/D19 canonical evaluation-authority regression entrypoint."""

from test_serp_evaluation_release_v3 import (  # noqa: F401
    test_d17_consumes_ci_v5_bundle_and_seals_governed_v5_promotion,
    test_d17_rejects_blocked_activation_and_missing_treatment,
    test_d17_rejects_ci_v3_bundle_without_compatibility_fallback,
    test_d17_rejects_component_evidence_tampering,
    test_d17_rejects_noncanonical_release_bytes_even_when_digest_matches,
    test_d17_uses_a_read_only_session_for_the_exact_ci_release_operation,
    test_d19_builds_scoreless_reference_only_paired_request_v5,
    test_d19_rejects_duplicate_promotion_member_even_when_digest_matches,
    test_d19_rejects_inline_selection_or_scoring_fields,
    test_d19_rejects_noncanonical_lifecycle_number_even_when_digest_matches,
    test_d19_rereads_the_v5_promotion_and_both_release_manifests,
)
