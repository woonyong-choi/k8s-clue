"""Shared readers for persisted outbound-agent inventory evidence."""

from __future__ import annotations

from collections.abc import Mapping

# Persisted beside the agent summary so history never has to guess whether a cut was complete.
COMPLETE_CUT_PROOF_KEY = "timeline_proof"
# Set only on scoped evidence snapshots: they are not fleet truth (live_inventory false), yet their
# per-namespace collection_coverage may still prove that a missing row was deleted.
SCOPED_COVERAGE_AUTHORITY_KEY = "scoped_coverage_authority"
FIRST_OBSERVATION_BASIS = "first_observed"


def snapshot_source_summary(
    snapshot: Mapping[str, object] | None,
) -> Mapping[str, object] | None:
    """Return the agent-owned source summary from current or legacy snapshot rows.

    Persisted rows wrap the collected summary under ``summary.summary`` alongside
    health and usage projections. Older test fixtures and pre-envelope rows store
    the collected summary directly under ``summary``. Consumers use this one
    compatibility boundary instead of guessing the storage shape independently.
    """

    if not isinstance(snapshot, Mapping):
        return None
    envelope = snapshot.get("summary")
    if not isinstance(envelope, Mapping):
        return None
    source = envelope.get("summary")
    return source if isinstance(source, Mapping) else envelope
