"""Incident history read contract.

The response joins retained Timeline evidence with an explicit completeness
statement.  A history that is not ``complete`` never means "nothing happened":
consumers must treat ``partial`` and ``unavailable`` as missing observation.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import Field, model_validator

from packages.contracts.modeling import StrictModel
from packages.contracts.timeline.models import (
    TimelineCoverage,
    TimelineEvent,
    TimelineInventoryLocatorSubject,
    TimelineResourceSubject,
    TimelineWindow,
)

INCIDENT_HISTORY_PATH = "/incidents/{correlation_id}/history"

HistoryLifecycle = Literal["present", "deleted", "unobserved"]
DeletedAtBound = Literal["observed", "gap_bounded"]
CompletenessStatus = Literal["complete", "partial", "unavailable"]
DeploymentStatus = Literal[
    "merged",
    "deploy_pending",
    "verification_pending",
    "deployed_symptom_persists",
    "recovered",
    "unknown",
]
CompletenessReason = Literal[
    "collection_gap",
    "retention_boundary",
    "partial_scope",
    "source_unavailable",
    "truncated",
    "no_observation",
]

HistoryResourceSubject = Annotated[
    TimelineResourceSubject | TimelineInventoryLocatorSubject,
    Field(discriminator="kind"),
]


class IncidentHistoryResource(StrictModel):
    """One resource identity.  A different UID is always a different resource."""

    subject: HistoryResourceSubject
    lifecycle: HistoryLifecycle
    first_observed_at: datetime | None = None
    last_observed_at: datetime | None = None
    deleted_at: datetime | None = None
    deleted_at_bound: DeletedAtBound | None = None

    @model_validator(mode="after")
    def validate_deletion_bound(self) -> IncidentHistoryResource:
        if self.lifecycle == "deleted":
            if self.deleted_at is None or self.deleted_at_bound is None:
                raise ValueError("deleted history resource requires a bounded deletion time")
        elif self.deleted_at is not None or self.deleted_at_bound is not None:
            raise ValueError("only a deleted history resource carries a deletion time")
        return self


class IncidentHistoryEvidence(StrictModel):
    """Evidence window reference; it is linked to events only by ``event_id``."""

    evidence_key: str = Field(min_length=1)
    source_id: str = Field(min_length=1)
    window_start: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    observed_at: datetime


class IncidentHistoryCompleteness(StrictModel):
    status: CompletenessStatus
    coverage: tuple[TimelineCoverage, ...] = ()
    reasons: tuple[CompletenessReason, ...] = ()

    @model_validator(mode="after")
    def validate_status_reasons(self) -> IncidentHistoryCompleteness:
        if len(set(self.reasons)) != len(self.reasons):
            raise ValueError("completeness reasons must be unique")
        if self.status == "complete" and (self.reasons or self.coverage):
            raise ValueError("complete history cannot carry gaps or reasons")
        if self.status == "unavailable" and "no_observation" not in self.reasons:
            raise ValueError("unavailable history must say no observation was recorded")
        if self.status != "unavailable" and "no_observation" in self.reasons:
            raise ValueError("no_observation is only valid for unavailable history")
        if self.status == "partial" and not self.reasons:
            raise ValueError("partial history must name why it is partial")
        if "truncated" in self.reasons and self.status != "partial":
            raise ValueError("truncated history must be partial")
        return self


class IncidentDeploymentImageWindow(StrictModel):
    evidence_key: str
    observed_at: datetime
    status: Literal["healthy", "failing", "unknown"]
    reason: str
    ready_pod_count: int
    consecutive_healthy_windows: int
    confirmed_recovered: bool


class IncidentDeploymentTracking(StrictModel):
    status: DeploymentStatus
    reason: str
    merge_commit_sha: str
    merged_at: datetime
    repo_ref: str | None = None
    manifest_path: str | None = None
    namespace: str | None = None
    deployment_name: str | None = None
    git_source_status: str | None = None
    git_image: str | None = None
    git_observed_at: datetime | None = None
    argocd_status: str | None = None
    argocd_project: str | None = None
    argocd_application: str | None = None
    argocd_revision: str | None = None
    argocd_health: str | None = None
    argocd_checked_at: datetime | None = None
    image_windows: tuple[IncidentDeploymentImageWindow, ...] = ()


class IncidentHistoryResponse(StrictModel):
    correlation_id: str = Field(min_length=1)
    cluster_id: str = Field(min_length=1)
    window: TimelineWindow
    events: tuple[TimelineEvent, ...] = ()
    resources: tuple[IncidentHistoryResource, ...] = ()
    evidence: tuple[IncidentHistoryEvidence, ...] = ()
    deployment: IncidentDeploymentTracking | None = None
    completeness: IncidentHistoryCompleteness
    truncated: bool = False

    @model_validator(mode="after")
    def validate_truncation_is_not_complete(self) -> IncidentHistoryResponse:
        if self.truncated and "truncated" not in self.completeness.reasons:
            raise ValueError("truncated history must report the truncated reason")
        if self.completeness.status == "unavailable" and self.events:
            raise ValueError("unavailable history cannot contain events")
        return self
