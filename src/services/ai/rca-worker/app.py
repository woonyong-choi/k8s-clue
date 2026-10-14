"""rca-worker — evidence.bundle.built -> rca.candidates.planned -> rca.candidates.evaluated -> rca.completed."""

from __future__ import annotations

from collections.abc import AsyncIterator

from domains.rca.events import (
    EvidenceBundleBuiltBody,
    RcaAnalysisBlockedBody,
    RcaCandidatesEvaluatedBody,
    RcaCandidatesPlannedBody,
    RcaCompletedBody,
)
from packages.contracts.event_bus.bodies import EventBody
from packages.contracts.stores import RcaBacklogStore, RcaStore
from packages.runtime.app import App, EventContext
from services.ai.agent.pipeline import (
    CauseEvaluationPipeline,
    CausePlanningPipeline,
    RcaCompletionPipeline,
)

app = App("rca-worker")
planning_pipeline = CausePlanningPipeline()
evaluation_pipeline = CauseEvaluationPipeline()
pipeline = RcaCompletionPipeline()
BACKLOG_RULE_RESOLVED_REASON = "matching RCA rule is now available"


@app.on(EvidenceBundleBuiltBody, consumer="plan-worker")
async def on_evidence_bundle_built(
    evt: EvidenceBundleBuiltBody,
    ctx: EventContext[RcaBacklogStore],
) -> AsyncIterator[EventBody]:
    bodies = planning_pipeline.plan_bodies(evt)
    planned = next((body for body in bodies if isinstance(body, RcaCandidatesPlannedBody)), None)
    if (
        planned is not None
        and planned.rule_missing is None
        and evt.incident is not None
        and ctx.db is not None
    ):
        # 증상에 맞는 RCA rule 이 생기면 과거 missing-rule backlog 를 자동 해소한다.
        await ctx.db.resolve_rca_backlog_item_for_rule(
            evt.evidence.workspace_id,
            evt.incident.symptom,
            BACKLOG_RULE_RESOLVED_REASON,
        )
    for body in bodies:
        yield body


@app.on(RcaCandidatesPlannedBody, consumer="analyze-worker")
async def on_candidates_planned(
    evt: RcaCandidatesPlannedBody,
) -> AsyncIterator[EventBody]:
    yield evaluation_pipeline.evaluate_body(evt)


@app.on(RcaCandidatesEvaluatedBody, consumer="rca-worker")
async def on_candidates_evaluated(
    evt: RcaCandidatesEvaluatedBody,
    ctx: EventContext[RcaStore],
) -> AsyncIterator[EventBody]:
    result = pipeline.complete_body(evt)
    if isinstance(result, RcaCompletedBody):
        # correlation_id is the public lookup key used by the incident detail UI.
        # Skipping persistence because a similar resource report exists leaves the
        # newly emitted incident with no report at all. Incident de-duplication belongs
        # at the incident projection boundary; every emitted correlation must retain
        # its own evidence-backed report.
        report_body = result.to_body()
        report_body["analysis_status"] = "completed"
        await ctx.db.save_rca_report(
            ctx.correlation_id,
            result.workspace_id,
            result.root_cause,
            result.action,
            report_body,
        )
    elif isinstance(result, RcaAnalysisBlockedBody):
        # A blocked analysis is still a durable RCA outcome. Persist its ranked
        # candidates and evidence trail so the issue detail can explain why the
        # cause was not finalized and what evidence is still required.
        report_body = result.to_body()
        report_body["analysis_status"] = "blocked"
        await ctx.db.save_rca_report(
            ctx.correlation_id,
            result.workspace_id,
            "insufficient_evidence",
            "추가 근거 수집 후 RCA 재분석",
            report_body,
        )
    yield result


if __name__ == "__main__":
    app.run()
