"""recovery-worker — rca.completed -> recovery.planned -> recovery.action_selected -> command or PR request."""

from __future__ import annotations

from collections.abc import AsyncIterator

from domains.rca.events import (
    RcaCompletedBody,
    RecoveryActionSelectedBody,
    RecoveryPlannedBody,
    RecoverySelectionRequestedBody,
)
from packages.contracts.event_bus.bodies import EventBody
from packages.contracts.stores import RecoveryPlanStore
from packages.runtime.app import App, EventContext
from services.ai.agent.pipeline import RecoveryPlanningPipeline
from services.ai.agent.recovery.authority import DatabaseGitOpsAuthorityReadPort
from services.ai.agent.recovery.dispatch import RecoveryDispatcher
from services.ai.agent.recovery.select import RecoverySelector

app = App("recovery-worker")
pipeline = RecoveryPlanningPipeline()
selector = RecoverySelector()
dispatcher = RecoveryDispatcher()


@app.on(RcaCompletedBody, consumer="recovery-worker")
async def on_rca_completed(evt: RcaCompletedBody) -> AsyncIterator[EventBody]:
    yield pipeline.plan_body(evt)


@app.on(RecoveryPlannedBody, consumer="select-worker")
async def on_recovery_planned(
    evt: RecoveryPlannedBody,
    ctx: EventContext[RecoveryPlanStore],
) -> AsyncIterator[EventBody]:
    body = selector.select_body(evt)
    if ctx.db is not None:
        if isinstance(body, RecoverySelectionRequestedBody):
            await ctx.db.upsert_recovery_plan(
                ctx.correlation_id,
                body.workspace_id,
                body.plan.to_body(),
                status="selection_requested",
            )
        elif isinstance(body, RecoveryActionSelectedBody):
            await ctx.db.upsert_recovery_plan(
                ctx.correlation_id,
                body.workspace_id,
                body.plan.to_body(),
                status="selected",
                selected_action_id=body.selected.action_id,
                selected_by=body.selected_by,
            )
    yield body


@app.on(RecoveryActionSelectedBody, consumer="dispatch-worker")
async def on_recovery_action_selected(
    evt: RecoveryActionSelectedBody,
    ctx: EventContext[object],
) -> AsyncIterator[EventBody]:
    authority = DatabaseGitOpsAuthorityReadPort(ctx.db) if ctx.db is not None else None
    yield await dispatcher.dispatch_body(
        evt,
        authority=authority,
        correlation_id=ctx.correlation_id,
    )


if __name__ == "__main__":
    app.run()
