from __future__ import annotations

from pathlib import Path

import pytest
from conftest import load_service

from packages.runtime import controller
from packages.runtime.discovery import discover_services

ROOT = Path(__file__).resolve().parents[1]
MERGED_AWAY = ("analyze-worker", "plan-worker", "select-worker", "dispatch-worker")
EXPECTED_SUBJECTS = {
    "ai/rca-worker": {
        "evidence.bundle.built",
        "rca.candidates.planned",
        "rca.candidates.evaluated",
    },
    "ai/recovery-worker": {
        "rca.completed",
        "recovery.planned",
        "recovery.action_selected",
    },
}


def test_merged_workers_are_removed_from_discovery_and_controller() -> None:
    names = {service.name for service in discover_services(ROOT)}

    assert names.isdisjoint(MERGED_AWAY)
    assert names.isdisjoint(set(MERGED_AWAY) & controller.CORE_CONTROLLER_SERVICE_NAMES)
    plan = controller.build_composition_plan(ROOT, event_bus_mode="inprocess")
    assert {service.name for service in plan.controller_services} == (
        controller.CORE_CONTROLLER_SERVICE_NAMES
    )


def test_independent_responsibilities_stay_separate() -> None:
    assert {"evidence-worker", "incident-worker", "rca-feedback-worker", "ai-diff-worker"} <= (
        controller.CORE_CONTROLLER_SERVICE_NAMES
    )


@pytest.mark.parametrize("service", sorted(EXPECTED_SUBJECTS))
def test_merged_worker_keeps_every_original_subscription(service: str) -> None:
    app = load_service(service).app
    spec = app.handler_spec()

    assert set(spec.subjects) == EXPECTED_SUBJECTS[service]
    assert len(spec.subjects) == len(set(spec.subjects))


@pytest.mark.parametrize("service", sorted(EXPECTED_SUBJECTS))
def test_each_subject_keeps_its_own_durable_consumer(service: str) -> None:
    spec = load_service(service).app.handler_spec()

    durables = {spec.durable_for(subject) for subject in spec.subjects}

    assert len(durables) == len(spec.subjects)
    assert all(durable.startswith(spec.service_name) for durable in durables)


def test_merged_subjects_are_not_consumed_by_other_workers() -> None:
    merged = {name.split("/")[1] for name in EXPECTED_SUBJECTS}
    owned = set().union(*EXPECTED_SUBJECTS.values())
    for service in discover_services(ROOT):
        if service.kind != "worker" or service.name in merged:
            continue
        module = load_service(service.path.parent.relative_to("src/services").as_posix())
        assert owned.isdisjoint(module.app.handler_spec().subjects), service.name
