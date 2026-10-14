from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import httpx
from providers import (
    KubernetesSnapshotProvider,
    TelemetryProvider,
)
from providers.base import ProviderResult
from queries import (
    TelemetryQueryDefinition,
    TelemetryQueryRegistry,
    TelemetrySource,
)
from span import get_tracer
from telemetry_registry import telemetry

from packages.config.logs import CONTEXT_KEY, get_logger
from packages.contracts.event_bus.interfaces import JsonObject

TRACER = get_tracer("target-cluster-agent.evidence")
LOGGER = get_logger(__name__)
STRICT_FAILURE_POLICY = "strict"
COLLECTION_STATUS_KEY = "collection_status"
COLLECTION_PROVIDERS_KEY = "providers"
COLLECTION_COMPLETED = "completed"
COLLECTION_PARTIAL = "partial"
COLLECTION_UNAVAILABLE = "unavailable"
COLLECTION_NOT_QUERIED = "not_queried"
PROVIDER_QUERY_FAILED_REASON = "provider_query_failed"
NO_QUERIES_CONFIGURED_REASON = "no_queries_configured"

__all__ = [
    "EvidenceCollector",
    "KubernetesSnapshotProvider",
    "TelemetryProvider",
]


@dataclass(frozen=True)
class ProviderCollectionOutcome:
    """One provider payload plus non-secret collection health metadata."""

    payload: ProviderResult
    status: JsonObject


class EvidenceCollector:
    def __init__(
        self,
        providers: Iterable[TelemetryProvider],
        registry: TelemetryQueryRegistry | None = None,
    ) -> None:
        self.providers = {provider.evidence_key: provider for provider in providers}
        self.registry = registry or TelemetryQueryRegistry()
        self.refresh_provider_queries()

    def register_query(self, definition: TelemetryQueryDefinition) -> TelemetryQueryDefinition:
        registered = self.registry.register(definition)
        self.refresh_provider_queries()
        return registered

    def replace_queries(
        self,
        source: TelemetrySource,
        definitions: tuple[TelemetryQueryDefinition, ...],
    ) -> tuple[TelemetryQueryDefinition, ...]:
        registered = self.registry.replace_source(source, definitions)
        self.refresh_provider_queries()
        return registered

    def replace_provider(self, provider: TelemetryProvider) -> TelemetryProvider:
        """Atomically replace one runtime provider while preserving registry-owned queries."""
        definitions = self.registry.for_source(provider.source)
        queries = tuple(definition.to_provider_query() for definition in definitions)
        provider.queries = queries
        self.providers[provider.evidence_key] = provider
        return provider

    def remove_provider(self, evidence_key: str) -> TelemetryProvider | None:
        """Remove a runtime-only provider without discarding its registered query policy."""
        return self.providers.pop(evidence_key, None)

    def refresh_provider_queries(self) -> None:
        for provider in self.providers.values():
            definitions = self.registry.for_source(provider.source)
            provider.queries = tuple(definition.to_provider_query() for definition in definitions)

    # 구성된 전체 provider 로 telemetry evidence payload 생성.
    async def collect_evidence(self) -> JsonObject:
        return await self.collect()

    # 지정 provider(미지정 시 전체)로 evidence payload 생성.
    async def collect(self, *evidence_keys: str) -> JsonObject:
        selected_keys = self._select_provider_keys(evidence_keys)
        statuses: dict[str, JsonObject] = {}
        with TRACER.start_payload_span(
            "evidence.collect",
            namespace="evidence",
            expected_fields=selected_keys,
        ) as evidence:
            for evidence_key in selected_keys:
                outcome = await self._collect_provider_outcome(evidence_key)
                evidence[evidence_key] = outcome.payload
                statuses[evidence_key] = outcome.status
            evidence[COLLECTION_STATUS_KEY] = collection_status_payload(statuses)
            return evidence

    async def collect_query_policy(
        self,
        evidence_key: str,
        definitions: tuple[TelemetryQueryDefinition, ...],
        *,
        failure_policy: str = "allow_partial",
    ) -> JsonObject:
        provider = self.providers[evidence_key]
        queries = tuple(definition.to_provider_query() for definition in definitions)
        outcome = await self._collect_with_queries_outcome(
            provider,
            queries,
            propagate_errors=failure_policy == STRICT_FAILURE_POLICY,
        )
        return {
            evidence_key: outcome.payload,
            COLLECTION_STATUS_KEY: collection_status_payload({evidence_key: outcome.status}),
        }

    def _select_provider_keys(self, requested_keys: tuple[str, ...]) -> tuple[str, ...]:
        selected_keys = requested_keys or tuple(self.providers)
        unknown_keys = tuple(key for key in selected_keys if key not in self.providers)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            available = ", ".join(self.providers) or "<none>"
            raise ValueError(f"unknown evidence provider key(s): {unknown}; available: {available}")
        return selected_keys

    async def _collect_provider(self, evidence_key: str) -> ProviderResult:
        return await self._collect_with_provider(self.providers[evidence_key])

    async def _collect_provider_outcome(
        self,
        evidence_key: str,
    ) -> ProviderCollectionOutcome:
        provider = self.providers[evidence_key]
        return await self._collect_with_queries_outcome(provider, provider.queries)

    async def run_query(self, definition: TelemetryQueryDefinition) -> ProviderResult:
        provider = self._provider_for_source(definition.source)
        return await self._collect_with_queries(provider, (definition.to_provider_query(),))

    def _provider_for_source(self, source: TelemetrySource) -> TelemetryProvider:
        return self.providers[telemetry.spec(source).evidence_key]

    # 공통 collect -> query -> normalize -> package 흐름을 provider 로 실행.
    async def _collect_with_provider(self, provider: TelemetryProvider) -> ProviderResult:
        return await self._collect_with_queries(provider, provider.queries)

    async def _collect_with_queries(
        self,
        provider: TelemetryProvider,
        queries: tuple[object, ...],
        *,
        propagate_errors: bool = False,
    ) -> ProviderResult:
        outcome = await self._collect_with_queries_outcome(
            provider,
            queries,
            propagate_errors=propagate_errors,
        )
        return outcome.payload

    async def _collect_with_queries_outcome(
        self,
        provider: TelemetryProvider,
        queries: tuple[object, ...],
        *,
        propagate_errors: bool = False,
    ) -> ProviderCollectionOutcome:
        with TRACER.start_as_current_span(provider.span_name) as span:
            span.count(provider.query_count_attribute, queries)
            query_count = len(queries)
            completed_query_count = 0
            failed_query_count = 0
            if query_count == 0:
                return ProviderCollectionOutcome(
                    payload=provider.build_response(provider.empty_results()),
                    status=provider_collection_status(
                        provider,
                        state=COLLECTION_NOT_QUERIED,
                        query_count=0,
                        completed_query_count=0,
                        failed_query_count=0,
                        reason_codes=(NO_QUERIES_CONFIGURED_REASON,),
                    ),
                )
            try:
                async with httpx.AsyncClient(timeout=provider.timeout_seconds) as client:
                    results = provider.empty_results()

                    for telemetry_query in queries:
                        try:
                            payload = await provider.query(client, telemetry_query)
                            provider.append_result(results, telemetry_query, payload)
                            completed_query_count += 1
                        except Exception as exc:
                            if propagate_errors:
                                raise
                            failed_query_count += 1
                            span.error(exc)
                            LOGGER.warning(
                                provider.failure_message,
                                extra={
                                    CONTEXT_KEY: {
                                        "source": provider.source,
                                        "query_name": telemetry_query_name(telemetry_query),
                                    }
                                },
                                exc_info=exc,
                            )

                span.count(provider.result_count_attribute, results)
                span.flag(f"{provider.source}.fallback_used", failed_query_count > 0)
                state = (
                    COLLECTION_COMPLETED
                    if failed_query_count == 0
                    else COLLECTION_PARTIAL
                    if completed_query_count > 0
                    else COLLECTION_UNAVAILABLE
                )
                reason_codes = (PROVIDER_QUERY_FAILED_REASON,) if failed_query_count > 0 else ()
                payload = provider.build_response(results)
                state = payload_collection_state(payload, state)
                return ProviderCollectionOutcome(
                    payload=payload,
                    status=provider_collection_status(
                        provider,
                        state=state,
                        query_count=query_count,
                        completed_query_count=completed_query_count,
                        failed_query_count=failed_query_count,
                        reason_codes=reason_codes,
                    ),
                )

            except Exception as exc:
                span.error(exc)
                span.flag(f"{provider.source}.fallback_used", not propagate_errors)
                LOGGER.warning(
                    provider.failure_message,
                    extra={CONTEXT_KEY: {"source": provider.source}},
                    exc_info=exc,
                )
                if propagate_errors:
                    raise
                return ProviderCollectionOutcome(
                    payload=provider.build_response(provider.empty_results()),
                    status=provider_collection_status(
                        provider,
                        state=COLLECTION_UNAVAILABLE,
                        query_count=query_count,
                        completed_query_count=completed_query_count,
                        failed_query_count=max(
                            failed_query_count,
                            query_count - completed_query_count,
                        ),
                        reason_codes=(PROVIDER_QUERY_FAILED_REASON,),
                    ),
                )


def payload_collection_state(payload: ProviderResult, state: str) -> str:
    if not isinstance(payload, dict) or state == COLLECTION_UNAVAILABLE:
        return state
    statuses = payload.get("provider_status", {})
    values = (
        [value.get("status") for value in statuses.values() if isinstance(value, dict)]
        if isinstance(statuses, dict)
        else []
    )
    if values and all(value == COLLECTION_UNAVAILABLE for value in values):
        return COLLECTION_UNAVAILABLE
    limits = payload.get("collection_limits", {})
    is_truncated = isinstance(limits, dict) and limits.get("truncated") is True
    if is_truncated or any(value not in {"success", COLLECTION_COMPLETED} for value in values):
        return COLLECTION_PARTIAL
    return state


def provider_collection_status(
    provider: TelemetryProvider,
    *,
    state: str,
    query_count: int,
    completed_query_count: int,
    failed_query_count: int,
    reason_codes: tuple[str, ...],
) -> JsonObject:
    """Build bounded status metadata without leaking exception or endpoint details."""
    return {
        "status": state,
        "source": provider.source,
        "query_count": query_count,
        "completed_query_count": completed_query_count,
        "failed_query_count": failed_query_count,
        "reason_codes": list(reason_codes),
    }


def collection_status_payload(statuses: dict[str, JsonObject]) -> JsonObject:
    """Summarize per-provider health while preserving the existing evidence buckets."""
    completed = sorted(
        key for key, status in statuses.items() if status.get("status") == COLLECTION_COMPLETED
    )
    partial = sorted(
        key for key, status in statuses.items() if status.get("status") == COLLECTION_PARTIAL
    )
    failed = sorted(
        key
        for key, status in statuses.items()
        if status.get("status") in {COLLECTION_UNAVAILABLE, COLLECTION_NOT_QUERIED}
    )
    return {
        "complete": len(completed) == len(statuses),
        "completed_providers": completed,
        "partial_providers": partial,
        "failed_providers": failed,
        "pending_providers": [],
        COLLECTION_PROVIDERS_KEY: statuses,
    }


def telemetry_query_name(telemetry_query: object) -> str:
    """Return the stable name of one provider query for logs."""
    for attribute in ("query_name", "metric_name"):
        value = getattr(telemetry_query, attribute, None)
        if isinstance(value, str) and value:
            return value
    return type(telemetry_query).__name__
