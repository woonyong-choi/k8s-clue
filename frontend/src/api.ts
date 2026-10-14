export interface RcaReport {
  id: number;
  correlation_id: string;
  analysis_status: "completed" | "blocked";
  root_cause: string;
  action: string;
  incident_id: string | null;
  cluster_id: string | null;
  symptom: string | null;
  severity: string | null;
  confidence: number | null;
  reason: string | null;
  evidence_ref: string | null;
  supporting_evidence: string[];
  missing_evidence: string[];
  resource_kind: string | null;
  resource_name: string | null;
  namespace: string | null;
  created_at: string | null;
}

export interface RecoveryCandidate {
  action_id: string;
  title: string;
  description: string;
  route: string;
  risk_level: string;
  blast_radius: string;
  approval_required: boolean;
  rollback_plan: string;
  prerequisites: string[];
  validation_checks: string[];
  evidence_refs: string[];
}

export interface RemediationBundle {
  meta: {
    correlation_id: string;
    incident_id: string | null;
    cluster_id: string;
    created_at: string | null;
  };
  diagnosis: {
    root_cause: string;
    confidence: number | null;
    supporting_evidence: string[];
    missing_evidence: string[];
    selected_candidate_id: string | null;
  };
  remediation: {
    status: string;
    selected_action_id: string | null;
    selected_by: string | null;
    candidates: RecoveryCandidate[];
    evidence_ref: string;
  } | null;
}

export type HistoryReason =
  | "collection_gap" | "retention_boundary" | "partial_scope"
  | "source_unavailable" | "truncated" | "no_observation";

export interface HistoryResourceRef {
  api_group: string;
  version: string;
  kind: string;
  namespace: string | null;
  name: string;
  uid: string;
}

export type HistorySubject =
  | { kind: "resource"; resource: HistoryResourceRef }
  | {
    kind: "inventory_locator";
    inventory_key: string;
    resource_kind: string;
    namespace: string | null;
    name: string;
  }
  | { kind: "incident"; incident_id: string; correlation_id: string | null }
  | {
    kind: "application_workflow";
    application_id: string;
    binding_id: string;
    workflow_run_id: string;
  };

export interface HistoryEvent {
  event_id: string;
  source: string;
  source_key: string;
  event_type: string;
  severity: string;
  occurred_at: string;
  title: string;
  subject: HistorySubject;
  metadata: Record<string, unknown>;
}

export interface HistoryResource {
  subject: Extract<HistorySubject, { kind: "resource" | "inventory_locator" }>;
  lifecycle: "present" | "deleted" | "unobserved";
  first_observed_at: string | null;
  last_observed_at: string | null;
  deleted_at: string | null;
  deleted_at_bound: "observed" | "gap_bounded" | null;
}

export interface HistoryCoverage {
  source: string;
  from_ms: number;
  to_ms: number;
  reason: "collection_gap" | "retention_boundary" | "partial_scope";
}

export interface IncidentHistory {
  correlation_id: string;
  cluster_id: string;
  window: { from_ms: number; to_ms: number };
  events: HistoryEvent[];
  resources: HistoryResource[];
  evidence: {
    evidence_key: string;
    source_id: string;
    window_start: string;
    event_id: string;
    observed_at: string;
  }[];
  completeness: {
    status: "complete" | "partial" | "unavailable";
    coverage: HistoryCoverage[];
    reasons: HistoryReason[];
  };
  truncated: boolean;
}

export class HttpError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "HttpError";
    this.status = status;
  }
}

export async function listRcaReports(signal?: AbortSignal): Promise<RcaReport[]> {
  const value = await getJson("/api/rca-reports?limit=50", signal);
  if (!isRecord(value) || !Array.isArray(value.items) || !value.items.every(isRcaReport)) {
    throw new Error("RCA report response is invalid.");
  }
  return value.items;
}

export async function getRemediationBundle(
  correlationId: string,
  signal?: AbortSignal,
): Promise<RemediationBundle> {
  const value = await getJson(
    `/api/rca/bundles/${encodeURIComponent(correlationId)}`,
    signal,
  );
  if (!isRemediationBundle(value)) {
    throw new Error("Remediation bundle response is invalid.");
  }
  return value;
}

export async function getIncidentHistory(
  correlationId: string,
  signal?: AbortSignal,
): Promise<IncidentHistory> {
  const value = await getJson(
    `/api/incidents/${encodeURIComponent(correlationId)}/history`,
    signal,
  );
  if (!isIncidentHistory(value)) {
    throw new Error("Incident history response is invalid.");
  }
  return value;
}

async function getJson(path: string, signal?: AbortSignal): Promise<unknown> {
  const response = await fetch(path, {
    credentials: "include",
    headers: { Accept: "application/json" },
    signal,
  });
  if (!response.ok) {
    const hint = response.status === 401 ? "로그인이 필요합니다." : `HTTP ${response.status}`;
    throw new HttpError(hint, response.status);
  }
  return response.json() as Promise<unknown>;
}

function isRcaReport(value: unknown): value is RcaReport {
  if (!isRecord(value)) return false;
  return (
    typeof value.id === "number" && Number.isFinite(value.id)
    && typeof value.correlation_id === "string"
    && (value.analysis_status === "completed" || value.analysis_status === "blocked")
    && typeof value.root_cause === "string"
    && typeof value.action === "string"
    && isNullableNumber(value.confidence)
    && isStringList(value.supporting_evidence)
    && isStringList(value.missing_evidence)
    && [
      "incident_id", "cluster_id", "symptom", "severity", "reason", "evidence_ref",
      "resource_kind", "resource_name", "namespace", "created_at",
    ].every((field) => isNullableString(value[field]))
  );
}

function isRemediationBundle(value: unknown): value is RemediationBundle {
  if (!isRecord(value) || !isRecord(value.meta) || !isRecord(value.diagnosis)) {
    return false;
  }
  return (
    typeof value.meta.correlation_id === "string"
    && typeof value.meta.cluster_id === "string"
    && isNullableString(value.meta.incident_id)
    && isNullableString(value.meta.created_at)
    && typeof value.diagnosis.root_cause === "string"
    && isNullableNumber(value.diagnosis.confidence)
    && isNullableString(value.diagnosis.selected_candidate_id)
    && isStringList(value.diagnosis.supporting_evidence)
    && isStringList(value.diagnosis.missing_evidence)
    && (value.remediation === null || isRemediation(value.remediation))
  );
}

function isRemediation(value: unknown): boolean {
  return (
    isRecord(value)
    && typeof value.status === "string"
    && typeof value.evidence_ref === "string"
    && isNullableString(value.selected_action_id)
    && isNullableString(value.selected_by)
    && Array.isArray(value.candidates)
    && value.candidates.every(isRecoveryCandidate)
  );
}

function isRecoveryCandidate(value: unknown): value is RecoveryCandidate {
  if (!isRecord(value)) return false;
  return (
    ["action_id", "title", "description", "route", "risk_level", "blast_radius", "rollback_plan"]
      .every((field) => typeof value[field] === "string")
    && typeof value.approval_required === "boolean"
    && isStringList(value.prerequisites)
    && isStringList(value.validation_checks)
    && isStringList(value.evidence_refs)
  );
}

const HISTORY_REASONS = [
  "collection_gap", "retention_boundary", "partial_scope",
  "source_unavailable", "truncated", "no_observation",
];
const COVERAGE_REASONS = ["collection_gap", "retention_boundary", "partial_scope"];

function isIncidentHistory(value: unknown): value is IncidentHistory {
  if (!isRecord(value) || !isRecord(value.window) || !isRecord(value.completeness)) {
    return false;
  }
  const { completeness } = value;
  const status = completeness.status;
  if (
    typeof value.correlation_id !== "string"
    || typeof value.cluster_id !== "string"
    || !isEpochRange(value.window.from_ms, value.window.to_ms)
    || typeof value.truncated !== "boolean"
    || !isList(value.events, isHistoryEvent)
    || !isList(value.resources, isHistoryResource)
    || !isList(value.evidence, isHistoryEvidence)
    || !["complete", "partial", "unavailable"].includes(status as string)
    || !isList(completeness.coverage, isHistoryCoverage)
    || !isList(completeness.reasons, isHistoryReason)
  ) {
    return false;
  }
  const reasons = completeness.reasons as string[];
  if (value.truncated && (status !== "partial" || !reasons.includes("truncated"))) return false;
  if (status === "complete" && (reasons.length > 0 || value.truncated)) return false;
  if (status === "complete" && completeness.coverage.length > 0) return false;
  if (status === "unavailable" && (value.events.length > 0 || !reasons.includes("no_observation"))) {
    return false;
  }
  return status === "unavailable" || !reasons.includes("no_observation");
}

function isHistoryEvent(value: unknown): value is HistoryEvent {
  return (
    isRecord(value)
    && ["event_id", "source", "source_key", "event_type", "severity", "title"]
      .every((field) => typeof value[field] === "string")
    && isUtcTimestamp(value.occurred_at)
    && isHistorySubject(value.subject)
    && isRecord(value.metadata)
  );
}

function isHistorySubject(value: unknown): value is HistorySubject {
  if (!isRecord(value)) return false;
  if (value.kind === "resource") {
    const resource = value.resource;
    return (
      isRecord(resource)
      && ["api_group", "version", "kind", "name", "uid"]
        .every((field) => typeof resource[field] === "string")
      && isNullableString(resource.namespace)
    );
  }
  if (value.kind === "inventory_locator") {
    return (
      ["inventory_key", "resource_kind", "name"].every((field) => typeof value[field] === "string")
      && isNullableString(value.namespace)
    );
  }
  if (value.kind === "incident") {
    return typeof value.incident_id === "string" && isNullableString(value.correlation_id);
  }
  return (
    value.kind === "application_workflow"
    && ["application_id", "binding_id", "workflow_run_id"]
      .every((field) => typeof value[field] === "string")
  );
}

function isHistoryResource(value: unknown): value is HistoryResource {
  if (!isRecord(value) || !isHistorySubject(value.subject)) return false;
  if (value.subject.kind !== "resource" && value.subject.kind !== "inventory_locator") {
    return false;
  }
  const bound = value.deleted_at_bound;
  return (
    ["present", "deleted", "unobserved"].includes(value.lifecycle as string)
    && isNullableUtcTimestamp(value.first_observed_at)
    && isNullableUtcTimestamp(value.last_observed_at)
    && isNullableUtcTimestamp(value.deleted_at)
    && (bound === null || bound === "observed" || bound === "gap_bounded")
    && (value.deleted_at === null) === (bound === null)
  );
}

function isHistoryCoverage(value: unknown): value is HistoryCoverage {
  return (
    isRecord(value)
    && typeof value.source === "string"
    && isEpochRange(value.from_ms, value.to_ms)
    && COVERAGE_REASONS.includes(value.reason as string)
  );
}

function isHistoryEvidence(value: unknown): boolean {
  return (
    isRecord(value)
    && ["evidence_key", "source_id", "window_start", "event_id"]
      .every((field) => typeof value[field] === "string")
    && isUtcTimestamp(value.observed_at)
  );
}

function isHistoryReason(value: unknown): value is HistoryReason {
  return typeof value === "string" && HISTORY_REASONS.includes(value);
}

function isList<T>(value: unknown, guard: (item: unknown) => item is T): value is T[];
function isList(value: unknown, guard: (item: unknown) => boolean): boolean;
function isList(value: unknown, guard: (item: unknown) => boolean): boolean {
  return Array.isArray(value) && value.every(guard);
}

const MAX_EPOCH_MS = 8_640_000_000_000_000;
const UTC_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/u;

/** 계약의 epoch ms 범위: 양의 너비를 갖고 Date가 표현할 수 있는 정수 구간. */
function isEpochRange(from: unknown, to: unknown): boolean {
  return (
    typeof from === "number" && typeof to === "number"
    && Number.isInteger(from) && Number.isInteger(to)
    && from >= 0 && to > from && to <= MAX_EPOCH_MS
  );
}

/** 계약의 시각은 UTC `Z`로 직렬화되며 실제로 해석 가능한 값이어야 한다. */
function isUtcTimestamp(value: unknown): value is string {
  return typeof value === "string" && UTC_TIMESTAMP.test(value) && !Number.isNaN(Date.parse(value));
}

function isNullableUtcTimestamp(value: unknown): value is string | null {
  return value === null || isUtcTimestamp(value);
}

function isNullableString(value: unknown): value is string | null {
  return value === null || typeof value === "string";
}

function isNullableNumber(value: unknown): value is number | null {
  return value === null || (typeof value === "number" && Number.isFinite(value));
}

function isStringList(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === "string");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
