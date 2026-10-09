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

async function getJson(path: string, signal?: AbortSignal): Promise<unknown> {
  const response = await fetch(path, {
    credentials: "include",
    headers: { Accept: "application/json" },
    signal,
  });
  if (!response.ok) {
    const hint = response.status === 401 ? "로그인이 필요합니다." : `HTTP ${response.status}`;
    throw new Error(hint);
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
