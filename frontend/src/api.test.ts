import { afterEach, describe, expect, it, vi } from "vitest";
import {
  getIncidentHistory,
  getRemediationBundle,
  listRcaReports,
  type IncidentHistory,
  type RcaReport,
  type RemediationBundle,
} from "./api";

afterEach(() => vi.unstubAllGlobals());

function respond(value: unknown) {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
    ok: true,
    json: async () => value,
  }));
}

function report(): RcaReport {
  return {
    id: 1,
    correlation_id: "incident/1",
    analysis_status: "completed",
    root_cause: "image_not_found",
    action: "image_rollback",
    incident_id: null,
    cluster_id: "local",
    symptom: null,
    severity: null,
    confidence: null,
    reason: null,
    evidence_ref: null,
    supporting_evidence: ["ErrImagePull"],
    missing_evidence: [],
    resource_kind: "Deployment",
    resource_name: "api",
    namespace: "demo",
    created_at: null,
  };
}

function bundle(): RemediationBundle {
  return {
    meta: { correlation_id: "incident/1", incident_id: null, cluster_id: "local", created_at: null },
    diagnosis: {
      root_cause: "image_not_found",
      confidence: null,
      supporting_evidence: ["ErrImagePull"],
      missing_evidence: [],
      selected_candidate_id: null,
    },
    remediation: {
      status: "selection_requested",
      selected_action_id: null,
      selected_by: null,
      evidence_ref: "evidence/1",
      candidates: [{
        action_id: "image_rollback",
        title: "이전 이미지 복원",
        description: "승인된 이미지로 복원합니다.",
        route: "safe_pr",
        risk_level: "low",
        blast_radius: "workload",
        approval_required: true,
        rollback_plan: "변경 커밋을 되돌립니다.",
        prerequisites: [],
        validation_checks: ["Pod Ready"],
        evidence_refs: ["evidence/1"],
      }],
    },
  };
}

// #2: API 응답 오류가 사건 누락이나 React 렌더링 예외로 바뀌지 않아야 한다.
describe("RCA response validation", () => {
  it("accepts nullable report fields", async () => {
    const value = report();
    respond({ items: [value] });

    await expect(listRcaReports()).resolves.toEqual([value]);
  });

  it.each([
    { supporting_evidence: [{}] },
    { missing_evidence: [null] },
    { confidence: {} },
    { reason: {} },
    { id: "1" },
  ])("rejects invalid reports instead of hiding them: %j", async (invalid) => {
    respond({ items: [report(), { ...report(), ...invalid }] });

    await expect(listRcaReports()).rejects.toThrow("RCA report response is invalid.");
  });

  it.each([bundle(), { ...bundle(), remediation: null }])("accepts a valid bundle: %j", async (value) => {
    respond(value);

    await expect(getRemediationBundle("incident/1")).resolves.toEqual(value);
  });

  it.each([
    { validation_checks: undefined },
    { validation_checks: [{}] },
    { description: {} },
    { approval_required: "true" },
    { prerequisites: null },
    { evidence_refs: [null] },
  ])("rejects invalid recovery candidates: %j", async (invalid) => {
    const value = bundle();
    Object.assign(value.remediation!.candidates[0], invalid);
    respond(value);

    await expect(getRemediationBundle("incident/1")).rejects.toThrow("Remediation bundle response is invalid.");
  });

  it("rejects non-text evidence before rendering", async () => {
    const value = bundle();
    respond({ ...value, diagnosis: { ...value.diagnosis, supporting_evidence: [{}] } });

    await expect(getRemediationBundle("incident/1")).rejects.toThrow("Remediation bundle response is invalid.");
  });
});

function historyBody(): IncidentHistory {
  return {
    correlation_id: "incident/1",
    cluster_id: "local",
    window: { from_ms: 1, to_ms: 2 },
    events: [],
    resources: [],
    evidence: [],
    completeness: { status: "complete", coverage: [], reasons: [] },
    truncated: false,
  };
}

describe("getIncidentHistory", () => {
  it("getIncidentHistory_valid_body_requests_api_path_with_encoded_id", async () => {
    respond(historyBody());
    await expect(getIncidentHistory("incident/1")).resolves.toEqual(historyBody());
    expect(vi.mocked(fetch).mock.calls[0][0]).toBe("/api/incidents/incident%2F1/history");
  });

  it("getIncidentHistory_truncated_but_complete_is_rejected", async () => {
    respond({ ...historyBody(), truncated: true });
    await expect(getIncidentHistory("incident/1")).rejects.toThrow("invalid");
  });

  it("getIncidentHistory_complete_with_reasons_is_rejected", async () => {
    const body = historyBody();
    body.completeness.reasons = ["collection_gap"];
    respond(body);
    await expect(getIncidentHistory("incident/1")).rejects.toThrow("invalid");
  });

  it("getIncidentHistory_unavailable_with_events_is_rejected", async () => {
    const body = historyBody();
    body.completeness = { status: "unavailable", coverage: [], reasons: ["no_observation"] };
    body.events = [{
      event_id: "e", source: "incident", source_key: "k", event_type: "incident", severity: "info",
      occurred_at: "2023-11-14T22:00:00Z", title: "t", metadata: {},
      subject: { kind: "incident", incident_id: "i", correlation_id: null },
    }];
    respond(body);
    await expect(getIncidentHistory("incident/1")).rejects.toThrow("invalid");
  });

  it("getIncidentHistory_deleted_at_without_bound_is_rejected", async () => {
    const body = historyBody();
    body.resources = [{
      subject: { kind: "inventory_locator", inventory_key: "k", resource_kind: "Pod", namespace: null, name: "p" },
      lifecycle: "deleted", first_observed_at: null, last_observed_at: null,
      deleted_at: "2023-11-14T22:00:00Z", deleted_at_bound: null,
    }];
    respond(body);
    await expect(getIncidentHistory("incident/1")).rejects.toThrow("invalid");
  });

  it("getIncidentHistory_complete_with_coverage_gap_is_rejected", async () => {
    const body = historyBody();
    body.completeness.coverage = [{ source: "inventory", from_ms: 1, to_ms: 2, reason: "collection_gap" }];
    respond(body);
    await expect(getIncidentHistory("incident/1")).rejects.toThrow("invalid");
  });

  it("getIncidentHistory_partial_with_coverage_gap_is_accepted", async () => {
    const body = historyBody();
    body.completeness = {
      status: "partial",
      coverage: [{ source: "inventory", from_ms: 1, to_ms: 2, reason: "collection_gap" }],
      reasons: ["collection_gap"],
    };
    respond(body);
    await expect(getIncidentHistory("incident/1")).resolves.toEqual(body);
  });

  it.each([
    ["window_beyond_date_range", { from_ms: 1, to_ms: 8_640_000_000_000_001 }],
    ["window_zero_width", { from_ms: 5, to_ms: 5 }],
    ["window_reversed", { from_ms: 9, to_ms: 5 }],
    ["window_negative_start", { from_ms: -1, to_ms: 5 }],
    ["window_fractional", { from_ms: 1.5, to_ms: 5 }],
  ])("getIncidentHistory_%s_is_rejected", async (_name, window) => {
    respond({ ...historyBody(), window });
    await expect(getIncidentHistory("incident/1")).rejects.toThrow("invalid");
  });

  it("getIncidentHistory_coverage_beyond_date_range_is_rejected", async () => {
    const body = historyBody();
    body.completeness = {
      status: "partial",
      coverage: [{ source: "inventory", from_ms: 1, to_ms: 9e15, reason: "collection_gap" }],
      reasons: ["collection_gap"],
    };
    respond(body);
    await expect(getIncidentHistory("incident/1")).rejects.toThrow("invalid");
  });

  it.each([
    "2023-11-14T22:00:00+09:00",
    "2023-11-14 22:00:00Z",
    "2023-13-45T99:00:00Z",
    "not-a-time",
  ])("getIncidentHistory_non_utc_or_invalid_time_%s_is_rejected", async (occurredAt) => {
    const body = historyBody();
    body.events = [{
      event_id: "e", source: "incident", source_key: "k", event_type: "incident", severity: "info",
      occurred_at: occurredAt, title: "t", metadata: {},
      subject: { kind: "incident", incident_id: "i", correlation_id: null },
    }];
    respond(body);
    await expect(getIncidentHistory("incident/1")).rejects.toThrow("invalid");
  });

  it("getIncidentHistory_404_throws_http_error_with_status", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: false, status: 404 }));
    await expect(getIncidentHistory("incident/1")).rejects.toMatchObject({
      name: "HttpError",
      status: 404,
    });
  });
});
