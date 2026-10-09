import { afterEach, describe, expect, it, vi } from "vitest";
import { getRemediationBundle, listRcaReports, type RcaReport, type RemediationBundle } from "./api";

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
