import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import type { IncidentHistory as HistoryData } from "../api";
import { IncidentHistory, visibleMetadata } from "./IncidentHistory";

const UID_OLD = "11111111-aaaa-bbbb-cccc-000000000001";
const UID_NEW = "22222222-aaaa-bbbb-cccc-000000000002";

function resourceSubject(uid: string) {
  return {
    kind: "resource" as const,
    resource: { api_group: "apps", version: "v1", kind: "Deployment", namespace: "demo", name: "api", uid },
  };
}

function history(overrides: Partial<HistoryData> = {}): HistoryData {
  return {
    correlation_id: "incident-1",
    cluster_id: "local",
    window: { from_ms: 1_700_000_000_000, to_ms: 1_700_003_600_000 },
    events: [],
    resources: [],
    evidence: [],
    completeness: { status: "complete", coverage: [], reasons: [] },
    truncated: false,
    ...overrides,
  };
}

function render(value: HistoryData): string {
  return renderToStaticMarkup(<IncidentHistory state={{ kind: "ready", history: value }} />);
}

describe("IncidentHistory", () => {
  it("complete_empty_events_says_window_observed_without_recovery_claim", () => {
    const html = render(history());
    expect(html).toContain("창 전체 관측됨, 이벤트 없음");
    expect(html).not.toContain("복구됨");
  });

  it("partial_empty_events_is_not_shown_as_no_history", () => {
    const html = render(history({
      completeness: { status: "partial", coverage: [], reasons: ["source_unavailable"] },
    }));
    expect(html).toContain("불완전한 이력");
    expect(html).toContain("정상이나 복구로 판단하지 않습니다");
    expect(html).not.toContain("창 전체 관측됨");
  });

  it("unavailable_says_no_collection_records_not_empty_history", () => {
    const html = render(history({
      completeness: { status: "unavailable", coverage: [], reasons: ["no_observation"] },
    }));
    expect(html).toContain("수집 기록 없음");
    expect(html).toContain("이력이 없다는 뜻이 아닙니다");
  });

  it("truncated_is_reported_as_partial", () => {
    const html = render(history({
      truncated: true,
      completeness: { status: "partial", coverage: [], reasons: ["truncated"] },
    }));
    expect(html).toContain("가장 오래된 이벤트가 생략되었습니다");
  });

  it("same_name_with_different_uid_renders_two_resources", () => {
    const html = render(history({
      resources: [
        {
          subject: resourceSubject(UID_OLD), lifecycle: "deleted",
          first_observed_at: "2023-11-14T22:00:00Z", last_observed_at: "2023-11-14T22:10:00Z",
          deleted_at: "2023-11-14T22:10:00Z", deleted_at_bound: "observed",
        },
        {
          subject: resourceSubject(UID_NEW), lifecycle: "present",
          first_observed_at: "2023-11-14T22:11:00Z", last_observed_at: "2023-11-14T22:30:00Z",
          deleted_at: null, deleted_at_bound: null,
        },
      ],
    }));
    expect(html).toContain(`UID ${UID_OLD}`);
    expect(html).toContain(`UID ${UID_NEW}`);
    expect(html.match(/class="history-resource"/gu)).toHaveLength(2);
  });

  it("uids_sharing_a_long_prefix_stay_distinguishable", () => {
    const html = render(history({
      resources: [
        {
          subject: resourceSubject("uid-api-old"), lifecycle: "deleted",
          first_observed_at: null, last_observed_at: null,
          deleted_at: "2023-11-14T22:10:00Z", deleted_at_bound: "gap_bounded",
        },
        {
          subject: resourceSubject("uid-api-new"), lifecycle: "present",
          first_observed_at: null, last_observed_at: null, deleted_at: null, deleted_at_bound: null,
        },
      ],
    }));
    expect(html).toContain("UID uid-api-old");
    expect(html).toContain("UID uid-api-new");
  });

  it("locator_without_uid_is_marked_uncollected_and_gap_bound_deletion_is_uncertain", () => {
    const html = render(history({
      resources: [{
        subject: { kind: "inventory_locator", inventory_key: "k1", resource_kind: "Pod", namespace: "demo", name: "web" },
        lifecycle: "deleted", first_observed_at: null, last_observed_at: null,
        deleted_at: "2023-11-14T22:20:00Z", deleted_at_bound: "gap_bounded",
      }],
    }));
    expect(html).toContain("UID 미수집");
    expect(html).toContain("삭제 시각 불확정");
  });

  it("events_keep_server_order", () => {
    const base = { source: "kubernetes_event", source_key: "k", event_type: "k8s_event", severity: "info", metadata: {} };
    const html = render(history({
      events: [
        { ...base, event_id: "e1", occurred_at: "2023-11-14T22:00:00Z", title: "first", subject: resourceSubject(UID_OLD) },
        { ...base, event_id: "e2", occurred_at: "2023-11-14T22:05:00Z", title: "second", subject: resourceSubject(UID_NEW) },
      ],
    }));
    expect(html.indexOf("first")).toBeGreaterThan(-1);
    expect(html.indexOf("first")).toBeLessThan(html.indexOf("second"));
  });

  it("error_state_is_a_warning_not_an_empty_history", () => {
    const html = renderToStaticMarkup(
      <IncidentHistory state={{ kind: "error", message: "HTTP 500" }} />,
    );
    expect(html).toContain("이력을 불러오지 못했습니다");
    expect(html).toContain("history-warning");
  });

  it("metadata_shows_only_allowlisted_scalar_keys", () => {
    expect(visibleMetadata({ count: 3, secret: "x", status: "ok", nested: { a: 1 } }))
      .toEqual([["count", "3"], ["status", "ok"]]);
  });
});
