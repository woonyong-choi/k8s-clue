import type {
  HistoryEvent,
  HistoryReason,
  HistoryResource,
  HistorySubject,
  IncidentHistory as IncidentHistoryData,
} from "../api";
import "../../tokens/tokens.css";
import "./incident-history.css";

export type HistoryState =
  | { kind: "loading" }
  | { kind: "error"; message: string }
  | { kind: "ready"; history: IncidentHistoryData };

export type HistoryNotice = {
  tone: "info" | "warning";
  title: string;
  detail: string;
};

const REASON_LABELS: Record<HistoryReason, string> = {
  collection_gap: "수집 공백 구간이 있습니다",
  retention_boundary: "보존 기간 경계로 앞쪽 기록이 없습니다",
  partial_scope: "일부 범위만 수집되었습니다",
  source_unavailable: "일부 출처가 제외되었습니다",
  truncated: "표시 한도를 넘어 가장 오래된 이벤트가 생략되었습니다",
  no_observation: "이 창에서 수집 기록이 전혀 없습니다",
};

const METADATA_KEYS = [
  "count", "last_occurrence_at", "collection_complete", "collection_truncated",
  "collection_reason", "capture_observed_at", "freshness_max_age_seconds",
  "source_snapshot_id", "revision_id", "version_id", "status", "severity",
];

const LIFECYCLE_LABELS = {
  present: "관측 시점에 존재",
  deleted: "삭제 관측",
  unobserved: "수집 기록 없음",
} as const;

export function IncidentHistory({ state }: { state: HistoryState }) {
  if (state.kind === "loading") {
    return <HistoryPanel title="보존된 이력을 불러오는 중입니다." />;
  }
  if (state.kind === "error") {
    return <HistoryPanel title="이력을 불러오지 못했습니다." detail={state.message} tone="warning" />;
  }

  const { history } = state;
  const notice = describeCompleteness(history);
  return (
    <section className="history" aria-label="사건 이력">
      <div className="card-head history-head">
        <span className="eyebrow">HISTORY</span>
        <span className="history-window">{formatWindow(history)}</span>
      </div>
      <div className={`history-notice history-${notice.tone}`} role="status">
        <strong className="history-notice-title">{notice.title}</strong>
        <span className="history-notice-detail">{notice.detail}</span>
      </div>
      {history.completeness.coverage.length > 0 ? (
        <ul className="history-list history-coverage" aria-label="수집 공백">
          {history.completeness.coverage.map((item) => (
            <li key={`${item.source}-${item.reason}-${item.from_ms}`}>
              {item.source} · {REASON_LABELS[item.reason]} · {formatMs(item.from_ms)} ~ {formatMs(item.to_ms)}
            </li>
          ))}
        </ul>
      ) : null}

      <h3 className="history-heading">리소스</h3>
      {history.resources.length > 0 ? (
        <ul className="history-list">
          {history.resources.map((resource, index) => (
            <ResourceItem key={`${resourceKey(resource.subject)}-${index}`} resource={resource} />
          ))}
        </ul>
      ) : <p className="history-empty">표시할 리소스 항목이 없습니다.</p>}

      <h3 className="history-heading">이벤트 (서버 시간 순서)</h3>
      {history.events.length > 0 ? (
        <ol className="history-list">
          {history.events.map((event) => <EventItem key={event.event_id} event={event} />)}
        </ol>
      ) : <p className="history-empty">이 창에 표시할 이벤트가 없습니다.</p>}
    </section>
  );
}

export function describeCompleteness(history: IncidentHistoryData): HistoryNotice {
  const { status, reasons } = history.completeness;
  if (status === "unavailable") {
    return {
      tone: "warning",
      title: "수집 기록 없음",
      detail: "이 창에서 이 클러스터의 수집 기록이 없습니다. 이력이 없다는 뜻이 아닙니다.",
    };
  }
  if (status === "partial") {
    const detail = reasons.map((reason) => REASON_LABELS[reason]).join(" · ");
    return { tone: "warning", title: "불완전한 이력", detail: `${detail}. 정상이나 복구로 판단하지 않습니다.` };
  }
  return {
    tone: "info",
    title: history.events.length > 0 ? "창 전체 관측됨" : "창 전체 관측됨, 이벤트 없음",
    detail: history.events.length > 0
      ? "요청한 창 전체에서 수집 공백이 없었습니다."
      : "요청한 창 전체를 관측했고 기록된 이벤트가 없습니다.",
  };
}

export function subjectLabel(subject: HistorySubject): string {
  switch (subject.kind) {
    case "resource": {
      const { kind, namespace, name } = subject.resource;
      return `${kind} ${namespace ? `${namespace}/` : ""}${name}`;
    }
    case "inventory_locator":
      return `${subject.resource_kind} ${subject.namespace ? `${subject.namespace}/` : ""}${subject.name}`;
    case "incident":
      return `Incident ${subject.incident_id}`;
    case "application_workflow":
      return `Workflow ${subject.application_id} / ${subject.workflow_run_id}`;
  }
}

export function visibleMetadata(metadata: Record<string, unknown>): [string, string][] {
  return METADATA_KEYS.flatMap((key) => {
    const value = metadata[key];
    return typeof value === "string" || typeof value === "number" || typeof value === "boolean"
      ? [[key, String(value)] as [string, string]]
      : [];
  });
}

function ResourceItem({ resource }: { resource: HistoryResource }) {
  const { subject } = resource;
  return (
    <li className="history-resource">
      <strong>{subjectLabel(subject)}</strong>
      <UidBadge subject={subject} />
      <span className="history-lifecycle">{LIFECYCLE_LABELS[resource.lifecycle]}</span>
      <small className="history-note">
        첫 관측 {resource.first_observed_at ?? "—"} · 마지막 관측 {resource.last_observed_at ?? "—"}
      </small>
      {resource.deleted_at ? (
        <small className="history-note">
          {resource.deleted_at_bound === "gap_bounded"
            ? `삭제 시각 불확정: 삭제를 직접 관측하지 못했고 ${resource.deleted_at}이 상한이다`
            : `삭제 관측 ${resource.deleted_at}`}
        </small>
      ) : null}
    </li>
  );
}

function EventItem({ event }: { event: HistoryEvent }) {
  const metadata = visibleMetadata(event.metadata);
  return (
    <li className="history-event">
      <time className="history-time" dateTime={event.occurred_at}>{event.occurred_at}</time>
      <div className="history-event-body">
        <strong>{event.title}</strong>
        <small className="history-note">
          {event.source} · {event.event_type} · {event.severity} · {subjectLabel(event.subject)}
        </small>
        <UidBadge subject={event.subject} />
        {metadata.length > 0 ? (
          <small className="history-note">{metadata.map(([key, value]) => `${key}=${value}`).join(" · ")}</small>
        ) : null}
      </div>
    </li>
  );
}

function UidBadge({ subject }: { subject: HistorySubject }) {
  if (subject.kind === "resource") {
    const { uid } = subject.resource;
    return <code className="history-uid">UID {uid}</code>;
  }
  if (subject.kind === "inventory_locator") {
    return <code className="history-uid history-uid-missing">UID 미수집</code>;
  }
  return null;
}

function HistoryPanel({ title, detail, tone }: { title: string; detail?: string; tone?: "warning" }) {
  return (
    <section className="history" aria-label="사건 이력">
      <span className="eyebrow">HISTORY</span>
      <div className={`history-notice history-${tone ?? "info"}`} role="status">
        <strong className="history-notice-title">{title}</strong>
        {detail ? <span className="history-notice-detail">{detail}</span> : null}
      </div>
    </section>
  );
}

function resourceKey(subject: HistoryResource["subject"]): string {
  return subject.kind === "resource" ? subject.resource.uid : subject.inventory_key;
}

function formatWindow(history: IncidentHistoryData): string {
  return `${formatMs(history.window.from_ms)} ~ ${formatMs(history.window.to_ms)}`;
}

function formatMs(ms: number): string {
  return new Date(ms).toISOString();
}
