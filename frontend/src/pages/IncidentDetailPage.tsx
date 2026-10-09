import { useEffect, useState } from "react";
import { getRemediationBundle, type RemediationBundle } from "../api";
import type { AppRoute } from "../router";

const PIPELINE = [
  ["Evidence", "보존된 Kubernetes 상태"],
  ["RCA", "결정론적 원인 판정"],
  ["Patch", "허용 필드만 변경"],
  ["Draft PR", "고정된 base SHA"],
  ["Verify", "배포 후 증거 재수집"],
] as const;

export default function IncidentDetailPage({ route }: { route: AppRoute }) {
  const correlationId = route.kind === "incident" ? route.correlationId : "";
  const [bundle, setBundle] = useState<RemediationBundle | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!correlationId) return;
    const abort = new AbortController();
    getRemediationBundle(correlationId, abort.signal)
      .then(setBundle)
      .catch((reason: unknown) => {
        if (!abort.signal.aborted) {
          setError(reason instanceof Error ? reason.message : "RCA bundle을 불러오지 못했습니다.");
        }
      });
    return () => abort.abort();
  }, [correlationId]);

  if (error) return <DetailMessage title="Bundle을 불러오지 못했습니다." detail={error} />;
  if (!bundle) return <DetailMessage title="보존된 증거 묶음을 불러오는 중입니다." />;

  const selected = bundle.remediation?.candidates.find(
    (candidate) => candidate.action_id === bundle.remediation?.selected_action_id,
  ) ?? null;

  return (
    <section className="page detail-page">
      <a className="back-link" href="/incidents">← Incidents</a>
      <div className="detail-heading">
        <div>
          <span className="eyebrow">CORRELATION</span>
          <h1>{bundle.meta.correlation_id}</h1>
        </div>
        <div className="detail-meta">
          <span>{bundle.meta.cluster_id}</span>
          <span>{bundle.meta.incident_id ?? "incident pending"}</span>
        </div>
      </div>

      <ol className="pipeline">
        {PIPELINE.map(([title, detail], index) => (
          <li key={title}>
            <span>{String(index + 1).padStart(2, "0")}</span>
            <strong>{title}</strong>
            <small>{detail}</small>
          </li>
        ))}
      </ol>

      <div className="detail-grid">
        <article className="detail-card diagnosis">
          <span className="eyebrow">ROOT CAUSE</span>
          <h2>{bundle.diagnosis.root_cause}</h2>
          <div className="confidence">
            <span>confidence</span>
            <strong>{confidenceLabel(bundle.diagnosis.confidence)}</strong>
          </div>
          <h3>Supporting evidence</h3>
          <ul>
            {bundle.diagnosis.supporting_evidence.map((item) => <li key={item}>{item}</li>)}
          </ul>
          {bundle.diagnosis.missing_evidence.length > 0 ? (
            <>
              <h3>Missing evidence</h3>
              <ul className="muted-list">
                {bundle.diagnosis.missing_evidence.map((item) => <li key={item}>{item}</li>)}
              </ul>
            </>
          ) : null}
        </article>

        <article className="detail-card remediation">
          <div className="card-head">
            <span className="eyebrow">SAFE CHANGE</span>
            <span className="status status-open">{bundle.remediation?.status ?? "PENDING"}</span>
          </div>
          {selected ? (
            <>
              <h2>{selected.title}</h2>
              <p>{selected.description}</p>
              <dl>
                <div><dt>Delivery</dt><dd>{routeLabel(selected.route)}</dd></div>
                <div><dt>Risk</dt><dd>{selected.risk_level}</dd></div>
                <div><dt>Blast radius</dt><dd>{selected.blast_radius}</dd></div>
                <div><dt>Approval</dt><dd>{selected.approval_required ? "required" : "review required by policy"}</dd></div>
              </dl>
              <h3>Validation checks</h3>
              <ul>
                {selected.validation_checks.map((item) => <li key={item}>{item}</li>)}
              </ul>
              <div className="rollback">
                <strong>Rollback</strong>
                <span>{selected.rollback_plan}</span>
              </div>
            </>
          ) : bundle.remediation?.selected_action_id ? (
            <p>선택된 수정안을 찾지 못했습니다. 저장된 후보와 선택 상태를 확인해야 합니다.</p>
          ) : bundle.remediation && bundle.remediation.candidates.length > 0 ? (
            <p>수정 후보 {bundle.remediation.candidates.length}개가 있으나 아직 선택되지 않았습니다.</p>
          ) : (
            <p>허용 범위 안에서 만들 수 있는 수정안이 없습니다. 자동 변경 없이 evidence를 보존합니다.</p>
          )}
        </article>
      </div>
    </section>
  );
}

function DetailMessage({ title, detail }: { title: string; detail?: string }) {
  return (
    <section className="state-panel">
      <span className="eyebrow">RCA BUNDLE</span>
      <h1>{title}</h1>
      {detail ? <p>{detail}</p> : null}
      <a className="button" href="/incidents">Incident 목록</a>
    </section>
  );
}

function confidenceLabel(value: number | null): string {
  return value === null ? "—" : `${Math.round(value * 100)}%`;
}

function routeLabel(route: string): string {
  return ["safe_pr", "draft_pr", "pull_request"].includes(route) ? "GitOps Draft PR" : route;
}
