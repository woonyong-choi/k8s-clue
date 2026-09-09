import { lazy, Suspense, type ReactNode } from "react";
import { OverviewPage } from "./pages/OverviewPage";
import { resolveRoute, type AppRoute } from "./router";

const IncidentsPage = lazy(() => import("./pages/IncidentsPage"));
const IncidentDetailPage = lazy(() => import("./pages/IncidentDetailPage"));

const PRODUCT_DESCRIPTION =
  "Kubernetes 장애 증거를 보존하고 제한된 변경안을 GitOps Draft PR로 제안한 뒤 배포 결과를 다시 검증하는 운영 제어면";

export function App() {
  const route = resolveRoute(window.location.pathname);

  return (
    <div className="app">
      <header className="topbar">
        <a className="brand" href="/" aria-label="Clue 홈">
          <span className="brand-mark" aria-hidden="true">C</span>
          <span>Clue</span>
        </a>
        <nav aria-label="주요 메뉴">
          <a href="/" aria-current={route.kind === "overview" ? "page" : undefined}>Golden Path</a>
          <a
            href="/incidents"
            aria-current={route.kind === "incidents" || route.kind === "incident" ? "page" : undefined}
          >
            Incidents
          </a>
          <a href="https://github.com/woonyong-kr/k8s-clue-python-reference">GitHub</a>
        </nav>
      </header>
      <main>
        <Suspense fallback={<RouteLoading />}>
          {pageForRoute(route)}
        </Suspense>
      </main>
      <footer>
        <strong>Clue</strong>
        <span>{PRODUCT_DESCRIPTION}</span>
      </footer>
    </div>
  );
}

function pageForRoute(route: AppRoute): ReactNode {
  if (route.kind === "incidents") return <IncidentsPage />;
  if (route.kind === "incident") return <IncidentDetailPage route={route} />;
  return <OverviewPage route={route} />;
}

function RouteLoading() {
  return (
    <section className="state-panel" aria-live="polite">
      <span className="eyebrow">LOADING</span>
      <h1>증거 묶음을 불러오는 중입니다.</h1>
    </section>
  );
}
