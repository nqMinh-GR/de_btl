import React, { useState } from "react";
import { useApp } from "../AppContext";
import { dash, fmtClock } from "../format";

export default function AlertsView() {
  const { currentRoute, currentRun, serviceAlerts, alertsPayload, warnings, pipeline, meta, handleSyncFeed, snapshot } = useApp();
  const [raw, setRaw] = useState(false);
  const [hidden, setHidden] = useState({});
  const freshness = meta?.freshness;
  const latency = pipeline?.latency || {};
  const visible = warnings.filter((item) => !hidden[`${item.code}|${item.feed_type || ""}`]);

  return (
    <main className="flex-1 p-5 space-y-5 max-w-[1600px] w-full font-mono">
      <div className="pb-3 border-b border-outline-variant">
        <div className="text-[10px] text-on-surface-variant mb-1">{currentRoute.name} · Run {currentRun.shortId}</div>
        <h1 className="text-headline-xl font-headline font-semibold">Cảnh báo & sự cố</h1>
      </div>

      <div className="flex flex-wrap gap-3 text-[11px]">
        <span className="px-3 py-1.5 border border-outline-variant rounded">Pipeline: {warnings.length}</span>
        <span className="px-3 py-1.5 border border-outline-variant rounded">Dịch vụ MBTA: {serviceAlerts.length}</span>
        <span className="px-3 py-1.5 border border-outline-variant rounded">
          snapshot {dash(alertsPayload?.snapshot_id)}
        </span>
      </div>

      <section className="bg-surface-container-low border border-outline-variant rounded-lg p-4 space-y-3">
        <div className="flex items-center justify-between">
          <h2 className="text-[14px] font-semibold">Cảnh báo dịch vụ MBTA</h2>
          <button onClick={() => setRaw(true)} className="text-[11px] text-secondary underline">Payload JSON</button>
        </div>
        {serviceAlerts.length === 0 && (
          <div className="text-[12px] text-on-surface-variant border border-outline-variant rounded p-3">
            Không có cảnh báo dịch vụ cho phạm vi này.
            {alertsPayload?.reason ? ` Lý do: ${alertsPayload.reason}.` : ""} Đánh giá lúc {fmtClock(alertsPayload?.evaluated_at)}.
          </div>
        )}
        {serviceAlerts.map((alert) => (
          <article key={alert.alert_id} className="border border-outline-variant rounded p-3 text-[12px] space-y-1">
            <div className="flex flex-wrap gap-2 text-[10px] font-bold">
              <span className="px-1.5 py-0.5 rounded bg-surface-container-highest">{alert.active_state_at_clock}</span>
              <span className="text-tertiary">{dash(alert.effect)}</span>
              <span className="text-outline">{dash(alert.cause)}</span>
            </div>
            <h3 className="font-semibold">{alert.header_text || alert.alert_id}</h3>
            {alert.description_text && <p className="text-on-surface-variant">{alert.description_text}</p>}
            <div className="text-[11px] text-outline">
              Tuyến {alert.route_ids?.join(", ") || "—"} · {alert.inherited ? "kế thừa snapshot trước" : "snapshot hiện tại"}
            </div>
          </article>
        ))}
      </section>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <section className="lg:col-span-2 bg-surface-container-low border border-outline-variant rounded-lg p-4 space-y-3">
          <div className="flex items-center justify-between">
            <h2 className="text-[14px] font-semibold">Cảnh báo chất lượng pipeline</h2>
            <button onClick={handleSyncFeed} className="text-[11px] text-primary">Làm mới</button>
          </div>
          {visible.length === 0 && <div className="text-[12px] text-outline">Không có warning trên phản hồi vừa nhận.</div>}
          {visible.map((warning) => {
            const key = `${warning.code}|${warning.feed_type || ""}`;
            return (
              <div key={key} className="border border-outline-variant rounded p-3 text-[12px]">
                <div className="font-bold text-tertiary">{warning.code} {warning.feed_type ? `(${warning.feed_type})` : ""}</div>
                <p className="mt-1">{warning.message}</p>
                <button onClick={() => setHidden((prev) => ({ ...prev, [key]: true }))} className="mt-2 text-[11px] border border-outline-variant rounded px-2 py-0.5">
                  Ẩn
                </button>
              </div>
            );
          })}
        </section>
        <aside className="space-y-3">
          <div className="bg-surface-container-low border border-outline-variant rounded-lg p-4 text-[11px] space-y-2">
            <div className="font-semibold uppercase tracking-wider">Ngưỡng độ mới</div>
            <div className="flex justify-between"><span>Vehicle positions</span><span>{dash(freshness?.vehicle_stale_threshold_seconds)} s</span></div>
            <div className="flex justify-between"><span>Trip updates</span><span>{dash(freshness?.prediction_stale_threshold_seconds)} s</span></div>
            <div className="flex justify-between"><span>Alerts</span><span>{dash(freshness?.alert_stale_threshold_seconds)} s</span></div>
            <div className="flex justify-between"><span>policy</span><span>{dash(freshness?.policy_version)}</span></div>
          </div>
          <div className="bg-surface-container-low border border-outline-variant rounded-lg p-4 text-[11px] space-y-2">
            <div className="font-semibold uppercase tracking-wider">Độ trễ hiển thị đã đo</div>
            {Object.keys(latency).length === 0 && <div className="text-outline">Chưa có số đo visibility latency.</div>}
            {Object.entries(latency).map(([feed, seconds]) => (
              <div key={feed} className="flex justify-between">
                <span>{feed}</span>
                <span>{Number(seconds).toFixed(2)} s</span>
              </div>
            ))}
            <div className="text-outline">run_state {dash(pipeline?.run_state)}</div>
          </div>
        </aside>
      </div>

      {raw && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70">
          <div className="bg-surface-container-low border border-outline-variant rounded-lg w-full max-w-3xl max-h-[80vh] flex flex-col">
            <div className="px-4 py-3 border-b border-outline-variant flex justify-between">
              <span className="text-[13px] font-bold">Alerts payload</span>
              <button onClick={() => setRaw(false)}>Đóng</button>
            </div>
            <pre className="p-4 overflow-auto text-[11px] bg-[#090D11]">{JSON.stringify(snapshot?.alerts || alertsPayload, null, 2)}</pre>
          </div>
        </div>
      )}
    </main>
  );
}
