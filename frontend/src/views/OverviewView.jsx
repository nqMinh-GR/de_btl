import React, { useState } from "react";
import { useApp } from "../AppContext";
import { dash, fmtClock, fmtRate, fmtSeconds, shortId } from "../format";

function Metric({ label, value, hint, tone = "text-on-surface" }) {
  return (
    <div className="bg-surface-container-low border border-outline-variant rounded p-3.5">
      <div className="text-[11px] text-on-surface-variant mb-2">{label}</div>
      <div className={`text-metric-display font-headline font-bold ${tone}`}>{dash(value)}</div>
      <div className="mt-2 text-[10px] text-outline">{hint}</div>
    </div>
  );
}

export default function OverviewView() {
  const {
    currentRoute,
    currentRun,
    feedState,
    formattedWallClock,
    vehicleCounts,
    routeMetrics,
    warnings,
    snapshot,
    meta,
    setActiveTab,
    setIsSnapshotModalOpen,
    handleSyncFeed,
  } = useApp();
  const [hidden, setHidden] = useState({});
  const counts = vehicleCounts || {};
  const prediction = routeMetrics?.prediction_metrics;
  const vehicleMetrics = routeMetrics?.vehicle_metrics;
  const late = prediction?.predicted_late_trip_count;
  const eligible = prediction?.eligible_trip_count;
  const excluded = prediction?.excluded_by_reason || {};
  const excludedEntries = Object.entries(excluded);
  const visibleWarnings = warnings.filter((item) => !hidden[`${item.code}|${item.feed_type || ""}`]);
  const staleFeeds = [feedState.vp, feedState.tu, feedState.alerts].filter((feed) => feed.stale || feed.status === "STALE");

  const exportJson = () => {
    const blob = new Blob([JSON.stringify(snapshot || {}, null, 2)], { type: "application/json" });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = `transitpulse_${currentRoute.id || "snapshot"}.json`;
    link.click();
    URL.revokeObjectURL(link.href);
  };

  return (
    <main className="flex-1 p-5 space-y-6 max-w-[1600px] w-full font-mono">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-3 pb-3 border-b border-outline-variant">
        <div>
          <div className="flex items-center gap-2 text-on-surface-variant text-[11px] mb-1">
            <span>{currentRoute.name}</span>
            <span>·</span>
            <span className="text-primary font-bold">{currentRun.shortId}</span>
            <span>·</span>
            <span>Đồng hồ ứng dụng: {fmtClock(formattedWallClock)}</span>
          </div>
          <h1 className="text-headline-xl font-headline text-on-surface font-semibold">Tổng quan tuyến</h1>
        </div>
        <div className="flex items-center gap-2">
          <button onClick={exportJson} className="px-3 py-1.5 rounded border border-outline-variant text-[11px]">
            Export Snapshot JSON
          </button>
          <button onClick={handleSyncFeed} className="px-3 py-1.5 rounded bg-primary text-on-primary text-[11px] font-semibold">
            Làm mới
          </button>
        </div>
      </div>

      <section className="space-y-3">
        <h2 className="text-headline-md font-headline font-semibold">Xe trên tuyến</h2>
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
          <Metric label="Xe có vị trí mới" value={counts.fresh_vehicle_count} hint="fresh trong snapshot" tone="text-primary" />
          <Metric label="Vị trí cũ (stale)" value={counts.stale_vehicle_count} hint={`ngưỡng ${dash(vehicleMetrics?.stale_threshold_seconds)} s`} tone="text-tertiary" />
          <Metric label="Không rõ tuổi" value={counts.unknown_age_vehicle_count} hint="thiếu timestamp" />
          <Metric label="Không hợp lệ" value={counts.invalid_vehicle_count} hint="kể cả bản ghi không có vehicle_id" />
          <Metric label="Có mặt trong snapshot" value={counts.snapshot_membership_count} hint="số vehicle_id phân biệt" tone="text-primary" />
        </div>
        <div className="text-[11px] text-on-surface-variant border border-outline-variant rounded px-3 py-2">
          Số đếm do API tính lại theo đồng hồ ứng dụng. metrics_as_of {fmtClock(vehicleMetrics?.metrics_as_of)} · snapshot {shortId(vehicleMetrics?.snapshot_id)}
        </div>
      </section>

      <section className="space-y-3">
        <h2 className="text-headline-md font-headline font-semibold">Dự báo trễ</h2>
        <div className="border border-outline-variant bg-surface-container-low rounded p-4">
          <div className="text-[14px] font-bold">
            Độ trễ dự kiến ≥ {dash(prediction?.late_threshold_seconds)} s: {dash(late)} trên {dash(eligible)} chuyến đủ điều kiện ({fmtRate(prediction?.predicted_late_rate)})
          </div>
          <p className="text-[12px] text-on-surface-variant mt-1">
            Mẫu số là trip instance đủ điều kiện. Tỷ lệ null khi mẫu số bằng 0, không phải 0%.
          </p>
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <div className="lg:col-span-2 bg-surface-container-low border border-outline-variant rounded divide-y divide-outline-variant text-[11px]">
            {[
              ["Độ trễ dự kiến trung bình", prediction?.mean_predicted_delay_seconds == null ? "—" : `${Number(prediction.mean_predicted_delay_seconds).toFixed(1)} s`],
              ["Số chuyến bị loại khỏi KPI", dash(prediction?.excluded_trip_count)],
              ["Không rõ độ bất định", dash(prediction?.unknown_uncertainty_count)],
              ["Số trip instance", dash(prediction?.trip_instance_count)],
              ["Chính sách stop mục tiêu", dash(prediction?.target_policy)],
              ["policy_version", dash(prediction?.policy_version)],
              ["Tuổi feed TU", fmtSeconds(prediction?.feed_age_seconds)],
              ["metrics_as_of", fmtClock(prediction?.metrics_as_of)],
            ].map(([label, value]) => (
              <div key={label} className="flex items-center justify-between px-3.5 py-2">
                <span className="text-on-surface-variant">{label}</span>
                <span className="font-semibold">{value}</span>
              </div>
            ))}
          </div>
          <div className="bg-surface-container-low border border-outline-variant rounded p-3.5 text-[11px] space-y-2">
            <div className="font-semibold uppercase tracking-wider">Lý do loại khỏi KPI</div>
            {excludedEntries.length === 0 && <div className="text-outline">Không có chuyến bị loại, hoặc KPI chưa có.</div>}
            {excludedEntries.map(([reason, count]) => (
              <div key={reason} className="flex items-center justify-between border border-outline-variant rounded px-2 py-1.5">
                <span className="text-tertiary font-bold">{reason}</span>
                <span>{count}</span>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="space-y-3">
        <div className="flex items-center justify-between">
          <h2 className="text-headline-md font-headline font-semibold">Tình trạng feed GTFS-RT</h2>
          <span className="text-[11px] text-on-surface-variant">{staleFeeds.length} feed stale</span>
        </div>
        <div className="overflow-x-auto border border-outline-variant rounded">
          <table className="w-full text-left text-[11px]">
            <thead>
              <tr className="bg-surface-container text-[10px] uppercase text-outline">
                <th className="py-2 px-3">Feed</th>
                <th className="py-2 px-3">Trạng thái</th>
                <th className="py-2 px-3">Ý nghĩa</th>
                <th className="py-2 px-3">snapshot_id</th>
                <th className="py-2 px-3">feed_timestamp</th>
                <th className="py-2 px-3">metrics_as_of</th>
                <th className="py-2 px-3 text-right">Tuổi nguồn</th>
              </tr>
            </thead>
            <tbody>
              {[feedState.vp, feedState.tu, feedState.alerts].map((feed) => (
                <tr key={feed.key} className="border-t border-outline-variant/50">
                  <td className="py-2 px-3 font-bold">{feed.name}</td>
                  <td className="py-2 px-3">{feed.status}</td>
                  <td className="py-2 px-3 text-on-surface-variant">{feed.stateDesc}</td>
                  <td className="py-2 px-3">
                    <button onClick={() => setIsSnapshotModalOpen(true)} className="text-secondary underline">
                      {shortId(feed.snapshotId)}
                    </button>
                  </td>
                  <td className="py-2 px-3">{feed.feedTimestamp}</td>
                  <td className="py-2 px-3">{feed.metricsAsOf}</td>
                  <td className="py-2 px-3 text-right">{fmtSeconds(feed.latencySeconds)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="space-y-2 pb-8">
        <div className="flex items-center justify-between">
          <h2 className="text-headline-md font-headline font-semibold">Cảnh báo chất lượng pipeline</h2>
          <button onClick={() => setActiveTab("alerts")} className="text-[11px] text-on-surface-variant">
            Mở trang cảnh báo
          </button>
        </div>
        {visibleWarnings.length === 0 && (
          <div className="text-[12px] text-on-surface-variant border border-outline-variant rounded px-3 py-3">
            Không có cảnh báo pipeline trên các phản hồi vừa nhận. request {dash(meta?.request_id)}
          </div>
        )}
        {visibleWarnings.map((warning) => {
          const key = `${warning.code}|${warning.feed_type || ""}`;
          return (
            <div key={key} className="border border-tertiary/40 rounded p-3 flex items-start justify-between gap-3">
              <div className="text-[12px]">
                <div className="font-bold text-tertiary">{warning.code} {warning.feed_type ? `· ${warning.feed_type}` : ""}</div>
                <p className="text-on-surface mt-1">{warning.message}</p>
              </div>
              <button
                onClick={() => setHidden((prev) => ({ ...prev, [key]: true }))}
                className="text-[11px] border border-outline-variant rounded px-2 py-1"
              >
                Ẩn
              </button>
            </div>
          );
        })}
      </section>
    </main>
  );
}
