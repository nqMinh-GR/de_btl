import React, { useState } from "react";
import { useApp } from "../AppContext";
import { dash, fmtClock, fmtSeconds } from "../format";

const FEEDS = [
  ["vehicle_positions", "Vị trí xe (VP)"],
  ["trip_updates", "Dự báo chuyến (TU)"],
  ["alerts", "Cảnh báo dịch vụ"],
];

export default function QualityView() {
  const { currentRun, quality, pipeline } = useApp();
  const [feedKey, setFeedKey] = useState("vehicle_positions");
  const feed = quality?.feeds?.[feedKey] || null;
  const components = pipeline?.components || [];
  const backlog = pipeline?.kafka_backlog;
  const healthy = components.filter((item) => item.status && !String(item.status).toLowerCase().includes("fail")).length;

  return (
    <main className="flex-1 p-5 space-y-6 max-w-[1600px] w-full font-mono">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-3 pb-3 border-b border-outline-variant">
        <div>
          <div className="text-[10px] text-on-surface-variant mb-1">Run {currentRun.shortId}</div>
          <h1 className="text-headline-xl font-headline font-semibold">Chất lượng dữ liệu & pipeline</h1>
        </div>
        <div className="text-[11px] border border-outline-variant rounded px-3 py-1.5">
          run_state <span className="text-primary font-bold">{dash(pipeline?.run_state)}</span>
          <span className="text-outline"> · clock {fmtClock(pipeline?.application_clock)}</span>
        </div>
      </div>

      <section className="bg-surface-container-low border border-outline-variant rounded-lg p-4 space-y-4">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <h2 className="text-[15px] font-semibold">Đối soát theo feed</h2>
          <div className="inline-flex rounded p-0.5 bg-surface-container border border-outline-variant text-[11px]">
            {FEEDS.map(([key, label]) => (
              <button
                key={key}
                onClick={() => setFeedKey(key)}
                className={`px-3 py-1 rounded ${feedKey === key ? "bg-surface-container-highest text-primary font-bold" : "text-on-surface-variant"}`}
              >
                {label}
              </button>
            ))}
          </div>
        </div>
        {!feed && <div className="text-[12px] text-outline">Feed này chưa có bản ghi chất lượng.</div>}
        {feed && (
          <>
            <div className="grid grid-cols-2 md:grid-cols-5 gap-3 text-[11px]">
              {[
                ["Source", feed.source_entity_count],
                ["Selected", feed.selected_entity_count],
                ["Fully valid", feed.fully_valid_entity_count],
                ["Partially valid", feed.partially_valid_entity_count],
                ["Rejected", feed.rejected_entity_count],
              ].map(([label, value]) => (
                <div key={label} className="bg-surface-container border border-outline-variant rounded p-3">
                  <div className="text-outline uppercase text-[10px]">{label}</div>
                  <div className="text-metric-display font-headline font-bold">{dash(value)}</div>
                </div>
              ))}
            </div>
            <div className="text-[11px] text-outline">
              snapshot {dash(feed.snapshot_id)} · partial {String(feed.is_partial)} · empty {String(feed.is_empty)}
              {feed.truncated ? " · reasons bị cắt" : ""}
            </div>
            <div className="overflow-x-auto border border-outline-variant rounded">
              <table className="w-full text-left text-[11px]">
                <thead>
                  <tr className="text-[10px] uppercase text-outline">
                    <th className="py-2 px-3">Reason</th>
                    <th className="py-2 px-3">Severity</th>
                    <th className="py-2 px-3">Disposition</th>
                    <th className="py-2 px-3">Số lượng</th>
                    <th className="py-2 px-3">Đơn vị</th>
                    <th className="py-2 px-3">Nhóm</th>
                  </tr>
                </thead>
                <tbody>
                  {(feed.reasons || []).map((reason) => (
                    <tr key={reason.reason_code} className="border-t border-outline-variant/40">
                      <td className="py-2 px-3 font-bold text-secondary">{reason.reason_code}</td>
                      <td className="py-2 px-3">{dash(reason.severity)}</td>
                      <td className="py-2 px-3">{dash(reason.disposition)}</td>
                      <td className="py-2 px-3">{dash(reason.count)}</td>
                      <td className="py-2 px-3">{dash(reason.unit)}</td>
                      <td className="py-2 px-3">{dash(reason.category)}</td>
                    </tr>
                  ))}
                  {(feed.reasons || []).length === 0 && (
                    <tr>
                      <td colSpan={6} className="py-4 px-3 text-outline">Không có reason code.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </>
        )}
      </section>

      <section className="bg-surface-container-low border border-outline-variant rounded-lg p-4 space-y-3">
        <div className="flex items-center justify-between">
          <h2 className="text-[15px] font-semibold">Heartbeat thành phần</h2>
          <span className="text-[11px] text-outline">{healthy}/{components.length || 0} có trạng thái</span>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-[11px]">
            <thead>
              <tr className="text-[10px] uppercase text-outline">
                <th className="py-2 px-3">Thành phần</th>
                <th className="py-2 px-3">Trạng thái</th>
                <th className="py-2 px-3">Heartbeat</th>
                <th className="py-2 px-3">Tuổi</th>
                <th className="py-2 px-3">Batch</th>
                <th className="py-2 px-3">Thời gian batch</th>
                <th className="py-2 px-3">Published</th>
                <th className="py-2 px-3">Visibility</th>
                <th className="py-2 px-3">Lỗi</th>
              </tr>
            </thead>
            <tbody>
              {components.map((item) => (
                <tr key={item.component} className="border-t border-outline-variant/40">
                  <td className="py-2 px-3 font-bold">{item.component}</td>
                  <td className="py-2 px-3">{dash(item.status)}</td>
                  <td className="py-2 px-3">{fmtClock(item.last_heartbeat_at)}</td>
                  <td className="py-2 px-3">{fmtSeconds(item.heartbeat_age_seconds)}</td>
                  <td className="py-2 px-3">{dash(item.last_batch_id)}</td>
                  <td className="py-2 px-3">{item.last_batch_duration_seconds == null ? "—" : `${item.last_batch_duration_seconds}s`}</td>
                  <td className="py-2 px-3">{dash(item.snapshots_published_total)}</td>
                  <td className="py-2 px-3">{item.last_visibility_latency_seconds == null ? "—" : `${item.last_visibility_latency_seconds}s`}</td>
                  <td className="py-2 px-3">{dash(item.last_error)}</td>
                </tr>
              ))}
              {components.length === 0 && (
                <tr>
                  <td colSpan={9} className="py-4 px-3 text-outline">Chưa có heartbeat.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section className="bg-surface-container-low border border-outline-variant rounded-lg p-4 space-y-3">
        <h2 className="text-[15px] font-semibold">Kafka backlog</h2>
        {!backlog && <div className="text-[12px] text-outline">Chưa có số đo backlog. Heartbeat chưa ghi kafka_backlog.</div>}
        {backlog && (
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            {backlog.map((item, index) => (
              <div key={`${item.topic}-${item.partition}-${index}`} className="border border-outline-variant rounded p-3 text-[11px]">
                <div className="font-bold truncate" title={item.topic}>{dash(item.topic)}</div>
                <div className="text-outline">partition {dash(item.partition)}</div>
                <div className="mt-2 text-primary font-bold">{dash(item.backlog)} msgs</div>
                <div className="text-outline">end {dash(item.end_offset)} · latest {dash(item.latest_offset)}</div>
              </div>
            ))}
          </div>
        )}
      </section>
    </main>
  );
}
