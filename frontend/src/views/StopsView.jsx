import React, { useEffect, useState } from "react";
import { useApp } from "../AppContext";
import { dash, delayText, etaMinutes, fmtClock, fmtSeconds, qualityLabel } from "../format";

export default function StopsView() {
  const { currentRoute, currentRun, feedState, stops, formattedWallClock, fetchPredictions, setSelectedVehicleId, setActiveTab } = useApp();
  const [direction, setDirection] = useState("all");
  const [stopId, setStopId] = useState("");
  const [showInvalid, setShowInvalid] = useState(false);
  const [search, setSearch] = useState("");
  const [payload, setPayload] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!stops.some((stop) => stop.stop_id === stopId)) {
      setStopId(stops[0]?.stop_id || "");
    }
  }, [stops, stopId]);

  useEffect(() => {
    if (!stopId) {
      setPayload(null);
      return undefined;
    }
    let cancelled = false;
    setError("");
    fetchPredictions(stopId, {
      include: showInvalid ? "all" : "displayable",
      directionId: direction === "all" ? undefined : direction,
    })
      .then((result) => {
        if (!cancelled) setPayload(result);
      })
      .catch((err) => {
        if (!cancelled) setError(err.message || "Không tải được dự báo");
      });
    return () => {
      cancelled = true;
    };
  }, [stopId, direction, showInvalid, fetchPredictions, currentRun.id]);

  const stop = stops.find((item) => item.stop_id === stopId);
  const needle = search.trim().toLowerCase();
  const predictions = (payload?.data?.predictions || []).filter((item) => {
    if (!needle) return true;
    return [item.trip_id, item.vehicle_id, item.route_id].filter(Boolean).join(" ").toLowerCase().includes(needle);
  });

  return (
    <main className="flex-1 p-5 space-y-5 max-w-[1600px] w-full font-mono">
      <div className="flex flex-col md:flex-row md:items-end justify-between gap-3 pb-3 border-b border-outline-variant">
        <div>
          <div className="text-[10px] text-on-surface-variant mb-1">
            {currentRoute.name} · Run {currentRun.shortId}
          </div>
          <h1 className="text-headline-xl font-headline font-semibold">Trạm & dự báo</h1>
        </div>
        <div className="text-[11px] text-on-surface-variant">
          {predictions.length} bản ghi · tuổi feed TU {fmtSeconds(feedState.tu.latencySeconds)}
        </div>
      </div>

      <div className="bg-surface-container-low border border-outline-variant rounded p-3.5 flex flex-wrap gap-4 items-end">
        <label className="flex flex-col gap-1 text-[11px]">
          Hướng
          <select value={direction} onChange={(event) => setDirection(event.target.value)} className="bg-surface-container-high border border-outline-variant rounded px-2 py-1.5">
            <option value="all">Tất cả hướng</option>
            <option value="0">Hướng 0</option>
            <option value="1">Hướng 1</option>
          </select>
        </label>
        <label className="flex flex-col gap-1 text-[11px] min-w-[280px] flex-1">
          Trạm ({stops.length})
          <select value={stopId} onChange={(event) => setStopId(event.target.value)} className="bg-surface-container-high border border-outline-variant rounded px-2 py-1.5">
            {stops.map((item) => (
              <option key={item.stop_id} value={item.stop_id}>
                {item.stop_name || item.stop_id} ({item.stop_id})
              </option>
            ))}
          </select>
        </label>
        <label className="flex items-center gap-2 text-[11px]">
          <input type="checkbox" checked={showInvalid} onChange={(event) => setShowInvalid(event.target.checked)} />
          Hiện cả dự báo INVALID
        </label>
      </div>

      <div className="text-[11px] text-outline">
        Stop {dash(stop?.stop_id)} · {stop?.latitude != null ? `${Number(stop.latitude).toFixed(5)}, ${Number(stop.longitude).toFixed(5)}` : "không có tọa độ"}
      </div>
      {error && <div className="text-[12px] text-tertiary border border-tertiary/40 rounded px-3 py-2">{error}</div>}
      {stops.length === 0 && <div className="text-[12px] text-outline">Tuyến này chưa có danh sách trạm.</div>}

      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        {predictions.slice(0, 3).map((item) => {
          const minutes = etaMinutes(item.predicted_time, formattedWallClock);
          return (
            <div key={item.prediction_id} className="bg-surface-container-low border border-outline-variant rounded-lg p-3.5">
              <div className="flex items-center justify-between text-[11px]">
                <span className="font-bold">{dash(item.trip_id)}</span>
                <span className="px-1.5 py-0.5 rounded bg-surface-container-highest text-[10px] font-bold">{qualityLabel(item.prediction_quality)}</span>
              </div>
              <div className="text-metric-display font-headline font-bold text-primary my-1">
                {minutes == null ? "—" : minutes} <span className="text-[14px] text-outline">phút</span>
              </div>
              <div className="text-[12px]">{delayText(item.predicted_delay_seconds)}</div>
              <div className="text-[10px] text-outline mt-1">Dự kiến {fmtClock(item.predicted_time)}</div>
              <div className="text-[10px] mt-2">
                Xe: {item.vehicle_id ? (
                  <button
                    className="text-primary font-bold"
                    onClick={() => {
                      setSelectedVehicleId(item.vehicle_id);
                      setActiveTab("history");
                    }}
                  >
                    {item.vehicle_id}
                  </button>
                ) : "chưa gán"}
                {" · "}±{dash(item.uncertainty_seconds)} s
              </div>
            </div>
          );
        })}
      </div>

      <div className="bg-surface-container-low border border-outline-variant rounded-lg overflow-hidden">
        <div className="px-3 py-2 flex items-center justify-between border-b border-outline-variant">
          <span className="text-[13px] font-semibold">Bảng dự báo tại {stop?.stop_name || stopId || "trạm"}</span>
          <input
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Tìm trip hoặc xe"
            className="bg-surface-container-lowest border border-outline-variant rounded px-2 py-1 text-[11px]"
          />
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-[11px]">
            <thead>
              <tr className="text-[10px] uppercase text-outline">
                <th className="py-2 px-3">Trip</th>
                <th className="py-2 px-3">Xe</th>
                <th className="py-2 px-3">Dự kiến</th>
                <th className="py-2 px-3">Lịch</th>
                <th className="py-2 px-3">Độ lệch</th>
                <th className="py-2 px-3">Bất định</th>
                <th className="py-2 px-3">Chất lượng</th>
                <th className="py-2 px-3">Target</th>
              </tr>
            </thead>
            <tbody>
              {predictions.map((item) => (
                <tr key={item.prediction_id} className="border-t border-outline-variant/40">
                  <td className="py-2 px-3 text-secondary">{dash(item.trip_id)}</td>
                  <td className="py-2 px-3">
                    {item.vehicle_id ? (
                      <button
                        className="text-primary font-bold"
                        onClick={() => {
                          setSelectedVehicleId(item.vehicle_id);
                          setActiveTab("history");
                        }}
                      >
                        {item.vehicle_id}
                      </button>
                    ) : "—"}
                  </td>
                  <td className="py-2 px-3">{fmtClock(item.predicted_time)}</td>
                  <td className="py-2 px-3">{fmtClock(item.scheduled_time)}</td>
                  <td className="py-2 px-3">{delayText(item.predicted_delay_seconds)}</td>
                  <td className="py-2 px-3">{item.uncertainty_seconds == null ? "—" : `±${item.uncertainty_seconds}s`}</td>
                  <td className="py-2 px-3">{qualityLabel(item.prediction_quality)}</td>
                  <td className="py-2 px-3">{item.is_target ? "TARGET" : "—"}</td>
                </tr>
              ))}
              {predictions.length === 0 && (
                <tr>
                  <td colSpan={8} className="py-6 px-3 text-center text-outline">Không có dự báo cho bộ lọc này.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </main>
  );
}
