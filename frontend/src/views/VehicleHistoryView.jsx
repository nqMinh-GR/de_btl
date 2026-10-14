import React, { useEffect, useRef, useState } from "react";
import { useApp } from "../AppContext";
import { apiGet } from "../api/client";
import TransitMap from "../components/TransitMap";
import { dash, fmtClock } from "../format";

export default function VehicleHistoryView() {
  const { currentRun, selectedVehicleId, setSelectedVehicleId, vehicles, meta } = useApp();
  const [inputId, setInputId] = useState(selectedVehicleId || "");
  const [fromUtc, setFromUtc] = useState("");
  const [toUtc, setToUtc] = useState("");
  const [observations, setObservations] = useState([]);
  const [pageToken, setPageToken] = useState(null);
  const [range, setRange] = useState(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const skipAuto = useRef(false);

  useEffect(() => {
    if (selectedVehicleId) setInputId(selectedVehicleId);
  }, [selectedVehicleId]);

  const load = async ({ append, token, vehicleId = inputId.trim() } = {}) => {
    if (!currentRun.id || !vehicleId) {
      setError("Chọn run và nhập mã xe.");
      return;
    }
    setLoading(true);
    setError("");
    if (vehicleId !== selectedVehicleId) {
      skipAuto.current = true;
      setSelectedVehicleId(vehicleId);
    }
    try {
      const result = await apiGet(`/vehicles/${encodeURIComponent(vehicleId)}/history`, {
        run_id: currentRun.id,
        from: fromUtc || undefined,
        to: toUtc || undefined,
        page_token: token || undefined,
        page_size: 100,
      });
      const rows = result.data?.observations || [];
      setObservations((prev) => (append ? [...prev, ...rows] : rows));
      setPageToken(result.data?.next_page_token || null);
      setRange(result.data?.range || null);
    } catch (err) {
      setError(err.message || "Không tải được lịch sử");
      if (!append) {
        setObservations([]);
        setPageToken(null);
      }
    } finally {
      setLoading(false);
    }
  };

  const firstVehicleId = vehicles[0]?.vehicle_id || "";

  useEffect(() => {
    if (!selectedVehicleId && firstVehicleId) setSelectedVehicleId(firstVehicleId);
  }, [selectedVehicleId, firstVehicleId, setSelectedVehicleId]);

  useEffect(() => {
    if (skipAuto.current) {
      skipAuto.current = false;
      return undefined;
    }
    const id = selectedVehicleId;
    if (!currentRun.id || !id) return undefined;
    setInputId(id);
    let cancelled = false;
    (async () => {
      setLoading(true);
      try {
        const result = await apiGet(`/vehicles/${encodeURIComponent(id)}/history`, {
          run_id: currentRun.id,
          page_size: 100,
        });
        if (cancelled) return;
        setObservations(result.data?.observations || []);
        setPageToken(result.data?.next_page_token || null);
        setRange(result.data?.range || null);
        setError("");
      } catch (err) {
        if (!cancelled) setError(err.message || "Không tải được lịch sử");
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [currentRun.id, selectedVehicleId]);

  const history = observations
    .filter((item) => Number.isFinite(item.latitude) && Number.isFinite(item.longitude))
    .map((item) => [item.longitude, item.latitude]);

  return (
    <main className="flex-1 p-5 space-y-5 max-w-[1600px] w-full font-mono">
      <div className="pb-3 border-b border-outline-variant">
        <div className="text-[10px] text-on-surface-variant mb-1">GET /vehicles/{"{id}"}/history</div>
        <h1 className="text-headline-xl font-headline font-semibold">Lịch sử xe ({inputId || "—"})</h1>
      </div>

      <div className="bg-surface-container-low border border-outline-variant rounded p-3.5 flex flex-col md:flex-row gap-3 md:items-end">
        <label className="flex flex-col gap-1 text-[11px] flex-1">
          Mã xe
          <div className="flex gap-2">
            <input value={inputId} onChange={(event) => setInputId(event.target.value)} className="flex-1 bg-surface-container-high border border-outline-variant rounded px-2 py-1.5" />
            <select
              value={vehicles.some((item) => item.vehicle_id === inputId) ? inputId : ""}
              onChange={(event) => setInputId(event.target.value)}
              className="bg-surface-container-high border border-outline-variant rounded px-2"
            >
              <option value="">Trong snapshot</option>
              {vehicles.map((vehicle) => (
                <option key={vehicle.vehicle_id} value={vehicle.vehicle_id}>{vehicle.vehicle_id}</option>
              ))}
            </select>
          </div>
        </label>
        <label className="flex flex-col gap-1 text-[11px]">
          Từ (UTC, để trống = mặc định API)
          <input value={fromUtc} onChange={(event) => setFromUtc(event.target.value)} placeholder="2026-10-14T07:00:00Z" className="bg-surface-container-high border border-outline-variant rounded px-2 py-1.5" />
        </label>
        <label className="flex flex-col gap-1 text-[11px]">
          Đến
          <input value={toUtc} onChange={(event) => setToUtc(event.target.value)} placeholder="2026-10-14T08:00:00Z" className="bg-surface-container-high border border-outline-variant rounded px-2 py-1.5" />
        </label>
        <button onClick={() => load()} className="px-3 py-1.5 rounded bg-primary text-on-primary text-[11px] font-semibold">
          Tìm kiếm
        </button>
        <button
          disabled={!pageToken || loading}
          onClick={() => load({ append: true, token: pageToken })}
          className="px-3 py-1.5 rounded border border-outline-variant text-[11px] disabled:opacity-40"
        >
          Tải thêm
        </button>
      </div>

      <div className="text-[11px] text-on-surface-variant border border-outline-variant rounded px-3 py-2 flex justify-between gap-3">
        <span>
          Khoảng [{fmtClock(range?.start)}, {fmtClock(range?.end)}) · {observations.length} quan sát
          {pageToken ? " · còn trang tiếp" : " · hết trang"}
          {loading ? " · đang tải" : ""}
        </span>
        {meta?.source_type === "synthetic" && (
          <span className="text-tertiary font-bold">Dữ liệu kiểm thử — không phải xe MBTA thật</span>
        )}
      </div>
      {error && <div className="text-[12px] text-tertiary border border-tertiary/40 rounded px-3 py-2">{error}</div>}

      <div className="bg-surface-container-low border border-outline-variant rounded-lg p-3">
        <h2 className="text-[14px] font-semibold mb-2">Vết GPS trên bản đồ</h2>
        <div className="h-[320px] rounded overflow-hidden border border-outline-variant">
          <TransitMap history={history} showStops={false} showVehicles={false} showTrails={false} fitKey={`history-${inputId}-${observations.length}`} />
        </div>
      </div>

      <div className="bg-surface-container-low border border-outline-variant rounded-lg overflow-hidden">
        <div className="px-3 py-2 border-b border-outline-variant text-[13px] font-semibold">
          Nhật ký quan sát ({observations.length})
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-[11px]">
            <thead>
              <tr className="text-[10px] uppercase text-outline">
                <th className="py-2 px-3">Thời điểm</th>
                <th className="py-2 px-3">Bản ghi</th>
                <th className="py-2 px-3">Nguồn thời điểm</th>
                <th className="py-2 px-3">Tọa độ</th>
                <th className="py-2 px-3">Tốc độ</th>
                <th className="py-2 px-3">Hướng</th>
                <th className="py-2 px-3">Trip</th>
                <th className="py-2 px-3">Trạm</th>
                <th className="py-2 px-3">Trạng thái</th>
                <th className="py-2 px-3">Cờ</th>
              </tr>
            </thead>
            <tbody>
              {[...observations].reverse().map((item) => (
                <tr key={item.observation_id} className="border-t border-outline-variant/40">
                  <td className="py-2 px-3 whitespace-nowrap">{fmtClock(item.event_time)}</td>
                  <td className="py-2 px-3">{dash(item.record_kind)}</td>
                  <td className="py-2 px-3">{dash(item.event_time_origin)}</td>
                  <td className="py-2 px-3">
                    {Number.isFinite(item.latitude) ? `${item.latitude.toFixed(5)}, ${item.longitude.toFixed(5)}` : "—"}
                  </td>
                  <td className="py-2 px-3">
                    {item.speed_mps == null ? "—" : `${(item.speed_mps * 3.6).toFixed(1)} km/h`}
                  </td>
                  <td className="py-2 px-3">{item.bearing == null ? "—" : `${item.bearing}°`}</td>
                  <td className="py-2 px-3">{dash(item.trip_id)}</td>
                  <td className="py-2 px-3">{dash(item.stop_id)}</td>
                  <td className="py-2 px-3">{dash(item.current_status)}</td>
                  <td className="py-2 px-3">{(item.quality_flags || []).join(", ") || "—"}</td>
                </tr>
              ))}
              {observations.length === 0 && (
                <tr>
                  <td colSpan={10} className="py-6 px-3 text-center text-outline">Chưa có quan sát trong khoảng này.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </main>
  );
}
