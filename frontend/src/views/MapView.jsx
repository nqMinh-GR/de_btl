import React, { useEffect, useMemo, useRef, useState } from "react";
import { useApp } from "../AppContext";
import TransitMap from "../components/TransitMap";
import { dash, fmtClock, fmtSeconds } from "../format";

const FRESHNESS_STYLE = {
  fresh: "bg-primary-container/20 border border-primary-container text-primary",
  stale: "bg-tertiary-container/30 border border-tertiary text-tertiary",
  unknown: "bg-surface-container-highest text-on-surface-variant border border-outline-variant",
  invalid: "bg-error-container text-on-error-container border border-error/40",
};

export default function MapView() {
  const {
    currentRoute,
    vehicles,
    vehicleCounts,
    geometry,
    stops,
    feedState,
    formattedWallClock,
    meta,
    handleSyncFeed,
    setSelectedVehicleId,
    setActiveTab,
    selectedVehicleId,
  } = useApp();
  const [layers, setLayers] = useState({ stops: true, buses: true, trails: true });
  const [query, setQuery] = useState("");
  const [trails, setTrails] = useState([]);
  const trailRef = useRef(new Map());
  const routeKey = currentRoute.id || "none";

  useEffect(() => {
    trailRef.current = new Map();
    setTrails([]);
  }, [routeKey]);

  useEffect(() => {
    const store = trailRef.current;
    for (const vehicle of vehicles) {
      if (vehicle.freshness_status === "invalid") continue;
      if (!Number.isFinite(vehicle.latitude) || !Number.isFinite(vehicle.longitude)) continue;
      const point = [vehicle.longitude, vehicle.latitude];
      const path = store.get(vehicle.vehicle_id) || [];
      const last = path[path.length - 1];
      if (!last || last[0] !== point[0] || last[1] !== point[1]) {
        path.push(point);
        store.set(vehicle.vehicle_id, path.slice(-40));
      }
    }
    setTrails(
      [...store.entries()].map(([vehicle_id, coordinates]) => ({ vehicle_id, coordinates })),
    );
  }, [vehicles]);

  const filtered = useMemo(() => {
    const needle = query.trim().toLowerCase();
    if (!needle) return vehicles;
    return vehicles.filter((vehicle) =>
      [vehicle.vehicle_id, vehicle.label, vehicle.trip_id, vehicle.stop_id]
        .filter(Boolean)
        .join(" ")
        .toLowerCase()
        .includes(needle),
    );
  }, [vehicles, query]);

  const selected = vehicles.find((vehicle) => vehicle.vehicle_id === selectedVehicleId) || null;

  const exportCsv = () => {
    const headers = [
      "vehicle_id",
      "label",
      "trip_id",
      "freshness_status",
      "observation_time",
      "observation_age_seconds",
      "current_status",
      "stop_id",
      "latitude",
      "longitude",
      "speed_mps",
      "bearing",
      "quality_flags",
    ];
    const rows = filtered.map((vehicle) =>
      headers.map((key) => {
        const value = vehicle[key];
        return Array.isArray(value) ? value.join("|") : value ?? "";
      }),
    );
    const csv = [headers.join(","), ...rows.map((row) => row.join(","))].join("\n");
    const link = document.createElement("a");
    link.href = `data:text/csv;charset=utf-8,${encodeURIComponent(csv)}`;
    link.download = `vehicles_${currentRoute.id || "route"}.csv`;
    link.click();
  };

  const counts = vehicleCounts || {};

  return (
    <main className="flex-1 p-4 flex flex-col gap-3 font-mono max-w-[1600px] w-full">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-2">
        <div className="flex flex-wrap items-center gap-3">
          <h1 className="text-headline-xl font-headline text-on-surface tracking-tight font-bold">
            Bản đồ giám sát
          </h1>
          <span className="text-[10px] px-2 py-0.5 rounded bg-surface-container-high border border-outline-variant text-on-surface-variant font-bold">
            {currentRoute.name}
          </span>
          <span className="text-outline text-[11px]">
            Vị trí làm mới mỗi 5 giây · đồng hồ {fmtClock(formattedWallClock)}
          </span>
        </div>
        <span className="text-[11px] text-outline">Nền đường phố OSM / CARTO</span>
      </div>

      {feedState.vp.stale && (
        <div className="bg-tertiary-container/15 border border-tertiary-container/40 rounded p-2.5 flex items-center justify-between gap-3 text-tertiary">
          <div className="text-[12px]">
            <span className="font-bold">Dữ liệu vị trí cũ.</span> Vẫn hiển thị quan sát gần nhất
            {feedState.vp.latencySeconds != null ? ` · tuổi nguồn ${fmtSeconds(feedState.vp.latencySeconds)}` : ""}.
          </div>
          <button
            onClick={handleSyncFeed}
            className="px-2.5 py-1 rounded bg-tertiary-container/30 border border-tertiary/40 text-[11px] font-bold"
          >
            Làm mới
          </button>
        </div>
      )}

      {meta?.source_type === "synthetic" && (
        <div className="text-[12px] border border-outline-variant rounded px-3 py-2 text-on-surface-variant">
          Dữ liệu kiểm thử — shape, trạm và xe là fixture synthetic, không phải xe MBTA đang chạy.
        </div>
      )}

      <div className="relative w-full h-[540px] rounded-lg border border-outline-variant overflow-hidden bg-[#090D11]">
        <TransitMap
          shapes={geometry?.shapes || []}
          stops={stops}
          vehicles={vehicles}
          trails={trails}
          showStops={layers.stops}
          showVehicles={layers.buses}
          showTrails={layers.trails}
          selectedVehicleId={selectedVehicleId}
          onSelectVehicle={setSelectedVehicleId}
          fitKey={routeKey}
        />
        <div className="absolute top-3 left-3 z-10 flex flex-col gap-2">
          <div className="bg-surface-container/95 border border-outline-variant rounded p-2 text-[11px] flex flex-col gap-1">
            {[
              ["stops", "Trạm dừng"],
              ["buses", "Xe buýt"],
              ["trails", "Vết di chuyển"],
            ].map(([key, label]) => (
              <label key={key} className="flex items-center gap-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={layers[key]}
                  onChange={() => setLayers((prev) => ({ ...prev, [key]: !prev[key] }))}
                />
                <span>{label}</span>
              </label>
            ))}
          </div>
          {selected && (
            <div className="bg-surface-container/95 border border-outline-variant rounded p-2 text-[11px] max-w-[240px]">
              <div className="font-bold text-primary">{selected.vehicle_id}</div>
              <div className="text-on-surface-variant">
                {selected.freshness_status} · {dash(selected.current_status)}
              </div>
              <div>
                {Number(selected.latitude).toFixed(5)}, {Number(selected.longitude).toFixed(5)}
              </div>
              <button
                onClick={() => setActiveTab("history")}
                className="mt-1 text-secondary underline"
              >
                Mở lịch sử
              </button>
            </div>
          )}
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-2 text-[11px]">
        {[
          ["Fresh", counts.fresh_vehicle_count],
          ["Stale", counts.stale_vehicle_count],
          ["Không rõ tuổi", counts.unknown_age_vehicle_count],
          ["Invalid (không vẽ)", counts.invalid_vehicle_count],
        ].map(([label, value]) => (
          <div key={label} className="bg-surface-container-low border border-outline-variant rounded px-3 py-2">
            <div className="text-outline">{label}</div>
            <div className="text-headline-md font-headline font-bold">{dash(value)}</div>
          </div>
        ))}
      </div>

      <div className="bg-surface-container-low border border-outline-variant rounded-lg overflow-hidden">
        <div className="px-3 py-2 flex items-center justify-between gap-2 border-b border-outline-variant">
          <span className="text-[12px] font-semibold">Xe trong snapshot ({filtered.length})</span>
          <div className="flex items-center gap-2">
            <input
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Lọc xe, chuyến, trạm"
              className="bg-surface-container-lowest border border-outline-variant rounded px-2 py-1 text-[11px]"
            />
            <button onClick={exportCsv} className="px-2 py-1 rounded border border-outline-variant text-[11px]">
              CSV
            </button>
          </div>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-[11px]">
            <thead>
              <tr className="text-outline uppercase text-[10px]">
                <th className="py-2 px-3">Xe</th>
                <th className="py-2 px-3">Độ mới</th>
                <th className="py-2 px-3">Quan sát</th>
                <th className="py-2 px-3">Tuổi</th>
                <th className="py-2 px-3">Trạng thái</th>
                <th className="py-2 px-3">Tọa độ</th>
                <th className="py-2 px-3">Tốc độ</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((vehicle) => (
                <tr
                  key={vehicle.vehicle_id}
                  onClick={() => setSelectedVehicleId(vehicle.vehicle_id)}
                  className="border-t border-outline-variant/40 hover:bg-surface-container cursor-pointer"
                >
                  <td className="py-2 px-3 font-bold">{vehicle.vehicle_id}</td>
                  <td className="py-2 px-3">
                    <span className={`px-1.5 py-0.5 rounded text-[10px] font-bold ${FRESHNESS_STYLE[vehicle.freshness_status] || ""}`}>
                      {vehicle.freshness_status}
                    </span>
                  </td>
                  <td className="py-2 px-3">{fmtClock(vehicle.observation_time)}</td>
                  <td className="py-2 px-3">{fmtSeconds(vehicle.observation_age_seconds)}</td>
                  <td className="py-2 px-3">{dash(vehicle.current_status)}</td>
                  <td className="py-2 px-3">
                    {Number.isFinite(vehicle.latitude) ? `${vehicle.latitude.toFixed(5)}, ${vehicle.longitude.toFixed(5)}` : "—"}
                  </td>
                  <td className="py-2 px-3">{vehicle.speed_mps == null ? "—" : `${vehicle.speed_mps} m/s`}</td>
                </tr>
              ))}
              {filtered.length === 0 && (
                <tr>
                  <td colSpan={7} className="py-6 px-3 text-center text-outline">
                    Không có xe trong snapshot của tuyến này.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </main>
  );
}
