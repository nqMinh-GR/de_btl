import React from "react";
import { useApp } from "../AppContext";
import { routeLabel, shortId } from "../format";

export default function Sidebar() {
  const {
    runs,
    routes,
    selectedRunId,
    setSelectedRunId,
    selectedRouteId,
    setSelectedRouteId,
    activeTab,
    setActiveTab,
    autoRefresh,
    setAutoRefresh,
    currentRun,
    currentRoute,
  } = useApp();

  const navItems = [
    { id: "overview", label: "Tổng quan tuyến", icon: "dashboard" },
    { id: "map", label: "Bản đồ", icon: "map" },
    { id: "stops", label: "Trạm & dự báo", icon: "schedule" },
    { id: "alerts", label: "Cảnh báo", icon: "warning" },
    { id: "quality", label: "Chất lượng & pipeline", icon: "analytics" },
  ];

  const detailItems = [{ id: "history", label: "Lịch sử xe", icon: "history" }];

  return (
    <aside className="fixed top-0 left-0 h-screen w-60 bg-surface-container-lowest border-r border-outline-variant flex flex-col justify-between py-4 px-3 select-none z-30 font-mono">
      <div className="flex flex-col gap-4">
        {/* Brand / Logo */}
        <div className="flex items-center gap-2.5 px-2">
          <div className="w-7 h-7 rounded bg-surface-container-high border border-outline-variant flex items-center justify-center text-primary shadow-sm">
            <span
              className="material-symbols-outlined text-primary text-[18px]"
              style={{ fontVariationSettings: "'FILL' 1" }}
            >
              sensors
            </span>
          </div>
          <div className="flex flex-col">
            <span className="text-headline-md font-headline text-on-surface tracking-tight font-semibold leading-none">
              TransitPulse MBTA
            </span>
            <span className="text-[10px] text-on-surface-variant leading-tight mt-0.5">
              GTFS-RT Telemetry
            </span>
          </div>
        </div>

        {/* Navigation Links */}
        <nav className="flex flex-col gap-1">
          {navItems.map((item) => {
            const isActive = activeTab === item.id;
            return (
              <button
                key={item.id}
                onClick={() => setActiveTab(item.id)}
                className={`w-full text-left px-3 py-2 rounded flex items-center gap-2.5 transition-all duration-150 ${
                  isActive
                    ? "bg-surface-container-high text-primary font-medium border-l-2 border-primary shadow-sm"
                    : "text-on-surface-variant hover:bg-surface-container hover:text-on-surface"
                }`}
              >
                <span
                  className={`material-symbols-outlined ${
                    isActive ? "text-primary" : "text-outline"
                  }`}
                  style={isActive ? { fontVariationSettings: "'FILL' 1" } : {}}
                >
                  {item.icon}
                </span>
                <span className="text-body-sm">{item.label}</span>
              </button>
            );
          })}

          {/* Section: Chi tiết */}
          <div className="pt-3 pb-1 px-3">
            <span className="text-[10px] uppercase text-outline tracking-wider font-semibold">
              Chi tiết
            </span>
          </div>

          {detailItems.map((item) => {
            const isActive = activeTab === item.id;
            return (
              <button
                key={item.id}
                onClick={() => setActiveTab(item.id)}
                className={`w-full text-left px-3 py-2 rounded flex items-center gap-2.5 transition-all duration-150 ${
                  isActive
                    ? "bg-surface-container-high text-primary font-medium border-l-2 border-primary shadow-sm"
                    : "text-on-surface-variant hover:bg-surface-container hover:text-on-surface"
                }`}
              >
                <span
                  className={`material-symbols-outlined ${
                    isActive ? "text-primary" : "text-outline"
                  }`}
                  style={isActive ? { fontVariationSettings: "'FILL' 1" } : {}}
                >
                  {item.icon}
                </span>
                <span className="text-body-sm">{item.label}</span>
              </button>
            );
          })}
        </nav>

        {/* Sidebar Filters: Run & Tuyến */}
        <div className="border-t border-outline-variant pt-3 flex flex-col gap-3 px-1">
          <span className="text-[11px] font-semibold text-on-surface">
            Run và tuyến
          </span>

          {/* Run selector */}
          <div className="flex flex-col gap-1">
            <label className="text-[10px] text-on-surface-variant">Run</label>
            <div className="relative">
              <select
                value={selectedRunId}
                onChange={(e) => setSelectedRunId(e.target.value)}
                className="w-full bg-surface-container-low border border-outline-variant rounded px-2.5 py-1.5 text-[10px] text-on-surface focus:outline-none focus:border-primary pr-6 appearance-none cursor-pointer hover:border-outline transition-colors"
              >
                {runs.length === 0 && <option value="">Chưa có run</option>}
                {runs.map((run) => (
                  <option key={run.run_id} value={run.run_id}>
                    {run.source_type || "run"} {shortId(run.run_id)}
                  </option>
                ))}
              </select>
              <span className="material-symbols-outlined absolute right-1.5 top-2 text-[14px] text-outline pointer-events-none">
                expand_more
              </span>
            </div>
          </div>

          {/* Tuyến selector */}
          <div className="flex flex-col gap-1">
            <label className="text-[10px] text-on-surface-variant">Tuyến</label>
            <div className="relative">
              <select
                value={selectedRouteId}
                onChange={(e) => setSelectedRouteId(e.target.value)}
                className="w-full bg-surface-container-low border border-outline-variant rounded px-2.5 py-1.5 text-[10px] text-on-surface focus:outline-none focus:border-primary pr-6 appearance-none cursor-pointer hover:border-outline transition-colors"
              >
                {routes.length === 0 && <option value="">Chưa có tuyến</option>}
                {routes.map((route) => (
                  <option key={route.route_id} value={route.route_id}>
                    {routeLabel(route)}
                  </option>
                ))}
              </select>
              <span className="material-symbols-outlined absolute right-1.5 top-2 text-[14px] text-outline pointer-events-none">
                expand_more
              </span>
            </div>
          </div>
        </div>
      </div>

      {/* Sidebar Footer / Auto-refresh toggle & Context Badge */}
      <div className="border-t border-outline-variant pt-3 px-1 flex flex-col gap-2.5">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <label className="relative inline-flex items-center cursor-pointer">
              <input
                type="checkbox"
                checked={autoRefresh}
                onChange={(e) => setAutoRefresh(e.target.checked)}
                className="sr-only peer"
                id="toggle-refresh"
              />
              <div className="w-7 h-4 bg-surface-container-highest peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-primary peer-checked:bg-primary-container after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-on-primary-container after:rounded-full after:h-3 after:w-3 after:transition-all"></div>
            </label>
            <label
              htmlFor="toggle-refresh"
              className="text-[11px] text-on-surface cursor-pointer select-none"
            >
              Tự làm mới mỗi 5 giây
            </label>
          </div>
          {autoRefresh && (
            <span
              className="w-1.5 h-1.5 rounded-full bg-primary animate-pulse-subtle"
              title="Feed polling loop active"
            ></span>
          )}
        </div>

        {/* Active Context Card */}
        <div className="bg-surface-container-low border border-outline-variant rounded p-2 flex flex-col gap-1">
          <div className="flex items-center justify-between text-[10px] text-on-surface-variant">
            <span className="flex items-center gap-1">
              <span className="material-symbols-outlined text-[12px]">
                tune
              </span>
              Run: {currentRun.mode.toUpperCase()}
            </span>
            <span className="text-primary font-bold">{currentRun.shortId}</span>
          </div>
          <div className="flex items-center justify-between text-[10px] text-on-surface-variant">
            <span className="flex items-center gap-1">
              <span className="material-symbols-outlined text-[12px]">
                directions_bus
              </span>
              Tuyến: {currentRoute.id}
            </span>
            <span
              className="text-on-surface truncate max-w-[100px]"
              title={currentRoute.longName}
            >
              {currentRoute.id || "—"}
            </span>
          </div>
        </div>
      </div>
    </aside>
  );
}
