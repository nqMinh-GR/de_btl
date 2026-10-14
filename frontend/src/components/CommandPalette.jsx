import React, { useState, useEffect } from "react";
import { useApp } from "../AppContext";
import { routeLabel } from "../format";

export default function CommandPalette() {
  const {
    isCommandPaletteOpen,
    setIsCommandPaletteOpen,
    setActiveTab,
    routes,
    setSelectedRouteId,
    vehicles,
    setSelectedVehicleId,
    handleSyncFeed,
    setIsSnapshotModalOpen,
  } = useApp();

  const [query, setQuery] = useState("");

  useEffect(() => {
    if (!isCommandPaletteOpen) {
      setQuery("");
    }
  }, [isCommandPaletteOpen]);

  if (!isCommandPaletteOpen) return null;

  const actions = [
    {
      category: "Điều hướng trang",
      items: [
        {
          label: "Tổng quan tuyến (Overview)",
          icon: "dashboard",
          action: () => setActiveTab("overview"),
        },
        {
          label: "Bản đồ giám sát (GIS Map)",
          icon: "map",
          action: () => setActiveTab("map"),
        },
        {
          label: "Trạm & dự báo (Stops & Predictions)",
          icon: "schedule",
          action: () => setActiveTab("stops"),
        },
        {
          label: "Cảnh báo Pipeline & Service (Alerts)",
          icon: "warning",
          action: () => setActiveTab("alerts"),
        },
        {
          label: "Chất lượng & Pipeline Ops (Quality)",
          icon: "analytics",
          action: () => setActiveTab("quality"),
        },
        {
          label: "Lịch sử xe (Vehicle History)",
          icon: "history",
          action: () => setActiveTab("history"),
        },
      ],
    },
    {
      category: "Chọn tuyến MBTA",
      items: routes.map((route) => ({
        label: routeLabel(route),
        icon: "directions_bus",
        action: () => setSelectedRouteId(route.route_id),
      })),
    },
    {
      category: "Chọn xe đang hoạt động",
      items: vehicles.map((v) => ({
        label: `Xe ${v.vehicle_id} (${v.label || "—"}) · ${v.route_id} · ${v.current_status || v.freshness_status}`,
        icon: "commute",
        action: () => {
          setSelectedVehicleId(v.vehicle_id);
          setActiveTab("history");
        },
      })),
    },
    {
      category: "Thao tác nhanh",
      items: [
        {
          label: "Làm mới dữ liệu từ API",
          icon: "refresh",
          action: handleSyncFeed,
        },
        {
          label: "Xem Snapshot Telemetry JSON",
          icon: "terminal",
          action: () => setIsSnapshotModalOpen(true),
        },
      ],
    },
  ];

  const filteredCategories = actions
    .map((cat) => ({
      ...cat,
      items: cat.items.filter((item) =>
        item.label.toLowerCase().includes(query.toLowerCase()),
      ),
    }))
    .filter((cat) => cat.items.length > 0);

  const handleSelect = (action) => {
    action();
    setIsCommandPaletteOpen(false);
  };

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center pt-20 p-4 bg-black/70 backdrop-blur-sm animate-fade-in font-mono">
      <div className="bg-surface-container-low border border-outline-variant rounded-lg w-full max-w-xl shadow-2xl overflow-hidden flex flex-col">
        {/* Search input */}
        <div className="p-3 bg-surface-container border-b border-outline-variant flex items-center gap-2.5">
          <span className="material-symbols-outlined text-outline text-[18px]">
            search
          </span>
          <input
            autoFocus
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Tìm trang, tuyến bus, mã xe, hoặc lệnh thao tác..."
            className="w-full bg-transparent text-[13px] text-on-surface placeholder:text-outline focus:outline-none font-mono"
          />
          <kbd className="px-1.5 py-0.5 rounded bg-surface-container-highest border border-outline text-[10px] text-outline">
            ESC
          </kbd>
        </div>

        {/* Results list */}
        <div className="max-h-80 overflow-y-auto p-2 divide-y divide-outline-variant/30">
          {filteredCategories.length === 0 ? (
            <div className="p-4 text-center text-[12px] text-outline">
              Không tìm thấy lệnh hoặc dữ liệu phù hợp với "{query}"
            </div>
          ) : (
            filteredCategories.map((cat, idx) => (
              <div key={idx} className="py-2 first:pt-0 last:pb-0">
                <div className="px-2 py-1 text-[10px] uppercase tracking-wider text-outline font-semibold">
                  {cat.category}
                </div>
                <div className="mt-1 space-y-0.5">
                  {cat.items.map((item, itemIdx) => (
                    <button
                      key={itemIdx}
                      onClick={() => handleSelect(item.action)}
                      className="w-full text-left px-2.5 py-1.5 rounded flex items-center justify-between text-[12px] text-on-surface hover:bg-surface-container-high hover:text-primary transition-colors group cursor-pointer"
                    >
                      <div className="flex items-center gap-2 truncate">
                        <span className="material-symbols-outlined text-outline group-hover:text-primary text-[15px]">
                          {item.icon}
                        </span>
                        <span className="truncate">{item.label}</span>
                      </div>
                      <span className="material-symbols-outlined text-outline text-[14px] opacity-0 group-hover:opacity-100 transition-opacity">
                        arrow_forward
                      </span>
                    </button>
                  ))}
                </div>
              </div>
            ))
          )}
        </div>

        {/* Footer shortcuts */}
        <div className="px-3 py-2 bg-surface-container-lowest border-t border-outline-variant flex items-center justify-between text-[10px] text-outline">
          <div className="flex items-center gap-3">
            <span>↑↓ di chuyển</span>
            <span>↵ chọn</span>
          </div>
          <span>TransitPulse Navigation Engine</span>
        </div>
      </div>
    </div>
  );
}
