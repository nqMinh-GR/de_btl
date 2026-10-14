import React, { useState } from "react";
import { useApp } from "../AppContext";
import { fmtClock, fmtSeconds } from "../format";

export default function Header() {
  const {
    currentRun,
    feedState,
    formattedWallClock,
    isSyncing,
    handleSyncFeed,
    setIsCommandPaletteOpen,
    setIsSnapshotModalOpen,
    setIsTerminalLogsOpen,
    totalAlerts,
    setActiveTab,
  } = useApp();

  const [copied, setCopied] = useState(false);

  const handleCopyRunId = () => {
    navigator.clipboard.writeText(currentRun.id);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <header className="sticky top-0 z-20 h-11 bg-surface-container-low border-b border-outline-variant flex items-center justify-between px-4 select-none font-mono">
      {/* Left Telemetry Cluster */}
      <div className="flex items-center gap-2 text-[11px] overflow-x-auto no-scrollbar py-1">
        {/* Mode Badge */}
        <span
          className={`inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-bold tracking-wider ${
            currentRun.mode === "live"
              ? "bg-primary-container text-on-primary-container"
              : currentRun.mode === "replay"
                ? "bg-tertiary-container text-on-tertiary-container"
                : "bg-surface-container-highest text-secondary border border-outline-variant"
          }`}
        >
          {currentRun.mode === "live" && (
            <span className="w-1.5 h-1.5 rounded-full bg-primary animate-pulse-subtle"></span>
          )}
          {currentRun.mode.toUpperCase()}
        </span>

        {/* Run Identifier */}
        <button
          onClick={handleCopyRunId}
          title={copied ? "Đã sao chép!" : "Bấm để sao chép Run ID đầy đủ"}
          className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded bg-surface-container-highest border border-outline-variant text-on-surface hover:border-primary transition-colors cursor-pointer"
        >
          <span className="text-outline text-[10px]">Run</span>
          <span className="text-primary font-bold">
            {currentRun.shortId}...
          </span>
          {copied ? (
            <span className="material-symbols-outlined text-[12px] text-primary">
              check
            </span>
          ) : (
            <span className="material-symbols-outlined text-[12px] text-outline">
              content_copy
            </span>
          )}
        </button>

        {/* Trạng thái RUNNING */}
        <div className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded bg-surface-container border border-outline-variant text-primary font-bold">
          <span className="text-outline text-[10px]">Trạng thái</span>
          <span className="text-primary text-[10px]">{currentRun.status}</span>
        </div>

        <span className="text-outline-variant">|</span>

        {/* Live Clock */}
        <div className="inline-flex items-center gap-1 text-on-surface-variant whitespace-nowrap">
          <span className="text-outline text-[10px]">Đồng hồ ứng dụng:</span>
          <span className="text-on-surface font-semibold text-[11px]">
            {fmtClock(formattedWallClock)}
          </span>
        </div>

        <span className="text-outline-variant hidden md:inline">|</span>

        {/* GTFS Hash */}
        <div className="hidden md:inline-flex items-center gap-1 text-on-surface-variant">
          <span className="text-outline text-[10px]">GTFS:</span>
          <span
            className="text-secondary font-mono text-[10px] truncate max-w-[110px]"
            title={currentRun.gtfsHash}
          >
            {currentRun.gtfsHash.substring(0, 12)}...
          </span>
        </div>

        <span className="text-outline-variant hidden lg:inline">|</span>

        {/* Feed Badges */}
        <div className="hidden lg:flex items-center gap-2">
          {/* VP Feed */}
          <div className="inline-flex items-center gap-1">
            <span className="text-outline text-[10px]">VP:</span>
            <span
              className={`inline-flex items-center px-1.5 py-0.2 rounded text-[10px] font-bold ${
                feedState.vp.status === "STALE"
                  ? "bg-tertiary-container/30 border border-tertiary-container text-tertiary"
                  : "bg-primary-container/20 border border-primary-container text-primary"
              }`}
            >
              {feedState.vp.status}
            </span>
            <span className="text-outline text-[10px]">
              {fmtSeconds(feedState.vp.latencySeconds)}
            </span>
          </div>

          {/* TU Feed */}
          <div className="inline-flex items-center gap-1">
            <span className="text-outline text-[10px]">TU:</span>
            <span
              className={`inline-flex items-center px-1.5 py-0.2 rounded text-[10px] font-bold ${
                feedState.tu.status === "STALE"
                  ? "bg-tertiary-container/30 border border-tertiary-container text-tertiary"
                  : "bg-primary-container/20 border border-primary-container text-primary"
              }`}
            >
              {feedState.tu.status}
            </span>
            <span className="text-outline text-[10px]">
              {fmtSeconds(feedState.tu.latencySeconds)}
            </span>
          </div>

          {/* Alerts Feed */}
          <div className="inline-flex items-center gap-1">
            <span className="text-outline text-[10px]">Alerts:</span>
            <span className="inline-flex items-center px-1.5 py-0.2 rounded bg-primary-container/20 border border-primary-container text-primary font-bold text-[10px]">
              {feedState.alerts.status}
            </span>
          </div>
        </div>
      </div>

      {/* Right Action Cluster */}
      <div className="flex items-center gap-2 pl-3 shrink-0">
        {/* Quick Search / Cmd+K */}
        <button
          onClick={() => setIsCommandPaletteOpen(true)}
          className="hidden sm:inline-flex items-center gap-1.5 px-2 py-1 rounded bg-surface-container-high border border-outline-variant hover:border-primary text-on-surface-variant hover:text-on-surface text-[10px] transition-colors"
          title="Mở Command Palette (Ctrl+K / ⌘K)"
        >
          <span className="material-symbols-outlined text-[13px]">search</span>
          <span className="hidden md:inline">Tìm kiếm</span>
          <kbd className="px-1 py-0.2 rounded bg-surface-container-highest border border-outline-variant text-[9px] text-outline">
            ⌘K
          </kbd>
        </button>

        {/* Sync Feed Button */}
        <button
          onClick={handleSyncFeed}
          disabled={isSyncing}
          className="inline-flex items-center gap-1 px-2.5 py-1 rounded bg-surface-container-high border border-outline-variant hover:border-primary text-on-surface text-[11px] transition-colors active:scale-95 disabled:opacity-50"
          title="Tái kích hoạt polling cycle và refresh feed realtime"
        >
          <span
            className={`material-symbols-outlined text-[14px] text-primary ${
              isSyncing ? "animate-spin" : ""
            }`}
          >
            refresh
          </span>
          <span className="hidden sm:inline">Sync Feed</span>
        </button>

        {/* Snapshot Log Button */}
        <button
          onClick={() => setIsSnapshotModalOpen(true)}
          className="inline-flex items-center gap-1 px-2.5 py-1 rounded bg-surface-container-high border border-outline-variant hover:border-primary text-on-surface text-[11px] transition-colors active:scale-95"
          title="Xem payload JSON snapshot hiện tại"
        >
          <span className="material-symbols-outlined text-[14px] text-secondary">
            terminal
          </span>
          <span className="hidden md:inline">Snapshot</span>
        </button>

        {/* Terminal Pipeline Logs Drawer Toggle */}
        <button
          onClick={() => setIsTerminalLogsOpen((prev) => !prev)}
          className="p-1 rounded bg-surface-container-high border border-outline-variant hover:border-primary text-on-surface-variant hover:text-primary transition-colors"
          title="Xem log streaming & Cassandra CAS pointer"
        >
          <span className="material-symbols-outlined text-[16px]">
            data_object
          </span>
        </button>

        <div className="h-4 w-px bg-outline-variant"></div>

        {/* Notification Bell */}
        <button
          onClick={() => setActiveTab("alerts")}
          className="relative text-on-surface-variant hover:text-primary transition-colors p-1"
          title={`${totalAlerts} cảnh báo đang kích hoạt`}
        >
          <span className="material-symbols-outlined text-[18px]">
            notifications
          </span>
          {totalAlerts > 0 && (
            <span className="absolute top-0 right-0 w-2 h-2 rounded-full bg-tertiary animate-ping"></span>
          )}
          {totalAlerts > 0 && (
            <span className="absolute top-0 right-0 w-2 h-2 rounded-full bg-tertiary"></span>
          )}
        </button>
      </div>
    </header>
  );
}
