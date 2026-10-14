import React from "react";
import { AppProvider, useApp } from "./AppContext";
import Sidebar from "./components/Sidebar";
import Header from "./components/Header";
import SnapshotLogModal from "./components/SnapshotLogModal";
import CommandPalette from "./components/CommandPalette";
import TerminalLogsDrawer from "./components/TerminalLogsDrawer";

// Views
import OverviewView from "./views/OverviewView";
import MapView from "./views/MapView";
import StopsView from "./views/StopsView";
import AlertsView from "./views/AlertsView";
import QualityView from "./views/QualityView";
import VehicleHistoryView from "./views/VehicleHistoryView";

function DashboardContent() {
  const { activeTab, connectionError, dataIssues } = useApp();

  return (
    <div className="min-h-screen flex bg-surface text-on-surface antialiased font-mono selection:bg-primary selection:text-on-primary">
      {/* 240px Fixed Sidebar */}
      <Sidebar />

      {/* Main Content Area */}
      <div className="flex-1 ml-60 flex flex-col min-w-0 bg-surface-container-lowest min-h-screen">
        {/* Sticky Telemetry Header Bar */}
        <Header />

        {(connectionError || dataIssues.length > 0) && (
          <div className="mx-4 mt-3 rounded border border-tertiary/50 bg-tertiary-container/15 px-3 py-2 text-[12px] text-tertiary">
            {connectionError || dataIssues.join(" · ")}
          </div>
        )}

        {/* View Routing */}
        <div className="flex-1 flex flex-col">
          {activeTab === "overview" && <OverviewView />}
          {activeTab === "map" && <MapView />}
          {activeTab === "stops" && <StopsView />}
          {activeTab === "alerts" && <AlertsView />}
          {activeTab === "quality" && <QualityView />}
          {activeTab === "history" && <VehicleHistoryView />}
        </div>
      </div>

      {/* Global Interactive Overlays */}
      <SnapshotLogModal />
      <CommandPalette />
      <TerminalLogsDrawer />
    </div>
  );
}

export default function App() {
  return (
    <AppProvider>
      <DashboardContent />
    </AppProvider>
  );
}
