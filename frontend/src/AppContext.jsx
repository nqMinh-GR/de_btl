import React, { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { apiGet } from "./api/client";
import { feedView, routeLabel, shortId } from "./format";

const AppContext = createContext(null);
const REFRESH_MS = 5000;

const EMPTY_RUN = {
  run_id: "",
  source_type: "—",
  run_state: "—",
  gtfs_version: "",
  label: "Chưa có run",
};

function dedupeWarnings(items) {
  const seen = new Set();
  const out = [];
  for (const item of items) {
    if (!item) continue;
    const key = `${item.code}|${item.feed_type || ""}|${item.message}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}

export function AppProvider({ children }) {
  const [selectedRunId, setSelectedRunIdState] = useState("");
  const [selectedRouteId, setSelectedRouteIdState] = useState("");
  const [activeTab, setActiveTab] = useState("overview");
  const [selectedVehicleId, setSelectedVehicleId] = useState("");
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [isCommandPaletteOpen, setIsCommandPaletteOpen] = useState(false);
  const [isSnapshotModalOpen, setIsSnapshotModalOpen] = useState(false);
  const [isTerminalLogsOpen, setIsTerminalLogsOpen] = useState(false);

  const [runs, setRuns] = useState([]);
  const [routes, setRoutes] = useState([]);
  const [runDetail, setRunDetail] = useState(null);
  const [vehiclesPayload, setVehiclesPayload] = useState(null);
  const [routeMetrics, setRouteMetrics] = useState(null);
  const [quality, setQuality] = useState(null);
  const [alertsPayload, setAlertsPayload] = useState(null);
  const [pipeline, setPipeline] = useState(null);
  const [geometry, setGeometry] = useState(null);
  const [stops, setStops] = useState([]);
  const [meta, setMeta] = useState(null);
  const [warnings, setWarnings] = useState([]);
  const [apiLogs, setApiLogs] = useState([]);
  const [connectionError, setConnectionError] = useState(null);
  const [dataIssues, setDataIssues] = useState([]);
  const [isSyncing, setIsSyncing] = useState(false);
  const [snapshot, setSnapshot] = useState(null);

  const runIdRef = useRef("");
  const routeIdRef = useRef("");
  const seq = useRef(0);
  const refreshRef = useRef(async () => {});

  const setSelectedRunId = useCallback((id) => {
    runIdRef.current = id;
    routeIdRef.current = "";
    setSelectedRouteIdState("");
    setSelectedRunIdState(id);
  }, []);

  const setSelectedRouteId = useCallback((id) => {
    routeIdRef.current = id;
    setSelectedRouteIdState(id);
  }, []);

  refreshRef.current = async () => {
    const token = ++seq.current;
    setIsSyncing(true);
    const logs = [];
    const note = (entry) => logs.push(entry);
    try {
      const health = await apiGet("/health/live");
      note(health.log);
      const runsRes = await apiGet("/runs", { limit: 20 });
      note(runsRes.log);
      if (token !== seq.current) return;
      const list = runsRes.data?.runs || [];
      setRuns(list);
      let runId = runIdRef.current;
      if (!list.some((run) => run.run_id === runId)) {
        runId = list[0]?.run_id || "";
        runIdRef.current = runId;
        setSelectedRunIdState(runId);
      }
      let routeList = [];
      const collected = [...(runsRes.warnings || [])];
      let latestMeta = runsRes.meta;
      if (runId) {
        const routesRes = await apiGet("/routes", { run_id: runId });
        note(routesRes.log);
        routeList = routesRes.data?.routes || [];
        collected.push(...(routesRes.warnings || []));
        latestMeta = routesRes.meta || latestMeta;
      }
      if (token !== seq.current) return;
      setRoutes(routeList);
      let routeId = routeIdRef.current;
      if (!routeList.some((route) => route.route_id === routeId)) {
        routeId = routeList[0]?.route_id || "";
        routeIdRef.current = routeId;
        setSelectedRouteIdState(routeId);
      }
      if (!runId) {
        setConnectionError(null);
        setDataIssues([]);
        setRunDetail(null);
        setVehiclesPayload(null);
        setRouteMetrics(null);
        setQuality(null);
        setAlertsPayload(null);
        setPipeline(null);
        setGeometry(null);
        setStops([]);
        setMeta(latestMeta);
        setWarnings(collected);
        setSnapshot({ meta: latestMeta, runs: list });
        return;
      }

      const jobs = {
        run: apiGet(`/runs/${encodeURIComponent(runId)}`),
        vehicles: apiGet("/vehicles", {
          run_id: runId,
          route_id: routeId || undefined,
          limit: 2000,
        }),
        quality: apiGet("/quality", { run_id: runId }),
        alerts: apiGet("/alerts", { run_id: runId, route_id: routeId || undefined }),
        pipeline: apiGet("/pipeline/status", { run_id: runId }),
      };
      if (routeId) {
        jobs.metrics = apiGet(`/routes/${encodeURIComponent(routeId)}/metrics`, { run_id: runId });
        jobs.geometry = apiGet(`/routes/${encodeURIComponent(routeId)}/geometry`, { run_id: runId });
        jobs.stops = apiGet(`/routes/${encodeURIComponent(routeId)}/stops`, { run_id: runId });
      }
      const settled = await Promise.all(
        Object.entries(jobs).map(async ([key, promise]) => {
          try {
            const result = await promise;
            note(result.log);
            collected.push(...(result.warnings || []));
            return [key, result];
          } catch (err) {
            note(err.log || { at: new Date().toISOString(), path: key, status: err.status || 0, warnings: [], error: err.message });
            return [key, { error: err }];
          }
        }),
      );
      if (token !== seq.current) return;
      const bag = Object.fromEntries(settled);
      const issues = settled
        .filter(([, value]) => value.error)
        .map(([key, value]) => `${key}: ${value.error.message}`);
      const offline = settled.every(([, value]) => value.error && value.error.status === 0);
      setConnectionError(offline ? "Không kết nối được API. Kiểm tra FastAPI đang chạy ở cổng 8000." : null);
      setDataIssues(offline ? [] : issues);
      if (bag.run?.data) setRunDetail(bag.run.data);
      if (bag.vehicles?.data) setVehiclesPayload(bag.vehicles.data);
      if (bag.quality?.data) setQuality(bag.quality.data);
      if (bag.alerts?.data) setAlertsPayload(bag.alerts.data);
      if (bag.pipeline?.data) setPipeline(bag.pipeline.data);
      if (routeId) {
        setRouteMetrics(bag.metrics?.data || null);
        setGeometry(bag.geometry?.data || null);
        setStops(bag.stops?.data?.stops || []);
      } else {
        setRouteMetrics(null);
        setGeometry(null);
        setStops([]);
      }
      latestMeta = bag.vehicles?.meta || bag.pipeline?.meta || bag.run?.meta || latestMeta;
      setMeta(latestMeta);
      setWarnings(dedupeWarnings(collected));
      setSnapshot({
        meta: latestMeta,
        warnings: dedupeWarnings(collected),
        run: bag.run?.data || null,
        vehicles: bag.vehicles?.data || null,
        metrics: bag.metrics?.data || null,
        quality: bag.quality?.data || null,
        alerts: bag.alerts?.data || null,
        pipeline: bag.pipeline?.data || null,
        geometry: bag.geometry?.data || null,
        stops: bag.stops?.data || null,
      });
    } catch (err) {
      if (token !== seq.current) return;
      note(err.log || { at: new Date().toISOString(), path: "refresh", status: err.status || 0, warnings: [], error: err.message });
      setConnectionError(err.message || "Không kết nối được API");
    } finally {
      if (token === seq.current) {
        setApiLogs((prev) => [...prev, ...logs].slice(-120));
        setIsSyncing(false);
      }
    }
  };

  useEffect(() => {
    let stopped = false;
    const tick = () => {
      if (!stopped) refreshRef.current();
    };
    tick();
    const timer = autoRefresh ? setInterval(tick, REFRESH_MS) : null;
    return () => {
      stopped = true;
      if (timer) clearInterval(timer);
    };
  }, [selectedRunId, selectedRouteId, autoRefresh]);

  useEffect(() => {
    const onKey = (event) => {
      if ((event.metaKey || event.ctrlKey) && event.key === "k") {
        event.preventDefault();
        setIsCommandPaletteOpen((open) => !open);
      }
      if (event.key === "Escape") {
        setIsCommandPaletteOpen(false);
        setIsSnapshotModalOpen(false);
        setIsTerminalLogsOpen(false);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const handleSyncFeed = useCallback(() => {
    refreshRef.current();
  }, []);

  const fetchPredictions = useCallback(async (stopId, options = {}) => {
    const runId = runIdRef.current;
    if (!runId || !stopId) return null;
    const result = await apiGet(`/stops/${encodeURIComponent(stopId)}/predictions`, {
      run_id: runId,
      route_id: routeIdRef.current || undefined,
      direction_id: options.directionId,
      include: options.include || "displayable",
      limit: 200,
    });
    setApiLogs((prev) => [...prev, result.log].slice(-120));
    return result;
  }, []);

  const currentRunRaw = runs.find((run) => run.run_id === selectedRunId) || runDetail || runs[0] || EMPTY_RUN;
  const currentRoute = routes.find((route) => route.route_id === selectedRouteId) || routes[0] || null;
  const vehicles = vehiclesPayload?.vehicles || [];
  const serviceAlerts = alertsPayload?.alerts || [];

  const currentRun = useMemo(
    () => ({
      id: currentRunRaw.run_id || "",
      shortId: shortId(currentRunRaw.run_id),
      mode: currentRunRaw.source_type || "—",
      status: currentRunRaw.run_state || runDetail?.run_state || "—",
      gtfsHash: currentRunRaw.gtfs_version || runDetail?.gtfs_version || "—",
      label: currentRunRaw.run_id
        ? `${currentRunRaw.source_type || "run"} ${shortId(currentRunRaw.run_id)}`
        : "Chưa có run",
      raw: currentRunRaw,
    }),
    [currentRunRaw, runDetail],
  );

  const currentRouteView = useMemo(
    () => ({
      id: currentRoute?.route_id || "",
      name: currentRoute ? routeLabel(currentRoute) : "Chưa có tuyến",
      longName: currentRoute?.long_name || currentRoute?.short_name || currentRoute?.route_id || "—",
      raw: currentRoute,
    }),
    [currentRoute],
  );

  const feedState = useMemo(
    () => ({
      vp: feedView(meta, "vehicle_positions", "VehiclePositions"),
      tu: feedView(meta, "trip_updates", "TripUpdates"),
      alerts: feedView(meta, "alerts", "Alerts"),
    }),
    [meta],
  );

  const value = {
    runs,
    routes,
    selectedRunId,
    setSelectedRunId,
    selectedRouteId,
    setSelectedRouteId,
    currentRun,
    currentRoute: currentRouteView,
    activeTab,
    setActiveTab,
    selectedVehicleId,
    setSelectedVehicleId,
    autoRefresh,
    setAutoRefresh,
    vehicles,
    vehicleCounts: vehiclesPayload?.counts || null,
    vehiclesPayload,
    routeMetrics,
    quality,
    serviceAlerts,
    alertsPayload,
    pipeline,
    geometry,
    stops,
    meta,
    warnings,
    feedState,
    apiLogs,
    connectionError,
    dataIssues,
    snapshot,
    isSyncing,
    handleSyncFeed,
    fetchPredictions,
    formattedWallClock: meta?.application_clock || "—",
    totalAlerts: warnings.length + serviceAlerts.length,
    isCommandPaletteOpen,
    setIsCommandPaletteOpen,
    isSnapshotModalOpen,
    setIsSnapshotModalOpen,
    isTerminalLogsOpen,
    setIsTerminalLogsOpen,
  };

  return <AppContext.Provider value={value}>{children}</AppContext.Provider>;
}

export function useApp() {
  const context = useContext(AppContext);
  if (!context) throw new Error("useApp must be used within an AppProvider");
  return context;
}
