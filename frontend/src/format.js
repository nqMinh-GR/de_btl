export function dash(value) {
  if (value == null || value === "") return "—";
  return String(value);
}

export function shortId(value) {
  if (!value) return "—";
  const text = String(value);
  return text.length > 12 ? `${text.slice(0, 8)}…` : text;
}

export function fmtClock(value) {
  if (!value) return "—";
  return String(value).replace("T", " ").replace(/\.\d+Z$/, " UTC").replace(/Z$/, " UTC");
}

export function fmtSeconds(value) {
  if (value == null || Number.isNaN(Number(value))) return "—";
  const number = Number(value);
  const abs = Math.abs(number);
  if (abs < 90) return `${number.toFixed(1)} s`;
  const minutes = Math.floor(abs / 60);
  const seconds = Math.round(abs % 60);
  return `${number.toFixed(0)} s (${minutes} phút ${seconds} giây)`;
}

export function fmtRate(value) {
  if (value == null || Number.isNaN(Number(value))) return "—";
  return `${(Number(value) * 100).toFixed(1)}%`;
}

export function routeLabel(route) {
  if (!route) return "—";
  if (route.short_name && route.long_name) return `${route.short_name} — ${route.long_name}`;
  return route.long_name || route.short_name || route.route_id || "—";
}

const QUALITY_LABEL = {
  realtime: "REALTIME",
  schedule_only: "CHỈ THEO LỊCH",
  degraded: "DEGRADED",
  unknown_uncertainty: "KHÔNG RÕ ĐỘ BẤT ĐỊNH",
  invalid: "INVALID",
};

export function qualityLabel(value) {
  if (!value) return "—";
  return QUALITY_LABEL[value] || String(value).toUpperCase();
}

export function delayText(seconds) {
  if (seconds == null || Number.isNaN(Number(seconds))) return "—";
  const number = Number(seconds);
  const sign = number > 0 ? "+" : "";
  return `${sign}${number} s`;
}

export function etaMinutes(predicted, clock) {
  if (!predicted || !clock) return null;
  const delta = new Date(predicted).getTime() - new Date(clock).getTime();
  if (Number.isNaN(delta)) return null;
  return Math.max(0, Math.round(delta / 60000));
}

export function feedView(meta, key, name) {
  const feed = meta?.feeds?.[key] || {};
  return {
    key,
    name,
    status: String(feed.state || "unavailable").toUpperCase(),
    latencySeconds: feed.source_age_seconds,
    snapshotId: feed.snapshot_id || "—",
    feedTimestamp: fmtClock(feed.feed_timestamp),
    metricsAsOf: fmtClock(feed.metrics_as_of),
    stateDesc: feed.detail || feed.state || "—",
    thresholdSeconds: feed.stale_threshold_seconds,
    stale: Boolean(feed.stale),
  };
}
