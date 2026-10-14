const DEFAULT_BASE = "/api/v1";

export function apiBase() {
  const raw = String(import.meta.env.VITE_API_BASE ?? DEFAULT_BASE).trim();
  return (raw || DEFAULT_BASE).replace(/\/$/, "");
}

export class ApiError extends Error {
  constructor(message, fields = {}) {
    super(message);
    this.name = "ApiError";
    this.status = fields.status ?? 0;
    this.code = fields.code ?? "request_failed";
    this.requestId = fields.requestId ?? null;
    this.body = fields.body ?? null;
    this.log = fields.log ?? null;
  }
}

function buildUrl(path, params) {
  const base = apiBase();
  const suffix = path.startsWith("/") ? path : `/${path}`;
  const absolute = base.startsWith("http")
    ? `${base}${suffix}`
    : new URL(`${base}${suffix}`, window.location.origin).toString();
  const url = new URL(absolute);
  for (const [key, value] of Object.entries(params || {})) {
    if (value == null || value === "") continue;
    url.searchParams.set(key, String(value));
  }
  return url;
}

export async function apiGet(path, params) {
  const url = buildUrl(path, params);
  const started = performance.now();
  const logBase = {
    at: new Date().toISOString(),
    path: `${url.pathname}${url.search}`,
    warnings: [],
  };
  let response;
  try {
    response = await fetch(url.toString(), {
      method: "GET",
      headers: { Accept: "application/json" },
    });
  } catch (err) {
    const log = { ...logBase, status: 0, ms: Math.round(performance.now() - started), error: err.message };
    throw new ApiError("Không kết nối được API. Kiểm tra FastAPI đang chạy ở cổng 8000.", {
      status: 0,
      code: "network",
      log,
    });
  }
  const body = await response.json().catch(() => null);
  const log = {
    ...logBase,
    status: response.status,
    ms: Math.round(performance.now() - started),
    warnings: (body?.warnings || []).map((item) => item.message).filter(Boolean),
  };
  if (!response.ok || !body || body.error) {
    const error = body?.error || {};
    const proxyDown = !body && response.status >= 500;
    if (proxyDown) log.error = "proxy không tới được FastAPI :8000";
    throw new ApiError(
      proxyDown
        ? "Không kết nối được API. Kiểm tra FastAPI đang chạy ở cổng 8000."
        : error.message || `API trả về HTTP ${response.status}`,
      {
        status: proxyDown ? 0 : response.status,
        code: proxyDown ? "network" : error.code || "http_error",
        requestId: error.request_id || body?.meta?.request_id || null,
        body,
        log,
      },
    );
  }
  return { data: body.data, meta: body.meta || null, warnings: body.warnings || [], log };
}
