"""Map rendering without any basemap (offline-safe, no Mapbox token).

* ``build_deck``: ``pdk.Deck(map_provider=None, map_style=None)`` with a PathLayer for route shapes, a
  ScatterplotLayer for stops and one for vehicles (fresh green, stale orange, unknown grey; invalid vehicles
  are never plotted). Tooltip strings are HTML-escaped before deck.gl interpolates them.
* ``render_svg``: a self-contained SVG schematic (equirectangular with cos(lat0)) used when the
  ``svg`` renderer is selected — no JavaScript or network assets at all.
* ``history_deck``: observed path of one vehicle (PathLayer + ScatterplotLayer).

Coordinates come only from API responses; non-finite or out-of-range coordinates are dropped.
"""

from __future__ import annotations

import html
import math
from collections.abc import Sequence
from typing import Any

SYNTHETIC_NOTICE = "Dữ liệu kiểm thử — không phải MBTA"
MAP_LABEL = "Lược đồ tuyến theo tọa độ (không có bản đồ nền)"
FRESHNESS_RGBA = {
    "fresh": [26, 152, 80, 230],
    "stale": [240, 140, 0, 230],
    "unknown": [140, 140, 140, 230],
}
FRESHNESS_HEX = {"fresh": "#1a9850", "stale": "#f08c00", "unknown": "#8c8c8c"}
FRESHNESS_LABEL = {"fresh": "FRESH", "stale": "STALE", "unknown": "KHÔNG RÕ TUỔI", "invalid": "INVALID"}
DIRECTION_RGBA = {0: [74, 111, 165, 220], 1: [138, 90, 168, 220]}
DEFAULT_PATH_RGBA = [91, 107, 127, 220]


def _esc(value: Any) -> str:
    return "—" if value is None else html.escape(str(value), quote=True)


def _num(row: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = row.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value)):
            return float(value)
    return None


def lonlat(lon: Any, lat: Any) -> tuple[float, float] | None:
    if not all(isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(float(v)) for v in (lon, lat)):
        return None
    lon_f, lat_f = float(lon), float(lat)
    if not (-180.0 <= lon_f <= 180.0 and -90.0 <= lat_f <= 90.0):
        return None
    return lon_f, lat_f


def vehicle_class(v: dict[str, Any]) -> str:
    status = str(v.get("freshness_status") or "unknown").lower()
    return status if status in ("fresh", "stale", "unknown", "invalid") else "unknown"


def shape_paths(shapes: Sequence[dict[str, Any]]) -> list[tuple[dict[str, Any], list[tuple[float, float]]]]:
    out = []
    for shape in shapes:
        pts = []
        for p in shape.get("path") or []:
            if isinstance(p, (list, tuple)) and len(p) >= 2:
                ll = lonlat(p[0], p[1])
                if ll is not None:
                    pts.append(ll)
        if len(pts) >= 2:
            out.append((shape, pts))
    return out


def stop_points(stops: Sequence[dict[str, Any]]) -> list[tuple[dict[str, Any], tuple[float, float]]]:
    out = []
    for s in stops:
        ll = lonlat(_num(s, "longitude", "stop_lon"), _num(s, "latitude", "stop_lat"))
        if ll is not None:
            out.append((s, ll))
    return out


def vehicle_points(vehicles: Sequence[dict[str, Any]]) -> list[tuple[dict[str, Any], tuple[float, float], str]]:
    out = []
    for v in vehicles:
        cls = vehicle_class(v)
        if cls == "invalid":
            continue  # listed in the table, never plotted
        ll = lonlat(_num(v, "longitude"), _num(v, "latitude"))
        if ll is not None:
            out.append((v, ll, cls))
    return out


def vehicle_tooltip(v: dict[str, Any], synthetic: bool, *, html_breaks: bool = True) -> str:
    age = v.get("observation_age_seconds")
    age_text = f"{float(age):.1f} s" if isinstance(age, (int, float)) and not isinstance(age, bool) else "—"
    flags = v.get("quality_flags") or []
    label = v.get("label")
    lines = []
    if synthetic:
        lines.append(SYNTHETIC_NOTICE)
    lines += [
        f"Xe {v.get('vehicle_id')}" + (f" ({label})" if label else ""),
        f"Chuyến: {v.get('trip_id') or '—'}",
        f"Quan sát lúc: {v.get('observation_time') or 'không có timestamp'}",
        f"Tuổi: {age_text}",
        f"Trạng thái: {FRESHNESS_LABEL.get(vehicle_class(v), '—')}",
        f"Cờ chất lượng: {', '.join(str(f) for f in flags) if flags else 'không có'}",
    ]
    escaped = [_esc(line) for line in lines]
    return "<br/>".join(escaped) if html_breaks else "\n".join(escaped)


def _view_state(points: Sequence[tuple[float, float]]) -> dict[str, float]:
    if not points:
        return {"longitude": -71.06, "latitude": 42.36, "zoom": 10.0}  # Boston; only used when nothing to draw
    lons = [p[0] for p in points]
    lats = [p[1] for p in points]
    span = max(max(lons) - min(lons), (max(lats) - min(lats)) * 1.4, 1e-4)
    zoom = math.log2(360.0 * 900.0 / (512.0 * span * 1.2))
    return {
        "longitude": (min(lons) + max(lons)) / 2.0,
        "latitude": (min(lats) + max(lats)) / 2.0,
        "zoom": max(2.0, min(17.0, zoom)),
    }


def build_deck(shapes: Sequence[dict[str, Any]], stops: Sequence[dict[str, Any]],
               vehicles: Sequence[dict[str, Any]], *, synthetic: bool = False) -> Any:
    import pydeck as pdk

    paths = shape_paths(shapes)
    stop_pts = stop_points(stops)
    veh_pts = vehicle_points(vehicles)
    path_data = []
    for shape, pts in paths:
        dirs = shape.get("direction_ids") or []
        color = DIRECTION_RGBA.get(dirs[0], DEFAULT_PATH_RGBA) if len(dirs) == 1 else DEFAULT_PATH_RGBA
        path_data.append({
            "path": [[lon, lat] for lon, lat in pts],
            "color": color,
            "tooltip": _esc(f"Shape {shape.get('shape_id')} · hướng {','.join(str(d) for d in dirs) or '—'}"),
        })
    stop_data = [
        {"position": [ll[0], ll[1]], "tooltip": _esc(f"Trạm {s.get('stop_name') or '—'} ({s.get('stop_id')})")}
        for s, ll in stop_pts
    ]
    vehicle_data = [
        {"position": [ll[0], ll[1]], "color": FRESHNESS_RGBA[cls], "tooltip": vehicle_tooltip(v, synthetic)}
        for v, ll, cls in veh_pts
    ]
    layers = [
        pdk.Layer("PathLayer", id="shapes", data=path_data, get_path="path", get_color="color",
                  width_min_pixels=3, width_units="pixels", get_width=3, pickable=True),
        pdk.Layer("ScatterplotLayer", id="stops", data=stop_data, get_position="position",
                  get_fill_color=[255, 255, 255, 255], get_line_color=[36, 41, 47, 255], stroked=True,
                  line_width_min_pixels=1, get_radius=4, radius_units="pixels", radius_min_pixels=3,
                  pickable=True),
        pdk.Layer("ScatterplotLayer", id="vehicles", data=vehicle_data, get_position="position",
                  get_fill_color="color", get_line_color=[255, 255, 255, 255], stroked=True,
                  line_width_min_pixels=1, get_radius=7, radius_units="pixels", radius_min_pixels=6,
                  pickable=True),
    ]
    points = [ll for _s, pts in paths for ll in pts] + [ll for _s, ll in stop_pts] + [ll for _v, ll, _c in veh_pts]
    view = pdk.ViewState(**_view_state(points), pitch=0, bearing=0)
    return pdk.Deck(
        layers=layers,
        initial_view_state=view,
        map_provider=None,  # no basemap tiles, no Mapbox token
        map_style=None,
        tooltip={"html": "{tooltip}", "style": {"fontSize": "12px", "backgroundColor": "#24292f", "color": "#ffffff"}},
    )


def history_deck(observations: Sequence[dict[str, Any]], *, synthetic: bool = False) -> Any:
    import pydeck as pdk

    ordered = sorted((o for o in observations if isinstance(o, dict)), key=lambda o: str(o.get("event_time") or ""))
    pts = []
    data = []
    for o in ordered:
        ll = lonlat(_num(o, "longitude"), _num(o, "latitude"))
        if ll is None:
            continue
        pts.append(ll)
        tip = [SYNTHETIC_NOTICE] if synthetic else []
        tip += [f"Lúc: {o.get('event_time')}", f"Chuyến: {o.get('trip_id') or '—'}",
                f"Trạm: {o.get('stop_id') or '—'}", f"Bản ghi: {o.get('record_kind') or '—'}"]
        data.append({"position": [ll[0], ll[1]], "tooltip": "<br/>".join(_esc(t) for t in tip)})
    layers = [
        pdk.Layer("PathLayer", id="observed_path", data=[{"path": [[lon, lat] for lon, lat in pts]}] if len(pts) >= 2 else [],
                  get_path="path", get_color=[9, 105, 218, 220], width_min_pixels=3, width_units="pixels", get_width=3),
        pdk.Layer("ScatterplotLayer", id="observations", data=data, get_position="position",
                  get_fill_color=[9, 105, 218, 255], get_radius=5, radius_units="pixels", radius_min_pixels=4,
                  pickable=True),
    ]
    return pdk.Deck(layers=layers, initial_view_state=pdk.ViewState(**_view_state(pts)), map_provider=None,
                    map_style=None, tooltip={"html": "{tooltip}"})


def render_svg(shapes: Sequence[dict[str, Any]], stops: Sequence[dict[str, Any]],
               vehicles: Sequence[dict[str, Any]], *, synthetic: bool = False,
               width: int = 800, height: int = 500, pad: int = 28) -> str:
    """Self-contained SVG document (escaped <title> tooltips). Returns '' when nothing can be drawn."""
    paths = shape_paths(shapes)
    stop_pts = stop_points(stops)
    veh_pts = vehicle_points(vehicles)
    points = [ll for _s, pts in paths for ll in pts] + [ll for _s, ll in stop_pts] + [ll for _v, ll, _c in veh_pts]
    if not points:
        return ""
    min_lon, max_lon = min(p[0] for p in points), max(p[0] for p in points)
    min_lat, max_lat = min(p[1] for p in points), max(p[1] for p in points)
    k = max(math.cos(math.radians((min_lat + max_lat) / 2.0)), 0.01)
    span_x, span_y = (max_lon - min_lon) * k, max_lat - min_lat
    scale = min((width - 2 * pad) / span_x if span_x > 0 else math.inf,
                (height - 2 * pad) / span_y if span_y > 0 else math.inf)
    if not math.isfinite(scale):
        scale = 1.0
    off_x, off_y = (width - span_x * scale) / 2.0, (height - span_y * scale) / 2.0

    def xy(ll: tuple[float, float]) -> tuple[float, float]:
        return off_x + (ll[0] - min_lon) * k * scale, off_y + (max_lat - ll[1]) * scale

    out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="100%" '
           f'style="max-width:{width}px;background:#fbfcfd;border:1px solid #d0d7de;border-radius:6px;'
           f'font-family:sans-serif" role="img" aria-label="{_esc(MAP_LABEL)}">']
    for shape, pts in paths:
        dirs = shape.get("direction_ids") or []
        color = {0: "#4a6fa5", 1: "#8a5aa8"}.get(dirs[0], "#5b6b7f") if len(dirs) == 1 else "#5b6b7f"
        coords = " ".join("%.1f,%.1f" % xy(ll) for ll in pts)
        out.append(f'<polyline points="{coords}" fill="none" stroke="{color}" stroke-width="2.5" '
                   f'stroke-opacity="0.85"><title>{_esc("Shape " + str(shape.get("shape_id")))}</title></polyline>')
    for s, ll in stop_pts:
        x, y = xy(ll)
        out.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.5" fill="#ffffff" stroke="#24292f" stroke-width="1.2">'
                   f'<title>{_esc("Trạm " + str(s.get("stop_name") or "—") + " (" + str(s.get("stop_id")) + ")")}</title>'
                   f"</circle>")
    for v, ll, cls in veh_pts:
        x, y = xy(ll)
        out.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="6.5" fill="{FRESHNESS_HEX[cls]}" stroke="#ffffff" '
                   f'stroke-width="1.5"><title>{vehicle_tooltip(v, synthetic, html_breaks=False)}</title></circle>')
    legend = [("fresh", "Xe fresh"), ("stale", "Xe stale"), ("unknown", "Xe không rõ tuổi")]
    out.append(f'<rect x="8" y="8" width="150" height="{18 * (len(legend) + 1) + 8}" rx="4" fill="#ffffff" '
               f'fill-opacity="0.9" stroke="#d0d7de"/>')
    for i, (cls, text) in enumerate(legend):
        cy = 24 + 18 * i
        out.append(f'<circle cx="22" cy="{cy}" r="5.5" fill="{FRESHNESS_HEX[cls]}"/>'
                   f'<text x="34" y="{cy + 4}" font-size="12" fill="#1f2933">{_esc(text)}</text>')
    cy = 24 + 18 * len(legend)
    out.append(f'<circle cx="22" cy="{cy}" r="3.5" fill="#ffffff" stroke="#24292f"/>'
               f'<text x="34" y="{cy + 4}" font-size="12" fill="#1f2933">Trạm</text>')
    if synthetic:
        out.append(f'<text x="{width - 10}" y="{height - 10}" font-size="12" text-anchor="end" fill="#6f42c1">'
                   f"{_esc(SYNTHETIC_NOTICE)}</text>")
    out.append("</svg>")
    return "".join(out)
