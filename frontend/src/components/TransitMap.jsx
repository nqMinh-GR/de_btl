import React, { useEffect, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

const STYLE = {
  version: 8,
  sources: {
    carto: {
      type: "raster",
      tiles: ["https://basemaps.cartocdn.com/rastertiles/dark_all/{z}/{x}/{y}@2x.png?key=cb1_4kcj_1_0b72a5269b143db42fc94b0e"],
      tileSize: 256,
      attribution: "&copy; OpenStreetMap &copy; CARTO",
    },
  },
  layers: [{ id: "carto", type: "raster", source: "carto" }],
};

const FRESHNESS_COLOR = ["match", ["get", "freshness"], "fresh", "#4edea3", "stale", "#ffb95f", "unknown", "#9aa3ad", "#ff6b6b"];

function busImage() {
  const canvas = document.createElement("canvas");
  canvas.width = 32;
  canvas.height = 32;
  const ctx = canvas.getContext("2d");
  ctx.translate(16, 16);
  ctx.beginPath();
  ctx.moveTo(0, -12);
  ctx.lineTo(8, 10);
  ctx.lineTo(0, 5);
  ctx.lineTo(-8, 10);
  ctx.closePath();
  ctx.fillStyle = "#e7fff4";
  ctx.fill();
  return ctx.getImageData(0, 0, 32, 32);
}

function lineCollection(features) {
  return { type: "FeatureCollection", features };
}

function pointCollection(features) {
  return { type: "FeatureCollection", features };
}

function drawable(lat, lon) {
  return Number.isFinite(lat) && Number.isFinite(lon);
}

export default function TransitMap({
  shapes = [],
  stops = [],
  vehicles = [],
  trails = [],
  history = [],
  showStops = true,
  showVehicles = true,
  showTrails = true,
  selectedVehicleId = "",
  onSelectVehicle,
  fitKey = "map",
  className = "",
}) {
  const containerRef = useRef(null);
  const mapRef = useRef(null);
  const fittedRef = useRef("");
  const onSelectRef = useRef(onSelectVehicle);
  const [ready, setReady] = useState(false);
  onSelectRef.current = onSelectVehicle;

  useEffect(() => {
    const map = new maplibregl.Map({
      container: containerRef.current,
      style: STYLE,
      center: [-71.09, 42.35],
      zoom: 12,
      attributionControl: true,
    });
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");
    map.on("load", () => {
      if (!map.hasImage("bus-chevron")) map.addImage("bus-chevron", busImage(), { pixelRatio: 2 });
      map.addSource("shapes", { type: "geojson", data: lineCollection([]) });
      map.addSource("trails", { type: "geojson", data: lineCollection([]) });
      map.addSource("history", { type: "geojson", data: lineCollection([]) });
      map.addSource("stops", { type: "geojson", data: pointCollection([]) });
      map.addSource("vehicles", { type: "geojson", data: pointCollection([]) });
      map.addLayer({
        id: "shapes",
        type: "line",
        source: "shapes",
        paint: { "line-color": "#8bd6b6", "line-width": 3, "line-opacity": 0.85 },
      });
      map.addLayer({
        id: "trails",
        type: "line",
        source: "trails",
        paint: { "line-color": "#ffb95f", "line-width": 2, "line-opacity": 0.8 },
      });
      map.addLayer({
        id: "history",
        type: "line",
        source: "history",
        paint: { "line-color": "#4edea3", "line-width": 4, "line-opacity": 0.95 },
      });
      map.addLayer({
        id: "stops",
        type: "circle",
        source: "stops",
        paint: {
          "circle-radius": 4,
          "circle-color": "#f4f7f5",
          "circle-stroke-color": "#101418",
          "circle-stroke-width": 1,
        },
      });
      map.addLayer({
        id: "vehicles",
        type: "circle",
        source: "vehicles",
        paint: {
          "circle-radius": ["case", ["==", ["get", "vehicle_id"], ""], 9, 7],
          "circle-color": FRESHNESS_COLOR,
          "circle-stroke-color": "#101418",
          "circle-stroke-width": 1,
        },
      });
      map.addLayer({
        id: "vehicle-heading",
        type: "symbol",
        source: "vehicles",
        layout: {
          "icon-image": "bus-chevron",
          "icon-size": 0.55,
          "icon-rotate": ["coalesce", ["get", "bearing"], 0],
          "icon-rotation-alignment": "map",
          "icon-allow-overlap": true,
        },
      });
      map.on("click", "vehicles", (event) => {
        const feature = event.features?.[0];
        const id = feature?.properties?.vehicle_id;
        if (id) onSelectRef.current?.(id);
      });
      map.on("mouseenter", "vehicles", () => {
        map.getCanvas().style.cursor = "pointer";
      });
      map.on("mouseleave", "vehicles", () => {
        map.getCanvas().style.cursor = "";
      });
      mapRef.current = map;
      setReady(true);
      map.resize();
    });
    mapRef.current = map;
    return () => {
      setReady(false);
      map.remove();
      mapRef.current = null;
    };
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !ready) return;
    const shapeFeatures = shapes
      .filter((shape) => Array.isArray(shape.path) && shape.path.length > 1)
      .map((shape) => ({
        type: "Feature",
        properties: { shape_id: shape.shape_id },
        geometry: { type: "LineString", coordinates: shape.path },
      }));
    const trailFeatures = trails
      .filter((trail) => trail.coordinates?.length > 1)
      .map((trail) => ({
        type: "Feature",
        properties: { vehicle_id: trail.vehicle_id },
        geometry: { type: "LineString", coordinates: trail.coordinates },
      }));
    const historyFeature = history.length > 1
      ? [{ type: "Feature", properties: {}, geometry: { type: "LineString", coordinates: history } }]
      : [];
    const stopFeatures = stops
      .filter((stop) => drawable(stop.latitude, stop.longitude))
      .map((stop) => ({
        type: "Feature",
        properties: { stop_id: stop.stop_id, name: stop.stop_name || stop.stop_id },
        geometry: { type: "Point", coordinates: [stop.longitude, stop.latitude] },
      }));
    const vehicleFeatures = vehicles
      .filter((vehicle) => vehicle.freshness_status !== "invalid" && drawable(vehicle.latitude, vehicle.longitude))
      .map((vehicle) => ({
        type: "Feature",
        properties: {
          vehicle_id: vehicle.vehicle_id,
          freshness: vehicle.freshness_status,
          bearing: vehicle.bearing,
        },
        geometry: { type: "Point", coordinates: [vehicle.longitude, vehicle.latitude] },
      }));
    map.getSource("shapes")?.setData(lineCollection(shapeFeatures));
    map.getSource("trails")?.setData(lineCollection(showTrails ? trailFeatures : []));
    map.getSource("history")?.setData(lineCollection(historyFeature));
    map.getSource("stops")?.setData(pointCollection(showStops ? stopFeatures : []));
    map.getSource("vehicles")?.setData(pointCollection(showVehicles ? vehicleFeatures : []));
    map.setLayoutProperty("stops", "visibility", showStops ? "visible" : "none");
    map.setLayoutProperty("vehicles", "visibility", showVehicles ? "visible" : "none");
    map.setLayoutProperty("vehicle-heading", "visibility", showVehicles ? "visible" : "none");
    map.setLayoutProperty("trails", "visibility", showTrails ? "visible" : "none");
    map.setPaintProperty("vehicles", "circle-radius", [
      "case",
      ["==", ["get", "vehicle_id"], selectedVehicleId || ""],
      10,
      7,
    ]);
    map.setPaintProperty("vehicles", "circle-stroke-width", [
      "case",
      ["==", ["get", "vehicle_id"], selectedVehicleId || ""],
      3,
      1,
    ]);

    if (fittedRef.current !== fitKey) {
      const coords = [
        ...shapeFeatures.flatMap((feature) => feature.geometry.coordinates),
        ...stopFeatures.map((feature) => feature.geometry.coordinates),
        ...vehicleFeatures.map((feature) => feature.geometry.coordinates),
        ...history,
      ];
      if (coords.length) {
        const bounds = coords.reduce(
          (box, coord) => box.extend(coord),
          new maplibregl.LngLatBounds(coords[0], coords[0]),
        );
        map.fitBounds(bounds, { padding: 48, maxZoom: 15, duration: 0 });
        fittedRef.current = fitKey;
      }
    }
  }, [ready, shapes, stops, vehicles, trails, history, showStops, showVehicles, showTrails, selectedVehicleId, fitKey]);

  return <div ref={containerRef} className={`h-full w-full ${className}`} />;
}
