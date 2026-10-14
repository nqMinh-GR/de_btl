import React, { useState } from "react";
import { useApp } from "../AppContext";

export default function SnapshotLogModal() {
  const {
    isSnapshotModalOpen,
    setIsSnapshotModalOpen,
    snapshot,
  } = useApp();
  const [copied, setCopied] = useState(false);

  if (!isSnapshotModalOpen) return null;

  const jsonString = JSON.stringify(snapshot || { note: "Chưa có payload API" }, null, 2);

  const handleCopy = () => {
    navigator.clipboard.writeText(jsonString);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 backdrop-blur-sm animate-fade-in font-mono">
      <div className="bg-surface-container-low border border-outline-variant rounded-lg w-full max-w-3xl max-h-[85vh] flex flex-col shadow-2xl overflow-hidden">
        {/* Header */}
        <div className="px-4 py-3 bg-surface-container border-b border-outline-variant flex items-center justify-between">
          <div className="flex items-center gap-2">
            <span className="material-symbols-outlined text-primary text-[18px]">
              terminal
            </span>
            <span className="text-headline-md font-headline font-semibold text-on-surface text-[14px]">
              Snapshot Telemetry JSON Inspector
            </span>
            <span className="text-[10px] px-1.5 py-0.5 rounded bg-surface-container-highest border border-outline text-secondary font-bold">
              GEN 4 READY
            </span>
          </div>
          <button
            onClick={() => setIsSnapshotModalOpen(false)}
            className="text-on-surface-variant hover:text-on-surface transition-colors p-1"
          >
            <span className="material-symbols-outlined text-[18px]">close</span>
          </button>
        </div>

        {/* Info Toolbar */}
        <div className="px-4 py-2 bg-surface-container-lowest border-b border-outline-variant/60 flex items-center justify-between text-[11px] text-on-surface-variant">
          <div className="flex items-center gap-3">
            <span>
              Snapshot ID:{" "}
              <code className="text-primary font-bold">
                {feedState.vp.snapshotId}
              </code>
            </span>
            <span>•</span>
            <span>
              Route:{" "}
              <strong className="text-on-surface">{currentRoute.id}</strong>
            </span>
          </div>
          <button
            onClick={handleCopy}
            className="inline-flex items-center gap-1 px-2.5 py-1 rounded bg-surface-container-high border border-outline-variant hover:border-primary text-on-surface text-[11px] transition-colors cursor-pointer"
          >
            <span className="material-symbols-outlined text-[13px] text-primary">
              {copied ? "check" : "content_copy"}
            </span>
            <span>{copied ? "Đã sao chép!" : "Copy JSON"}</span>
          </button>
        </div>

        {/* JSON Viewer */}
        <div className="flex-1 overflow-auto p-4 bg-[#090D11] text-[12px] leading-relaxed select-text">
          <pre className="text-on-surface font-mono">{jsonString}</pre>
        </div>

        {/* Footer */}
        <div className="px-4 py-2.5 bg-surface-container border-t border-outline-variant flex items-center justify-between text-[11px] text-outline">
          <span>Payload API vừa nhận · envelope data/meta/warnings</span>
          <button
            onClick={() => setIsSnapshotModalOpen(false)}
            className="px-3 py-1 rounded bg-surface-container-highest border border-outline-variant text-on-surface hover:bg-surface-bright transition-colors"
          >
            Đóng
          </button>
        </div>
      </div>
    </div>
  );
}
