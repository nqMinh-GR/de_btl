import React, { useEffect, useRef } from "react";
import { useApp } from "../AppContext";

export default function TerminalLogsDrawer() {
  const { isTerminalLogsOpen, setIsTerminalLogsOpen, currentRun, apiLogs } = useApp();
  const logContainerRef = useRef(null);

  useEffect(() => {
    if (logContainerRef.current) {
      logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight;
    }
  }, [apiLogs, isTerminalLogsOpen]);

  if (!isTerminalLogsOpen) return null;

  return (
    <div className="fixed bottom-0 right-0 left-60 z-30 bg-[#090D11] border-t border-outline-variant shadow-2xl flex flex-col font-mono max-h-[380px]">
      <div className="px-4 py-2 bg-surface-container border-b border-outline-variant flex items-center justify-between text-[11px]">
        <div className="flex items-center gap-2">
          <span className="w-2 h-2 rounded-full bg-primary"></span>
          <span className="text-on-surface font-semibold">Nhật ký request API</span>
          <span className="text-secondary text-[10px]">Run {currentRun.shortId}</span>
        </div>
        <button onClick={() => setIsTerminalLogsOpen(false)} className="text-on-surface-variant">
          Đóng
        </button>
      </div>
      <div ref={logContainerRef} className="overflow-auto p-3 text-[11px] space-y-1 min-h-[160px]">
        {apiLogs.length === 0 && <div className="text-outline">Chưa có request.</div>}
        {apiLogs.map((entry, index) => (
          <div key={`${entry.at}-${entry.path}-${index}`} className={entry.status >= 400 || entry.status === 0 ? "text-tertiary" : "text-on-surface"}>
            [{entry.at}] {entry.status || "ERR"} {entry.path} {entry.ms != null ? `${entry.ms}ms` : ""}
            {entry.error ? ` — ${entry.error}` : ""}
            {entry.warnings?.length ? ` — ${entry.warnings.join(" | ")}` : ""}
          </div>
        ))}
      </div>
    </div>
  );
}
