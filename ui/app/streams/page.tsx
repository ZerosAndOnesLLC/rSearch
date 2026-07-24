"use client";

import { useEffect, useState } from "react";
import { StreamInfo, rq } from "@/lib/api";

export default function StreamsPage() {
  const [streams, setStreams] = useState<StreamInfo[]>([]);
  const [error, setError] = useState("");
  const [saved, setSaved] = useState("");

  const load = () =>
    rq<StreamInfo[]>("/_cat/indices")
      .then(setStreams)
      .catch((e) => setError(String((e as Error).message ?? e)));
  useEffect(() => {
    load();
  }, []);

  // Retention saves as soon as the value changes.
  async function saveRetention(name: string, hours: string) {
    try {
      await rq(`/_rsearch/streams/${encodeURIComponent(name)}/retention`, {
        method: "PUT",
        body: JSON.stringify({ hours: hours === "" ? null : Number(hours) }),
      });
      setSaved(name);
      setTimeout(() => setSaved(""), 1500);
      load();
    } catch (e) {
      setError(String((e as Error).message ?? e));
    }
  }

  return (
    <div className="flex flex-col gap-4 max-w-3xl">
      <h1 className="mono text-base">Streams</h1>
      {error && (
        <div className="panel p-3 text-sm" style={{ color: "var(--sev-err)" }}>
          {error}
        </div>
      )}
      <div className="panel">
        <table className="data">
          <thead>
            <tr>
              <th>Stream</th>
              <th>Documents</th>
              <th>Splits</th>
              <th>Retention (hours, blank keeps forever)</th>
            </tr>
          </thead>
          <tbody>
            {streams.map((stream) => (
              <tr key={stream.index}>
                <td className="mono">{stream.index}</td>
                <td className="mono">{Number(stream["docs.count"]).toLocaleString()}</td>
                <td className="mono">{stream.splits}</td>
                <td>
                  <input
                    className="field mono"
                    style={{ maxWidth: 140 }}
                    type="number"
                    min={0}
                    defaultValue={stream.retention_hours ?? ""}
                    onBlur={(e) => saveRetention(stream.index, e.target.value)}
                  />
                  {saved === stream.index && (
                    <span className="text-xs ml-2" style={{ color: "var(--sev-ok)" }}>
                      Saved
                    </span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
