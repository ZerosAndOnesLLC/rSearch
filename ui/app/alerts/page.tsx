"use client";

import { useEffect, useState } from "react";
import { rq } from "@/lib/api";

interface Alert {
  name: string;
  stream: string;
  condition_op: string;
  threshold: number;
  window_secs: number;
  interval_secs: number;
  webhook_url: string;
  enabled: boolean;
  last_status: string | null;
  last_count: number | null;
}

const EMPTY = {
  name: "",
  stream: "",
  query: "",
  condition_op: "gt",
  threshold: 0,
  window_secs: 300,
  interval_secs: 60,
  webhook_url: "",
};

export default function AlertsPage() {
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [form, setForm] = useState({ ...EMPTY });
  const [error, setError] = useState("");

  const load = (signal?: AbortSignal) =>
    rq<{ alerts: Alert[] }>("/_rsearch/alerts", { signal })
      .then((r) => setAlerts(r.alerts))
      .catch((e) => {
        if (signal?.aborted) return;
        setError(String((e as Error).message ?? e));
      });
  useEffect(() => {
    const controller = new AbortController();
    load(controller.signal);
    return () => controller.abort();
  }, []);

  async function save(e: React.FormEvent) {
    e.preventDefault();
    try {
      let query: unknown = {};
      if (form.query.trim())
        query = { query_string: { query: form.query.trim() } };
      await rq(`/_rsearch/alerts/${encodeURIComponent(form.name)}`, {
        method: "PUT",
        body: JSON.stringify({ ...form, query }),
      });
      setForm({ ...EMPTY });
      setError("");
      load();
    } catch (err) {
      setError(String((err as Error).message ?? err));
    }
  }

  async function remove(name: string) {
    await rq(`/_rsearch/alerts/${encodeURIComponent(name)}`, {
      method: "DELETE",
    }).catch((e) => setError(String((e as Error).message ?? e)));
    load();
  }

  return (
    <div className="flex flex-col gap-4 max-w-4xl">
      <h1 className="mono text-base">Alerts</h1>
      {error && (
        <div className="panel p-3 text-sm" style={{ color: "var(--sev-err)" }}>
          {error}
        </div>
      )}
      <form className="panel p-4 grid grid-cols-2 md:grid-cols-4 gap-3" onSubmit={save}>
        <div>
          <label>Name</label>
          <input className="field" value={form.name} required
            onChange={(e) => setForm({ ...form, name: e.target.value })} />
        </div>
        <div>
          <label>Stream</label>
          <input className="field" value={form.stream} required
            onChange={(e) => setForm({ ...form, stream: e.target.value })} />
        </div>
        <div className="col-span-2">
          <label>Query (blank matches everything)</label>
          <input className="field mono" value={form.query}
            placeholder="level:error"
            onChange={(e) => setForm({ ...form, query: e.target.value })} />
        </div>
        <div>
          <label>Fire when count is</label>
          <div className="flex gap-2">
            <select className="field" style={{ maxWidth: 90 }} value={form.condition_op}
              onChange={(e) => setForm({ ...form, condition_op: e.target.value })}>
              <option value="gt">above</option>
              <option value="lt">below</option>
            </select>
            <input className="field mono" type="number" value={form.threshold}
              onChange={(e) => setForm({ ...form, threshold: Number(e.target.value) })} />
          </div>
        </div>
        <div>
          <label>Window (seconds)</label>
          <input className="field mono" type="number" min={1} value={form.window_secs}
            onChange={(e) => setForm({ ...form, window_secs: Number(e.target.value) })} />
        </div>
        <div>
          <label>Check every (seconds)</label>
          <input className="field mono" type="number" min={1} value={form.interval_secs}
            onChange={(e) => setForm({ ...form, interval_secs: Number(e.target.value) })} />
        </div>
        <div>
          <label>Webhook URL</label>
          <input className="field mono" value={form.webhook_url} required
            placeholder="https://…"
            onChange={(e) => setForm({ ...form, webhook_url: e.target.value })} />
        </div>
        <div className="col-span-2 md:col-span-4">
          <button className="btn" type="submit">Save alert</button>
        </div>
      </form>
      <div className="panel">
        <table className="data">
          <thead>
            <tr>
              <th>Name</th><th>Stream</th><th>Condition</th>
              <th>Last run</th><th></th>
            </tr>
          </thead>
          <tbody>
            {alerts.map((alert) => (
              <tr key={alert.name}>
                <td className="mono">{alert.name}</td>
                <td className="mono">{alert.stream}</td>
                <td className="mono">
                  count {alert.condition_op} {alert.threshold} / {alert.window_secs}s
                </td>
                <td className="mono">
                  <span style={{
                    color: alert.last_status === "fired" ? "var(--sev-err)" : "var(--muted)",
                  }}>
                    {alert.last_status ?? "not yet run"}
                    {alert.last_count !== null ? ` (${alert.last_count})` : ""}
                  </span>
                </td>
                <td>
                  <button className="btn btn-quiet" onClick={() => remove(alert.name)}>
                    Delete
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
