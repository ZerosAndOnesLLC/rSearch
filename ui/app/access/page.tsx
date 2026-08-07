"use client";

import { useEffect, useState } from "react";
import { rq } from "@/lib/api";

interface User { username: string; role: string; streams: string[] }
interface ApiKey { name: string; actions: string[]; streams: string[] }

export default function AccessPage() {
  const [users, setUsers] = useState<User[]>([]);
  const [keys, setKeys] = useState<ApiKey[]>([]);
  const [error, setError] = useState("");
  const [newKey, setNewKey] = useState("");
  const [userForm, setUserForm] = useState({ name: "", password: "", role: "user" });
  const [keyForm, setKeyForm] = useState({ name: "", actions: "ingest", streams: "*" });

  const load = (signal?: AbortSignal) => {
    rq<{ users: User[] }>("/_rsearch/users", { signal })
      .then((r) => setUsers(r.users))
      .catch((e) => {
        if (signal?.aborted) return;
        setError(String((e as Error).message ?? e));
      });
    rq<{ api_keys: ApiKey[] }>("/_rsearch/api_keys", { signal })
      .then((r) => setKeys(r.api_keys))
      .catch(() => {});
  };
  useEffect(() => {
    const controller = new AbortController();
    load(controller.signal);
    return () => controller.abort();
  }, []);

  async function saveUser(e: React.FormEvent) {
    e.preventDefault();
    try {
      await rq(`/_rsearch/users/${encodeURIComponent(userForm.name)}`, {
        method: "PUT",
        body: JSON.stringify({ password: userForm.password, role: userForm.role }),
      });
      setUserForm({ name: "", password: "", role: "user" });
      setError("");
      load();
    } catch (err) { setError(String((err as Error).message ?? err)); }
  }

  async function saveKey(e: React.FormEvent) {
    e.preventDefault();
    try {
      const result = await rq<{ key: string }>("/_rsearch/api_keys", {
        method: "POST",
        body: JSON.stringify({
          name: keyForm.name,
          actions: keyForm.actions.split(",").map((s) => s.trim()),
          streams: keyForm.streams.split(",").map((s) => s.trim()),
        }),
      });
      setNewKey(result.key);
      setKeyForm({ name: "", actions: "ingest", streams: "*" });
      setError("");
      load();
    } catch (err) { setError(String((err as Error).message ?? err)); }
  }

  return (
    <div className="flex flex-col gap-6 max-w-4xl">
      <h1 className="mono text-base">Access</h1>
      {error && (
        <div className="panel p-3 text-sm" style={{ color: "var(--sev-err)" }}>{error}</div>
      )}

      <section className="flex flex-col gap-3">
        <h2 className="text-sm" style={{ color: "var(--muted)" }}>Users</h2>
        <form className="panel p-4 grid grid-cols-2 md:grid-cols-4 gap-3" onSubmit={saveUser}>
          <div>
            <label>Username</label>
            <input className="field" value={userForm.name} required
              onChange={(e) => setUserForm({ ...userForm, name: e.target.value })} />
          </div>
          <div>
            <label>Password (12+ characters)</label>
            <input className="field" type="password" value={userForm.password} required
              onChange={(e) => setUserForm({ ...userForm, password: e.target.value })} />
          </div>
          <div>
            <label>Role</label>
            <select className="field" value={userForm.role}
              onChange={(e) => setUserForm({ ...userForm, role: e.target.value })}>
              <option value="user">user</option>
              <option value="admin">admin</option>
            </select>
          </div>
          <div className="self-end">
            <button className="btn" type="submit">Save user</button>
          </div>
        </form>
        <div className="panel">
          <table className="data">
            <thead><tr><th>Username</th><th>Role</th><th>Streams</th></tr></thead>
            <tbody>
              {users.map((user) => (
                <tr key={user.username}>
                  <td className="mono">{user.username}</td>
                  <td className="mono">{user.role}</td>
                  <td className="mono">{user.streams.join(", ")}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="flex flex-col gap-3">
        <h2 className="text-sm" style={{ color: "var(--muted)" }}>API keys</h2>
        {newKey && (
          <div className="panel p-3 text-sm">
            Copy this key now — it is shown once:{" "}
            <span className="mono" style={{ color: "var(--accent)" }}>{newKey}</span>
          </div>
        )}
        <form className="panel p-4 grid grid-cols-2 md:grid-cols-4 gap-3" onSubmit={saveKey}>
          <div>
            <label>Name</label>
            <input className="field" value={keyForm.name} required
              onChange={(e) => setKeyForm({ ...keyForm, name: e.target.value })} />
          </div>
          <div>
            <label>Actions (ingest, search, admin)</label>
            <input className="field mono" value={keyForm.actions}
              onChange={(e) => setKeyForm({ ...keyForm, actions: e.target.value })} />
          </div>
          <div>
            <label>Streams (* for all)</label>
            <input className="field mono" value={keyForm.streams}
              onChange={(e) => setKeyForm({ ...keyForm, streams: e.target.value })} />
          </div>
          <div className="self-end">
            <button className="btn" type="submit">Create key</button>
          </div>
        </form>
        <div className="panel">
          <table className="data">
            <thead><tr><th>Name</th><th>Actions</th><th>Streams</th><th></th></tr></thead>
            <tbody>
              {keys.map((key) => (
                <tr key={key.name}>
                  <td className="mono">{key.name}</td>
                  <td className="mono">{key.actions.join(", ")}</td>
                  <td className="mono">{key.streams.join(", ")}</td>
                  <td>
                    <button className="btn btn-quiet" onClick={async () => {
                      await rq(`/_rsearch/api_keys/${encodeURIComponent(key.name)}`, { method: "DELETE" });
                      load();
                    }}>Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
