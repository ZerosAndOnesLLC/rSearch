"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { rq, setToken } from "@/lib/api";

export default function LoginPage() {
  const router = useRouter();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");

  async function signIn(e: React.FormEvent) {
    e.preventDefault();
    try {
      const result = await rq<{ token: string }>("/_rsearch/login", {
        method: "POST",
        body: JSON.stringify({ username, password }),
      });
      setToken(result.token);
      router.push("/");
    } catch (err) {
      setError(String((err as Error).message ?? err));
    }
  }

  return (
    <div className="max-w-sm mx-auto mt-24">
      <div className="mono text-lg mb-6">
        r<span style={{ color: "var(--accent)" }}>Search</span> sign in
      </div>
      <form className="panel p-5 flex flex-col gap-4" onSubmit={signIn}>
        <div>
          <label htmlFor="user">Username</label>
          <input id="user" className="field" value={username} autoFocus
            onChange={(e) => setUsername(e.target.value)} />
        </div>
        <div>
          <label htmlFor="pass">Password</label>
          <input id="pass" className="field" type="password" value={password}
            onChange={(e) => setPassword(e.target.value)} />
        </div>
        {error && (
          <div className="text-sm" style={{ color: "var(--sev-err)" }}>{error}</div>
        )}
        <button className="btn" type="submit">Sign in</button>
      </form>
    </div>
  );
}
