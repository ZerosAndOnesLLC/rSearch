"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError, Hit, StreamInfo, rq, search } from "@/lib/api";
import { useRouter } from "next/navigation";

const RANGES = [
  { label: "15 min", millis: 15 * 60 * 1000 },
  { label: "1 hour", millis: 60 * 60 * 1000 },
  { label: "24 hours", millis: 24 * 60 * 60 * 1000 },
  { label: "7 days", millis: 7 * 24 * 60 * 60 * 1000 },
  { label: "All time", millis: 0 },
];

function severity(source: Record<string, unknown>): {
  code: string;
  color: string;
} {
  const level = String(
    source.level ?? source.severity ?? source.status ?? "",
  ).toLowerCase();
  if (["error", "err", "crit", "fatal", "0", "1", "2", "3"].includes(level))
    return { code: "ERR", color: "var(--sev-err)" };
  if (["warn", "warning", "4"].includes(level))
    return { code: "WRN", color: "var(--sev-warn)" };
  if (typeof source.status === "number" && (source.status as number) >= 500)
    return { code: "ERR", color: "var(--sev-err)" };
  return { code: "INF", color: "var(--sev-ok)" };
}

export default function SearchPage() {
  const router = useRouter();
  const [streams, setStreams] = useState<StreamInfo[]>([]);
  const [stream, setStream] = useState("");
  const [query, setQuery] = useState("");
  const [range, setRange] = useState(RANGES[2]);
  const [hits, setHits] = useState<Hit[]>([]);
  const [total, setTotal] = useState<number | null>(null);
  const [error, setError] = useState("");
  const [open, setOpen] = useState<string | null>(null);

  // Latest query text for auto-runs, tracked outside runSearch's identity
  // so typing doesn't re-create the callback (and fire the auto-run
  // effect) per keystroke.
  const queryRef = useRef(query);
  useEffect(() => {
    queryRef.current = query;
  }, [query]);
  // In-flight search; superseded/unmounted requests are aborted so a slow
  // older response can never overwrite a newer result.
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    rq<StreamInfo[]>("/_cat/indices", { signal: controller.signal })
      .then((list) => {
        setStreams(list);
        if (list.length) setStream((current) => current || list[0].index);
      })
      .catch((e: unknown) => {
        if (controller.signal.aborted) return;
        if (e instanceof ApiError && e.status === 401) router.push("/login");
        else setError(String((e as Error).message ?? e));
      });
    return () => controller.abort();
  }, [router]);

  const runSearch = useCallback(
    (q: string) => {
      if (!stream) return;
      abortRef.current?.abort();
      const controller = new AbortController();
      abortRef.current = controller;
      const from = range.millis ? Date.now() - range.millis : null;
      search(stream, q, from, 100, controller.signal)
        .then((result) => {
          setHits(result.hits);
          setTotal(result.total);
          setError("");
        })
        .catch((e: unknown) => {
          if (controller.signal.aborted) return;
          if (e instanceof ApiError && e.status === 401) router.push("/login");
          else setError(String((e as Error).message ?? e));
        });
    },
    [stream, range, router],
  );

  // Auto-run only when the stream or window changes — typing in the query
  // box searches on Enter or the button, not per keystroke.
  useEffect(() => {
    runSearch(queryRef.current);
    return () => abortRef.current?.abort();
  }, [runSearch]);

  return (
    <div className="flex flex-col gap-4">
      <div className="flex gap-3 items-end flex-wrap">
        <div className="w-44">
          <label htmlFor="stream">Stream</label>
          <select
            id="stream"
            className="field"
            value={stream}
            onChange={(e) => setStream(e.target.value)}
          >
            {streams.map((s) => (
              <option key={s.index} value={s.index}>
                {s.index}
              </option>
            ))}
          </select>
        </div>
        <div className="flex-1 min-w-64">
          <label htmlFor="query">Query</label>
          <input
            id="query"
            className="field mono"
            placeholder='status:500 AND "timeout"  (blank matches everything)'
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && runSearch(query)}
          />
        </div>
        <div className="w-32">
          <label htmlFor="range">Window</label>
          <select
            id="range"
            className="field"
            value={range.label}
            onChange={(e) =>
              setRange(RANGES.find((r) => r.label === e.target.value)!)
            }
          >
            {RANGES.map((r) => (
              <option key={r.label}>{r.label}</option>
            ))}
          </select>
        </div>
        <button className="btn" onClick={() => runSearch(query)}>
          Search
        </button>
      </div>

      {error && (
        <div className="panel p-3 text-sm" style={{ color: "var(--sev-err)" }}>
          {error}
        </div>
      )}
      {total !== null && (
        <div className="text-xs" style={{ color: "var(--muted)" }}>
          <span className="mono">{total.toLocaleString()}</span> matching
          documents{hits.length < total ? `, newest ${hits.length} shown` : ""}
        </div>
      )}

      <div className="panel overflow-x-auto">
        {hits.length === 0 && total !== null && (
          <div className="p-6 text-sm" style={{ color: "var(--muted)" }}>
            No documents in this window. Widen the window or clear the query.
          </div>
        )}
        {hits.map((hit) => {
          const sev = severity(hit._source);
          const ts = hit.sort?.[0]
            ? new Date(hit.sort[0]).toISOString().replace("T", " ").slice(0, 23)
            : "";
          const message = String(
            hit._source.message ?? JSON.stringify(hit._source),
          );
          return (
            <div key={hit._id}>
              <div
                className="ledger-row mono"
                onClick={() => setOpen(open === hit._id ? null : hit._id)}
              >
                <div className="sev-tick" style={{ background: sev.color }} />
                <div className="sev-code" style={{ color: sev.color }}>
                  {sev.code}
                </div>
                <div className="ts">{ts}</div>
                <div className="truncate">{message}</div>
              </div>
              {open === hit._id && (
                <pre
                  className="mono text-xs p-4 overflow-x-auto"
                  style={{ background: "var(--bg)", color: "var(--muted)" }}
                >
                  {JSON.stringify(hit._source, null, 2)}
                </pre>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
