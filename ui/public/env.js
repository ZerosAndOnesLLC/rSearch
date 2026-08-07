// Runtime API configuration, loaded before the app bundle. Replace this
// file at deploy time (bind-mount, S3 object overwrite, or a container
// entrypoint that writes it) to point an already-built console at a
// different rSearch API — no rebuild needed.
//
//   window.__RSEARCH_API__ = "https://rsearch.example.com:9200";
//   window.__RSEARCH_API__ = ""; // same origin (console proxied with the API)
//
// When unset, the build-time NEXT_PUBLIC_RSEARCH_API value applies
// (default http://localhost:9200).
