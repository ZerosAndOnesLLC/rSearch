#!/usr/bin/env bash
# Generate a self-signed certificate for local development.
# Usage: scripts/dev-certs.sh [out-dir]   (default: ./certs)
set -euo pipefail

OUT="${1:-certs}"
mkdir -p "$OUT"

openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 365 \
  -keyout "$OUT/dev-key.pem" -out "$OUT/dev-cert.pem" \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

echo "wrote $OUT/dev-cert.pem and $OUT/dev-key.pem"
