#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

BUNDLE_PATH="${1:-}"

if [[ -n "${BUNDLE_PATH}" ]]; then
  bash deploy/restore_runtime_bundle.sh "${BUNDLE_PATH}" "${ROOT_DIR}"
fi

docker compose build
docker compose up -d
docker compose logs --tail=120 gemini-console
