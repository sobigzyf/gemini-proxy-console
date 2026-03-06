#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: bash deploy/restore_runtime_bundle.sh <bundle.tar.gz> [target_dir]"
  exit 1
fi

BUNDLE_PATH="$1"
TARGET_DIR="${2:-$(pwd)}"

if [[ ! -f "${BUNDLE_PATH}" ]]; then
  echo "Bundle not found: ${BUNDLE_PATH}"
  exit 1
fi

mkdir -p "${TARGET_DIR}"
tar -xzf "${BUNDLE_PATH}" -C "${TARGET_DIR}"

echo "Runtime bundle restored to: ${TARGET_DIR}"
