#!/usr/bin/env bash
set -euo pipefail

cd /app
mkdir -p gemini_accounts
mkdir -p /root/.cache
mkdir -p /root/.local/share/undetected_chromedriver

if [[ "${USE_XVFB:-1}" == "1" ]]; then
  export DISPLAY="${DISPLAY:-:99}"
  Xvfb "${DISPLAY}" -screen 0 "${XVFB_WHD:-1920x1080x24}" -nolisten tcp -ac &
  XVFB_PID=$!
  cleanup() {
    if kill -0 "${XVFB_PID}" 2>/dev/null; then
      kill "${XVFB_PID}" || true
    fi
  }
  trap cleanup EXIT INT TERM
fi

exec python -u run_console.py
