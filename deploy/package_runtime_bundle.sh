#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_NAME="${1:-runtime_bundle_$(date +%Y%m%d_%H%M%S).tar.gz}"

if [[ "${OUT_NAME}" != /* ]]; then
  OUT_PATH="${ROOT_DIR}/${OUT_NAME}"
else
  OUT_PATH="${OUT_NAME}"
fi

INCLUDE_ITEMS=(
  ".env"
  "console_config.json"
  "console_state.json"
  "maintenance_status.json"
  "mail_tokens.txt"
  "mailbox_tokens.json"
  "existing_accounts.json"
  "gemini_accounts"
)

EXISTING_ITEMS=()
for item in "${INCLUDE_ITEMS[@]}"; do
  if [[ -e "${ROOT_DIR}/${item}" ]]; then
    EXISTING_ITEMS+=("${item}")
  fi
done

if [[ ${#EXISTING_ITEMS[@]} -eq 0 ]]; then
  echo "No runtime data found to package."
  exit 1
fi

mkdir -p "$(dirname "${OUT_PATH}")"
tar -czf "${OUT_PATH}" -C "${ROOT_DIR}" "${EXISTING_ITEMS[@]}"

echo "Runtime bundle created:"
echo "  ${OUT_PATH}"
echo "Included:"
for item in "${EXISTING_ITEMS[@]}"; do
  echo "  - ${item}"
done
