#!/usr/bin/env bash
set -euo pipefail

CAPTURE_DIR_NAME="${1:?capture dir name required}"
OUTPUT_DIR_NAME="${2:?output dir name required}"

if [ "$(id -un)" = "root" ]; then
  SUDO=()
else
  SUDO=(sudo)
fi

CAPTURE_ROOT="$HOME/$CAPTURE_DIR_NAME"
OUTPUT_DIR="$HOME/$OUTPUT_DIR_NAME"
PID_FILE="$CAPTURE_ROOT/tcpdump.pid"
STATUS_FILE="$CAPTURE_ROOT/status.txt"
STATUS="capture_not_started"

"${SUDO[@]}" rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

if [ -f "$STATUS_FILE" ]; then
  STATUS="$(tr -d '\r\n' < "$STATUS_FILE")"
fi

if [ -f "$PID_FILE" ]; then
  PID="$(cat "$PID_FILE")"
  "${SUDO[@]}" kill -INT "$PID" >/dev/null 2>&1 || true
  for _ in $(seq 1 50); do
    if ! "${SUDO[@]}" kill -0 "$PID" >/dev/null 2>&1; then
      break
    fi
    sleep 0.1
  done
fi

if [ -d "$CAPTURE_ROOT" ]; then
  COPIED=0
  shopt -s nullglob
  for path in "$CAPTURE_ROOT"/*; do
    if [ ! -f "$path" ] || [ "$(basename "$path")" = "tcpdump.pid" ]; then
      continue
    fi
    "${SUDO[@]}" cp "$path" "$OUTPUT_DIR/$(basename "$path")"
    COPIED=1
  done

  if [ "$COPIED" -eq 0 ]; then
    printf '%s\n' "$STATUS" > "$OUTPUT_DIR/status.txt"
  fi

  "${SUDO[@]}" rm -rf "$CAPTURE_ROOT"
else
  printf '%s\n' "$STATUS" > "$OUTPUT_DIR/status.txt"
fi

"${SUDO[@]}" chown -R "$(id -un):$(id -gn)" "$OUTPUT_DIR"

if command -v 7z >/dev/null 2>&1; then
  find "$OUTPUT_DIR" -maxdepth 1 -type f ! -name '*.7z' -print0 |
    xargs -0 -P4 -I{} sh -c '7z a -t7z -mx=9 -m0=lzma2 -ms=on -bso0 -bsp0 "$1.7z" "$1" >/dev/null && rm -f "$1"' _ "{}" || true
fi

printf '%s\n' "$STATUS"