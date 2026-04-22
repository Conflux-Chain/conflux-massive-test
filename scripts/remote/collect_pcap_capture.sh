#!/usr/bin/env bash
set -euo pipefail

CAPTURE_ROOT="${1:?capture root required}"
OUTPUT_DIR="${2:?output dir required}"
ARCHIVE_PATH="$OUTPUT_DIR/pcap_capture_$(hostname).7z"
TEMP_ARCHIVE_PATH="$OUTPUT_DIR/.pcap_capture_$(hostname).7z.tmp"
CACHED_STATUS_FILE="$OUTPUT_DIR/.status.txt"
CACHED_METADATA_FILE="$OUTPUT_DIR/.metadata.txt"

if [ "$(id -un)" = "root" ]; then
  SUDO=()
else
  SUDO=(sudo)
fi

STAGING_DIR="$OUTPUT_DIR/staging.$$"
PID_FILE="$CAPTURE_ROOT/tcpdump.pid"
STATUS_FILE="$CAPTURE_ROOT/status.txt"
STATUS="capture_not_started"

if [ -f "$CACHED_STATUS_FILE" ]; then
  STATUS="$(tr -d '\r\n' < "$CACHED_STATUS_FILE")"
fi

if [ -f "$STATUS_FILE" ]; then
  STATUS="$(tr -d '\r\n' < "$STATUS_FILE")"
fi

if [ ! -d "$CAPTURE_ROOT" ] && [ -f "$ARCHIVE_PATH" ]; then
  printf '%s\n' "$STATUS"
  exit 0
fi

mkdir -p "$STAGING_DIR"

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
  shopt -s nullglob
  for path in "$CAPTURE_ROOT"/*; do
    if [ ! -f "$path" ] || [ "$(basename "$path")" = "tcpdump.pid" ]; then
      continue
    fi
    "${SUDO[@]}" cp "$path" "$STAGING_DIR/$(basename "$path")"
  done

  "${SUDO[@]}" rm -rf "$CAPTURE_ROOT"
fi

mkdir -p "$OUTPUT_DIR"
"${SUDO[@]}" chown -R "$(id -un):$(id -gn)" "$OUTPUT_DIR"
printf '%s\n' "$STATUS" > "$STAGING_DIR/status.txt"
printf '%s\n' "$STATUS" > "$CACHED_STATUS_FILE"
if [ -f "$STAGING_DIR/metadata.txt" ]; then
  cp "$STAGING_DIR/metadata.txt" "$CACHED_METADATA_FILE"
elif [ -f "$CACHED_METADATA_FILE" ]; then
  cp "$CACHED_METADATA_FILE" "$STAGING_DIR/metadata.txt"
fi

if command -v 7z >/dev/null 2>&1; then
  (
    cd "$STAGING_DIR"
    7z a -t7z -mx=9 -m0=lzma2 -ms=on -bso0 -bsp0 "$TEMP_ARCHIVE_PATH" ./*
  )
  mv "$TEMP_ARCHIVE_PATH" "$ARCHIVE_PATH"
  rm -rf "$STAGING_DIR"
fi

printf '%s\n' "$STATUS"