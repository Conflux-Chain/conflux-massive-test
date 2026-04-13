#!/usr/bin/env bash
set -euo pipefail

PORT_RANGE="${1:?port range required}"
CAPTURE_DIR_NAME="${2:?capture dir name required}"
OUTPUT_DIR_NAME="${3:?output dir name required}"
PROVIDER="${4:-unknown}"
REGION="${5:-unknown}"
ZONE="${6:-unknown}"
NODES_PER_HOST="${7:?nodes per host required}"

if [ "$(id -un)" = "root" ]; then
  SUDO=()
else
  SUDO=(sudo)
fi

CAPTURE_ROOT="$HOME/$CAPTURE_DIR_NAME"
OUTPUT_DIR="$HOME/$OUTPUT_DIR_NAME"
PID_FILE="$CAPTURE_ROOT/tcpdump.pid"
STATUS_FILE="$CAPTURE_ROOT/status.txt"
METADATA_FILE="$CAPTURE_ROOT/metadata.txt"
STDOUT_FILE="$CAPTURE_ROOT/tcpdump.stdout.log"
STDERR_FILE="$CAPTURE_ROOT/tcpdump.stderr.log"
PCAP_PATH="$CAPTURE_ROOT/$(hostname).pcap"

if [ -f "$PID_FILE" ]; then
  "${SUDO[@]}" kill -INT "$(cat "$PID_FILE")" >/dev/null 2>&1 || true
fi

"${SUDO[@]}" rm -rf "$CAPTURE_ROOT" "$OUTPUT_DIR"
mkdir -p "$CAPTURE_ROOT"

TCPDUMP_BIN="$(command -v tcpdump || true)"
if [ -z "$TCPDUMP_BIN" ] && [ -x /usr/sbin/tcpdump ]; then
  TCPDUMP_BIN=/usr/sbin/tcpdump
fi

if [ -z "$TCPDUMP_BIN" ]; then
  printf 'missing_tcpdump\n' > "$STATUS_FILE"
  cat > "$METADATA_FILE" <<EOF
provider=$PROVIDER
region=$REGION
zone=$ZONE
nodes_per_host=$NODES_PER_HOST
port_range=$PORT_RANGE
interface=any
status=missing_tcpdump
EOF
  cat "$STATUS_FILE"
  exit 0
fi

"${SUDO[@]}" env \
  PORT_RANGE="$PORT_RANGE" \
  PCAP_PATH="$PCAP_PATH" \
  STDOUT_FILE="$STDOUT_FILE" \
  STDERR_FILE="$STDERR_FILE" \
  PID_FILE="$PID_FILE" \
  TCPDUMP_BIN="$TCPDUMP_BIN" \
  bash -c '
    nohup "$TCPDUMP_BIN" -i any tcp and portrange "$PORT_RANGE" -s 150 -W 10 -C 100 -Z root -w "$PCAP_PATH" > "$STDOUT_FILE" 2> "$STDERR_FILE" < /dev/null &
    echo $! > "$PID_FILE"
  '

sleep 1

if ! "${SUDO[@]}" kill -0 "$(cat "$PID_FILE")" >/dev/null 2>&1; then
  printf 'start_failed\n' > "$STATUS_FILE"
  cat > "$METADATA_FILE" <<EOF
provider=$PROVIDER
region=$REGION
zone=$ZONE
nodes_per_host=$NODES_PER_HOST
port_range=$PORT_RANGE
interface=any
status=start_failed
EOF
  cat "$STATUS_FILE"
  exit 0
fi

printf 'started\n' > "$STATUS_FILE"
cat > "$METADATA_FILE" <<EOF
provider=$PROVIDER
region=$REGION
zone=$ZONE
nodes_per_host=$NODES_PER_HOST
port_range=$PORT_RANGE
interface=any
tcpdump_bin=$TCPDUMP_BIN
pcap_path=$PCAP_PATH
status=started
EOF

cat "$STATUS_FILE"