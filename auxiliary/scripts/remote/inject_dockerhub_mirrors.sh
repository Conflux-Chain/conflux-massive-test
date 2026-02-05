#!/bin/sh

# Only responsible for restarting Docker on the remote host.
# Exits non-zero if no known service manager is found.

set -e

if command -v systemctl >/dev/null 2>&1; then
  systemctl restart docker
elif command -v service >/dev/null 2>&1; then
  service docker restart
else
  echo "No service manager found to restart docker" >&2
  exit 1
fi
