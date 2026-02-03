#!/bin/bash

python3 stat_latency_map_reduce.py ./log ./output/blocks.log

cat ./log/conflux.log | grep "new block inserted into graph" > ./output/conflux.log.new_blocks
cp ./log/metrics.log ./output/metrics.log

# Copy any generated flamegraph SVGs and perf data into the output directory
cp ./log/*.svg ./output/ 2>/dev/null || true
cp ./log/perf.data ./output/ 2>/dev/null || true
# Also copy any flamegraph error logs and start markers to aid debugging
cp ./log/*.err ./output/ 2>/dev/null || true
cp ./log/flame_start_*.txt ./output/ 2>/dev/null || true
# Copy profiler exit codes and perf stderr logs to aid diagnosis
cp ./log/flame_exit_*.txt ./output/ 2>/dev/null || true
cp ./log/*.perf.err ./output/ 2>/dev/null || true