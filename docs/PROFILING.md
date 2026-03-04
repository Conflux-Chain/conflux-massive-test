# Profiling with Flamegraph ✅

- The node Docker image includes `flamegraph` (installed in `node_docker_image/Dockerfile`).
- To enable flamegraph profiling for all nodes in a run, pass `--enable-flamegraph` to `remote_simulate.py` (or set `SimulateOptions.enable_flamegraph = True`).
- After the run finishes, collected logs will contain flamegraph SVG(s) at `logs/{timestamp}/{node_id}/output/flame_{index}.svg`.

Example:

```bash
python remote_simulate.py --enable-flamegraph
```

Notes:
- The flamegraph wrapper uses `perf` under the hood; the Docker containers run with `--privileged` so `perf` should work on most Linux hosts.
- If flamegraph SVGs are missing, check `/root/log` on the remote host for `flame_*.svg` or `perf.data` and consult container logs for errors.