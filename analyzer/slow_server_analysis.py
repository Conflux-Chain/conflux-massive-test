#!/usr/bin/env python3
import argparse
import json
import re
import subprocess
import sys
import tempfile
from collections import defaultdict
from dataclasses import dataclass, asdict
from pathlib import Path
from statistics import mean
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from analyzer.stat_latency.stat_latency_map_reduce import HostLogReducer

RPC_LINE = re.compile(
    r"node\s+(?P<node>(?P<ip>\d+\.\d+\.\d+\.\d+)-\d+)\s+generate block\s+0x[0-9a-f]+,\s+rpc time\s+(?P<rpc>[0-9]+(?:\.[0-9]+)?)"
)


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    idx = int((len(sorted_values) - 1) * q)
    return sorted_values[idx]


@dataclass
class RpcSummary:
    host: str
    samples: int
    avg: float
    p95: float
    p99: float
    max: float
    score: float
    region: str | None
    zone: str | None
    provider: str | None


@dataclass
class PropagationSummary:
    host: str
    blocks: int
    txs: int
    by_block_ratio_avg: float
    sync_cons_gap_avg: float
    receive_p95: float
    sync_p95: float
    cons_p95: float
    score: float


def load_hosts_meta(hosts_file: Path) -> dict[str, dict[str, Any]]:
    if not hosts_file.exists():
        return {}
    with hosts_file.open("r", encoding="utf-8") as fp:
        hosts = json.load(fp)
    by_ip: dict[str, dict[str, Any]] = {}
    for host in hosts:
        ip = host.get("ip")
        if ip:
            by_ip[ip] = host
    return by_ip


def parse_rpc_times(remote_log: Path) -> dict[str, list[float]]:
    rpc_by_host: dict[str, list[float]] = defaultdict(list)
    if not remote_log.exists():
        return rpc_by_host

    with remote_log.open("r", encoding="utf-8") as fp:
        for line in fp:
            match = RPC_LINE.search(line)
            if not match:
                continue
            host = match.group("ip")
            rpc = float(match.group("rpc"))
            rpc_by_host[host].append(rpc)

    return rpc_by_host


def rank_rpc_hosts(rpc_by_host: dict[str, list[float]], host_meta: dict[str, dict[str, Any]]) -> list[RpcSummary]:
    rows: list[RpcSummary] = []
    for host, samples in rpc_by_host.items():
        if not samples:
            continue
        avg_v = mean(samples)
        p95 = percentile(samples, 0.95)
        p99 = percentile(samples, 0.99)
        mx = max(samples)
        score = 0.45 * p95 + 0.35 * p99 + 0.20 * mx

        meta = host_meta.get(host, {})
        rows.append(
            RpcSummary(
                host=host,
                samples=len(samples),
                avg=round(avg_v, 4),
                p95=round(p95, 4),
                p99=round(p99, 4),
                max=round(mx, 4),
                score=round(score, 4),
                region=meta.get("region"),
                zone=meta.get("zone"),
                provider=meta.get("provider"),
            )
        )

    rows.sort(key=lambda row: row.score, reverse=True)
    return rows


def _extract_blocks_log(node_dir: Path, work_dir: Path) -> Path | None:
    plain = node_dir / "blocks.log"
    if plain.exists():
        return plain

    archived = node_dir / "blocks.log.7z"
    if not archived.exists():
        return None

    out_dir = work_dir / node_dir.name
    out_dir.mkdir(parents=True, exist_ok=True)

    cmd = ["7z", "e", "-y", f"-o{str(out_dir)}", str(archived)]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return None

    extracted = out_dir / "blocks.log"
    return extracted if extracted.exists() else None


def summarize_node_propagation(blocks_log_file: Path, host: str) -> PropagationSummary | None:
    reducer = HostLogReducer.loadf(str(blocks_log_file))

    receive: list[float] = []
    sync: list[float] = []
    cons: list[float] = []

    for block in reducer.blocks.values():
        receive.extend(block.latencies.get("Receive", []))
        sync.extend(block.latencies.get("Sync", []))
        cons.extend(block.latencies.get("Cons", []))

    sync_gap_values: list[float] = []
    for stat in reducer.sync_cons_gap_stats:
        if hasattr(stat, "Avg"):
            sync_gap_values.append(float(stat.Avg))

    by_block_avg = mean(reducer.by_block_ratio) if reducer.by_block_ratio else 0.0
    sync_gap_avg = mean(sync_gap_values) if sync_gap_values else 0.0

    p95_receive = percentile(receive, 0.95)
    p95_sync = percentile(sync, 0.95)
    p95_cons = percentile(cons, 0.95)

    score = (
        0.45 * p95_receive
        + 0.35 * p95_sync
        + 0.20 * p95_cons
        + 0.05 * sync_gap_avg
        + 5.0 * by_block_avg
    )

    return PropagationSummary(
        host=host,
        blocks=len(reducer.blocks),
        txs=len(reducer.txs),
        by_block_ratio_avg=round(by_block_avg, 6),
        sync_cons_gap_avg=round(sync_gap_avg, 4),
        receive_p95=round(p95_receive, 4),
        sync_p95=round(p95_sync, 4),
        cons_p95=round(p95_cons, 4),
        score=round(score, 4),
    )


def analyze_run(run_dir: Path, top_n_for_propagation: int) -> dict[str, Any]:
    remote_log = run_dir / "remote_simulate.log"
    hosts_file = run_dir / "hosts.json"
    nodes_dir = run_dir / "nodes"

    host_meta = load_hosts_meta(hosts_file)
    rpc_by_host = parse_rpc_times(remote_log)
    rpc_rank = rank_rpc_hosts(rpc_by_host, host_meta)

    propagation_rank: list[PropagationSummary] = []
    propagation_errors: list[str] = []

    if nodes_dir.exists() and nodes_dir.is_dir():
        candidates = [row.host for row in rpc_rank[:top_n_for_propagation]]
        with tempfile.TemporaryDirectory(prefix=f"slow_server_{run_dir.name}_") as tmp:
            tmp_dir = Path(tmp)
            for host in candidates:
                node_dir = nodes_dir / f"{host}-0"
                if not node_dir.exists():
                    continue

                blocks_log = _extract_blocks_log(node_dir, tmp_dir)
                if blocks_log is None:
                    propagation_errors.append(f"{host}: missing or failed to extract blocks.log(.7z)")
                    continue

                try:
                    summary = summarize_node_propagation(blocks_log, host)
                    if summary:
                        propagation_rank.append(summary)
                except Exception as exc:
                    propagation_errors.append(f"{host}: parse error ({exc})")

        propagation_rank.sort(key=lambda row: row.score, reverse=True)

    return {
        "run": run_dir.name,
        "path": str(run_dir),
        "rpc_rank": [asdict(row) for row in rpc_rank],
        "propagation_rank": [asdict(row) for row in propagation_rank],
        "propagation_errors": propagation_errors,
    }


def print_run_report(result: dict[str, Any], top_n: int) -> None:
    print(f"\n=== Run {result['run']} ===")

    rpc_rows = result["rpc_rank"][:top_n]
    if not rpc_rows:
        print("No RPC generation lines found in remote_simulate.log")
    else:
        print("Top slow RPC hosts:")
        for row in rpc_rows:
            print(
                f"  {row['host']:>15}  provider={row.get('provider')}  zone={row.get('zone')}  score={row['score']:.3f}  "
                f"p95={row['p95']:.3f}s p99={row['p99']:.3f}s max={row['max']:.3f}s "
                f"samples={row['samples']}"
            )

    prop_rows = result["propagation_rank"][:top_n]
    if prop_rows:
        print("Top propagation-risk hosts (from node blocks logs):")
        for row in prop_rows:
            print(
                f"  {row['host']:>15}  score={row['score']:.3f}  "
                f"recv_p95={row['receive_p95']:.3f}s sync_p95={row['sync_p95']:.3f}s cons_p95={row['cons_p95']:.3f}s "
                f"gap_avg={row['sync_cons_gap_avg']:.2f} by_block={row['by_block_ratio_avg']:.4f}"
            )
    else:
        print("No propagation-risk rows generated (missing nodes/*/blocks.log(.7z) or parse failures).")


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze slow remote servers for Conflux performance runs")
    parser.add_argument(
        "--logs",
        nargs="+",
        default=[
            "logs/20260226160703",
            "logs/20260226163242",
        ],
        help="Run log directories to analyze",
    )
    parser.add_argument("--top", type=int, default=15, help="Top N hosts to print")
    parser.add_argument(
        "--top-propagation-candidates",
        type=int,
        default=30,
        help="Analyze propagation for top-N RPC slow hosts per run",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs/slow_server_analysis",
        help="Directory to write analysis artifacts",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    all_results: list[dict[str, Any]] = []

    for run in args.logs:
        run_dir = Path(run)
        if not run_dir.exists():
            print(f"Skip missing run directory: {run_dir}")
            continue
        result = analyze_run(run_dir, top_n_for_propagation=args.top_propagation_candidates)
        all_results.append(result)
        print_run_report(result, args.top)

        run_json = output_dir / f"{run_dir.name}.json"
        with run_json.open("w", encoding="utf-8") as fp:
            json.dump(result, fp, ensure_ascii=False, indent=2)

    summary_json = output_dir / "summary.json"
    with summary_json.open("w", encoding="utf-8") as fp:
        json.dump({"logs": all_results}, fp, ensure_ascii=False, indent=2)

    print(f"\nSaved reports to: {output_dir}")


if __name__ == "__main__":
    main()
