#!/usr/bin/env python3
"""Provision Aliyun servers and write instance inventory JSON."""
from __future__ import annotations

import argparse
import datetime
import json
from pathlib import Path
from typing import Iterable, Dict, Any

from loguru import logger

from ali_instances.multi_region_runner import provision_aliyun_hosts
from remote_simulation.launch_conflux_node import HostSpec


def generate_timestamp() -> str:
    return datetime.datetime.now().strftime("%Y%m%d%H%M%S")


def load_host_specs(data: object) -> list[HostSpec]:
    """Parse host specs from previously written inventory JSON or a raw list.

    Accepts either the full inventory dict that contains a "hosts" key or a
    bare list of host dicts. Returns a list of HostSpec instances.
    """
    if isinstance(data, dict) and "hosts" in data:
        hosts_data = data.get("hosts", [])
    else:
        hosts_data = data
    if not isinstance(hosts_data, list):
        raise ValueError("invalid ali_servers.json format")
    hosts: list[HostSpec] = []
    for item in hosts_data:
        if not isinstance(item, dict):
            continue
        hosts.append(
            HostSpec(
                ip=item["ip"],
                nodes_per_host=int(item["nodes_per_host"]),
                ssh_user=item.get("ssh_user", "root"),
                ssh_key_path=item.get("ssh_key_path"),
                provider=item.get("provider"),
                region=item.get("region"),
                instance_id=item.get("instance_id"),
            )
        )
    return hosts


def serialize_host(host: HostSpec) -> Dict[str, Any]:
    return {
        "ip": host.ip,
        "nodes_per_host": host.nodes_per_host,
        "ssh_user": host.ssh_user,
        "ssh_key_path": host.ssh_key_path,
        "provider": host.provider,
        "region": host.region,
        "instance_id": host.instance_id,
    }


def write_inventory(
    hosts: Iterable[HostSpec],
    timestamp: str,
    log_dir: Path,
    root: Path,
) -> None:
    data = {
        "timestamp": timestamp,
        "log_dir": str(log_dir.as_posix()),
        "hosts": [serialize_host(h) for h in hosts],
    }
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / "ali_servers.json").write_text(json.dumps(data, ensure_ascii=False, indent=2))
    (root / "ali_servers.json").write_text(json.dumps(data, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Provision Aliyun servers and write inventory JSON.")
    parser.add_argument(
        "--config",
        default="instance-region.json",
        help="Path to instance-region.json",
    )
    parser.add_argument(
        "--hardware",
        default="config/hardware.json",
        help="Path to hardware.json",
    )
    parser.add_argument(
        "--common-tag",
        default="conflux-massive-test",
        help="Common tag value for Aliyun resources",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    config_path = (root / args.config).resolve()
    hardware_path = (root / args.hardware).resolve()

    hosts, _ = provision_aliyun_hosts(
        config_path=config_path,
        hardware_path=hardware_path,
        common_tag=args.common_tag,
    )
    if not hosts:
        raise RuntimeError("no Aliyun hosts were provisioned")

    timestamp = generate_timestamp()
    log_dir = root / "logs" / timestamp
    write_inventory(hosts, timestamp, log_dir, root)
    logger.success(f"Aliyun instance inventory written to {log_dir}/ali_servers.json and {root}/ali_servers.json")


if __name__ == "__main__":
    main()
