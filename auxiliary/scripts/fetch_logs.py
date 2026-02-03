#!/usr/bin/env python3
"""Download logs from all hosts recorded in hosts.json into a target directory.

Example:
  .venv/bin/python auxiliary/scripts/fetch_logs.py --target logs/20260203153921
"""
import argparse
from pathlib import Path
import json
from loguru import logger
import subprocess


def load_hosts(path: Path):
    data = json.load(open(path, 'r'))
    # Return raw dicts with expected keys: ip and nodes_per_host (plus optional ssh_user)
    return [item for item in data]


def scp_download_cli(remote_path: str, local_path: str, ip_address: str, *, user: str = 'root', key_path: str | None = None, max_retries: int = 3):
    key_args = []
    if key_path:
        key_args = ['-i', key_path]
    scp_cmd = [
        'scp',
        '-r',
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
        *key_args,
        f"{user}@{ip_address}:{remote_path}",
        local_path,
    ]
    for attempt in range(max_retries):
        try:
            subprocess.run(scp_cmd, check=True, capture_output=True, text=True, timeout=300)
            return
        except subprocess.CalledProcessError as e:
            if attempt == max_retries - 1:
                raise
        except subprocess.TimeoutExpired:
            if attempt == max_retries - 1:
                raise


def rsync_download_cli(remote_path: str, local_path: str, ip_address: str, *, user: str = 'root', key_path: str | None = None, compress_level: int = 12, max_retries: int = 3):
    key_opt = f" -i {key_path}" if key_path else ''
    ssh_cmd = f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null{key_opt}"
    rsync_cmd = [
        'rsync',
        '-az',
        f'--compress-level={compress_level}',
        '--compress-choice=zstd',
        '--partial',
        '--stats',
        '-e', ssh_cmd,
        f'{user}@{ip_address}:{remote_path}',
        local_path,
    ]

    for attempt in range(max_retries):
        try:
            subprocess.run(rsync_cmd, check=True, capture_output=True, text=True, timeout=120)
            return
        except subprocess.CalledProcessError as e:
            # try scp as fallback on last attempt
            if attempt == max_retries - 1:
                scp_download_cli(remote_path, local_path, ip_address, user=user, key_path=key_path, max_retries=2)
                return
        except subprocess.TimeoutExpired:
            if attempt == max_retries - 1:
                scp_download_cli(remote_path, local_path, ip_address, user=user, key_path=key_path, max_retries=2)
                return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--hosts', default='hosts.json')
    parser.add_argument('--target', required=True)
    parser.add_argument('--user', default='root')
    args = parser.parse_args()

    hosts = load_hosts(Path(args.hosts))
    target_base = Path(args.target)
    target_base.mkdir(parents=True, exist_ok=True)

    for host in hosts:
        ip = host.get('ip')
        nodes_per_host = int(host.get('nodes_per_host', 1))
        ssh_user = host.get('ssh_user') or args.user

        for idx in range(nodes_per_host):
            remote = f"/root/output{idx}/"
            local = target_base / f"{ip}-{idx}"
            local.mkdir(parents=True, exist_ok=True)
            logger.info(f"Downloading from {ip}:{remote} -> {local}")
            try:
                # prefer using host-specific key if available
                key_path = host.get('ssh_key_path') if isinstance(host, dict) else None
                rsync_download_cli(remote, str(local), ip, user=ssh_user, key_path=key_path)
            except Exception as e:
                logger.warning(f"Failed to download logs from {ip} (node {idx}): {e}")

    # Retry missing files (blocks.log absent or empty)
    for host in hosts:
        ip = host.get('ip')
        nodes_per_host = int(host.get('nodes_per_host', 1))
        ssh_user = host.get('ssh_user') or args.user
        key_path = host.get('ssh_key_path')

        for idx in range(nodes_per_host):
            local = target_base / f"{ip}-{idx}"
            blocks_file = local / 'blocks.log'
            if not blocks_file.exists() or blocks_file.stat().st_size == 0:
                remote = f"/root/output{idx}/"
                logger.info(f"Retrying download from {ip}:{remote} -> {local}")
                try:
                    rsync_download_cli(remote, str(local), ip, user=ssh_user, key_path=key_path, max_retries=3)
                except Exception as e:
                    logger.warning(f"Retry failed for {ip} (node {idx}): {e}")

    logger.success("Download finished")

if __name__ == '__main__':
    main()
