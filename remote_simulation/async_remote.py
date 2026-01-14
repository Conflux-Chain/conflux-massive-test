"""AsyncSSH-based remote operations for remote_simulation.

This module exists to avoid spawning大量 ssh/scp 子进程。
It uses conflux_deployer.utils.remote.RemoteExecutor (asyncssh) to:
- upload config
- start/stop/destroy docker-based nodes
- collect logs via remote tar + SFTP download

Important: this path assumes the *server image* already contains the required
Docker image tag (e.g. conflux-node:latest) and helper scripts.
Therefore we DO NOT run `docker pull` here.
"""

from __future__ import annotations

import os
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from loguru import logger

from conflux_deployer.utils.remote import RemoteExecutor
from remote_simulation import docker_cmds
from remote_simulation.remote_node import RemoteNode


@dataclass(frozen=True)
class LaunchResult:
    nodes: List[RemoteNode]
    failed_hosts: List[str]


def _group_hosts_by_nodes_per_host(
    hosts: List[str],
    nodes_per_host: int,
) -> Dict[int, List[str]]:
    # For now v2 always uses uniform nodes_per_host.
    return {int(nodes_per_host): list(hosts)}


def launch_remote_nodes_asyncssh(
    ips: List[str],
    nodes_per_host: int,
    config_file_path: str,
    *,
    ssh_key_path: Optional[str] = None,
    ssh_user: str = "ubuntu",
    max_workers: int = 200,
    wait_ready_timeout_sec: int = 180,
) -> LaunchResult:
    """Launch nodes on remote hosts using asyncssh.

    - Uploads config to `~/config.toml`
    - Destroys old containers/logs
    - Starts `nodes_per_host` docker containers per host
    - Waits for node RPC readiness (NormalSyncPhase)

    Returns nodes that started successfully.
    """

    if not ips:
        return LaunchResult(nodes=[], failed_hosts=[])

    executor = RemoteExecutor(ssh_key_path=ssh_key_path, ssh_user=ssh_user, known_hosts=None)

    # 1) Upload config
    put = executor.copy_file_to_all(ips, config_file_path, "~/config.toml", max_workers=max_workers, retry=3)
    ok_hosts = [h for h, ok in put.items() if ok]
    failed_hosts = [h for h, ok in put.items() if not ok]

    if failed_hosts:
        logger.warning(f"Config upload failed on {len(failed_hosts)}/{len(ips)} hosts")

    if not ok_hosts:
        return LaunchResult(nodes=[], failed_hosts=failed_hosts)

    # 2) Cleanup previous run
    cleanup_res = executor.execute_on_all(ok_hosts, docker_cmds.destory_all_nodes(), max_workers=max_workers, retry=1, timeout=600)
    ok_hosts2 = [h for h, r in cleanup_res.items() if r.success]
    failed_hosts.extend([h for h, r in cleanup_res.items() if not r.success])

    if not ok_hosts2:
        return LaunchResult(nodes=[], failed_hosts=list(sorted(set(failed_hosts))))

    # 3) Start containers per host
    host_to_cmds: Dict[str, Dict[str, str]] = {}
    for host in ok_hosts2:
        cmds: Dict[str, str] = {}
        for idx in range(int(nodes_per_host)):
            cmds[f"node-{idx}"] = docker_cmds.launch_node(idx)
        host_to_cmds[host] = cmds

    start_res = executor.execute_commands_on_hosts(host_to_cmds, max_workers=max_workers, retry=1, timeout=900)

    nodes: List[RemoteNode] = []
    for host, results in start_res.items():
        for key, cmd_res in results.items():
            if cmd_res.success:
                try:
                    idx = int(key.split("-", 1)[1])
                except Exception:
                    continue
                nodes.append(RemoteNode(host=host, index=idx))

    # 4) Wait ready (RPC calls; no ssh)
    ready_nodes: List[RemoteNode] = []
    for n in nodes:
        if n.wait_for_ready():
            ready_nodes.append(n)

    # Mark hosts which have 0 ready nodes as failed
    ready_hosts = {n.host for n in ready_nodes}
    for host in ok_hosts2:
        if host not in ready_hosts:
            failed_hosts.append(host)

    return LaunchResult(nodes=ready_nodes, failed_hosts=list(sorted(set(failed_hosts))))


def stop_remote_nodes_asyncssh(
    ips: List[str],
    *,
    ssh_key_path: Optional[str] = None,
    ssh_user: str = "ubuntu",
    max_workers: int = 200,
) -> Dict[str, bool]:
    executor = RemoteExecutor(ssh_key_path=ssh_key_path, ssh_user=ssh_user, known_hosts=None)
    res = executor.execute_on_all(ips, docker_cmds.stop_all_nodes(), max_workers=max_workers, retry=1, timeout=600)
    return {h: r.success for h, r in res.items()}


def destroy_remote_nodes_asyncssh(
    ips: List[str],
    *,
    ssh_key_path: Optional[str] = None,
    ssh_user: str = "ubuntu",
    max_workers: int = 200,
) -> Dict[str, bool]:
    executor = RemoteExecutor(ssh_key_path=ssh_key_path, ssh_user=ssh_user, known_hosts=None)
    res = executor.execute_on_all(ips, docker_cmds.destory_all_nodes(), max_workers=max_workers, retry=1, timeout=900)
    return {h: r.success for h, r in res.items()}


def collect_logs_asyncssh(
    nodes: List[RemoteNode],
    local_path: str,
    *,
    ssh_key_path: Optional[str] = None,
    ssh_user: str = "ubuntu",
    max_workers: int = 80,
) -> None:
    """Collect logs without rsync.

    Strategy per node:
    - run docker_cmds.stop_node_and_collect_log(index) to generate ~/output{index}
    - tar the directory to /tmp/<nodeid>.tgz
    - SFTP download the tarball
    - extract to local_path/<node.id>/

    Notes:
    - This keeps remote execution asyncssh-only.
    - Local extraction uses Python tarfile.
    """

    if not nodes:
        return

    Path(local_path).mkdir(parents=True, exist_ok=True)

    executor = RemoteExecutor(ssh_key_path=ssh_key_path, ssh_user=ssh_user, known_hosts=None)

    # Run log generation per node (bounded concurrency via max_workers)
    host_to_cmds: Dict[str, Dict[str, str]] = {}
    for n in nodes:
        host_to_cmds.setdefault(n.host, {})[n.id] = docker_cmds.stop_node_and_collect_log(n.index)

    gen_res = executor.execute_commands_on_hosts(host_to_cmds, max_workers=max_workers, retry=1, timeout=1800)

    # Tar and download
    for n in nodes:
        host_results = gen_res.get(n.host, {})
        cmd_res = host_results.get(n.id)
        if not cmd_res or not cmd_res.success:
            logger.warning(f"Log generation failed on {n.id}: {cmd_res.stderr if cmd_res else 'no result'}")
            continue

        remote_tar = f"/tmp/{n.id}.tgz"
        remote_output_dir = f"~/output{n.index}"
        tar_cmd = f"tar -czf {remote_tar} -C {remote_output_dir} ."
        tar_ok = executor.execute_on_host(n.host, tar_cmd, retry=1, timeout=600)
        if not tar_ok.success:
            logger.warning(f"Tar failed on {n.id}: {tar_ok.stderr}")
            continue

        # Download tarball
        with tempfile.TemporaryDirectory() as td:
            local_tar = os.path.join(td, f"{n.id}.tgz")
            if not executor.download_file(n.host, remote_tar, local_tar, retry=2):
                logger.warning(f"Download failed on {n.id}")
                continue

            out_dir = Path(local_path) / n.id
            out_dir.mkdir(parents=True, exist_ok=True)
            with tarfile.open(local_tar, "r:gz") as tf:
                tf.extractall(path=out_dir)

        # Best-effort cleanup
        executor.execute_on_host(n.host, f"rm -f {remote_tar}", retry=0, timeout=60)

    logger.success(f"Logs collected to {os.path.abspath(local_path)}")
