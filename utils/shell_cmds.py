import json
import os
import subprocess
import time
from typing import List
from urllib.parse import urlparse

from loguru import logger


def _ssh_key_args() -> List[str]:
    key_path = os.getenv("SSH_KEY_PATH", "keys/ssh-key.pem").strip()
    if not key_path:
        return []
    return ["-i", key_path]

def scp(
    script_path: str,
    ip_address: str,
    user: str = "ubuntu",
    remote_path: str = "~",
    *,
    max_retries: int = 3,
    retry_delay: int = 15,
):
    scp_cmd = [
        'scp',
        '-o', 'StrictHostKeyChecking=no',
        "-o", "UserKnownHostsFile=/dev/null",
        *_ssh_key_args(),
        script_path,
        f'{user}@{ip_address}:{remote_path}'
    ]
    for attempt in range(max_retries):
        try:
            subprocess.run(scp_cmd, check=True, capture_output=True)
            return
        except subprocess.CalledProcessError as e:
            if attempt < max_retries - 1:
                logger.debug(f"{ip_address} SCP 失败 (尝试 {attempt + 1}/{max_retries}), {retry_delay} 秒后重试...  {e}")
                time.sleep(retry_delay)
            else:
                logger.debug(f"{ip_address} SCP 失败，已达到最大重试次数")
                raise

def rsync_download(remote_path: str, local_path: str, ip_address: str, *, user: str = "ubuntu", compress_level: int = 12, max_retries: int = 3):
    key_args = _ssh_key_args()
    key_opt = "" if not key_args else f" -i {key_args[1]}"
    rsync_cmd = [
        'rsync',
        '-az',  # -a: archive mode, -v: verbose, -z: compress
        '--compress-choice=zstd',  # 使用 zstd 压缩.
        # rsync: unrecognized option `--compress-choice=zstd'
        # '--whole-file',  # 跳过差异对比，直接传输整个文件
        f'--compress-level={compress_level}',
        '--partial',
        '--stats',
        '-e', f'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null{key_opt}',  # SSH 选项
        f'{user}@{ip_address}:{remote_path}',
        local_path,
    ]
    # Python 层面实现重试
    for attempt in range(max_retries):
        try:
            completed = subprocess.run(rsync_cmd, check=True, capture_output=True, text=True, timeout=20)
            # logger.debug(f"rsync completed: {completed.stdout}")
            return  # 成功则返回
        except subprocess.CalledProcessError as e:
            stderr = getattr(e, 'stderr', '')
            stdout = getattr(e, 'stdout', '')
            if attempt == max_retries - 1:  # 最后一次尝试
                logger.warning(
                    f"Cannot download files from {user}@{ip_address}:{remote_path} to {local_path}: returncode={e.returncode}, stdout={stdout}, stderr={stderr}"
                )
                raise Exception(f"Cannot download: returncode={e.returncode}, stderr={stderr}")
            logger.debug(
                f"rsync attempt {attempt + 1} failed (returncode={e.returncode}), retrying... stdout={stdout} stderr={stderr}"
            )
            # print(f"Attempt {attempt + 1} failed, retrying...")
        except subprocess.TimeoutExpired as e:
            if attempt == max_retries - 1:
                logger.warning(
                    f"Cannot download files from {user}@{ip_address}:{remote_path} to {local_path}: timeout after {e.timeout} seconds"
                )
                raise Exception("Cannot download: timeout")
            logger.debug(f"rsync attempt {attempt + 1} timed out, retrying...")
            # print(f"Timeout on attempt {attempt + 1}, retrying...")


def ssh(ip_address: str, user: str = "ubuntu", command: str | List[str] | None = None, *, max_retries: int = 3, retry_delay: int = 15):
    if command is None:
        return
    
    if type(command) is str:
        command = [command]

    ssh_cmd = [
        'ssh',
        '-o', 'StrictHostKeyChecking=no',
        "-o", "UserKnownHostsFile=/dev/null",
        *_ssh_key_args(),
        f'{user}@{ip_address}',
        *command
    ]

    for attempt in range(max_retries):
        try:
            result = subprocess.run(ssh_cmd, check=True, capture_output=True, text=True)
            return result
        except subprocess.CalledProcessError as e:
            if attempt < max_retries - 1:
                logger.debug(f"{ip_address} SSH 失败 (尝试 {attempt + 1}/{max_retries}), {retry_delay} 秒后重试...  {e}")
                time.sleep(retry_delay)
            else:
                logger.debug(f"{ip_address} SSH 失败，已达到最大重试次数")
                raise


def inject_dockerhub_mirrors(
    ip_address: str,
    *,
    user: str = "root",
    mirrors: List[str] | None = None,
    max_retries: int = 3,
    retry_delay: int = 15,
):
    if not mirrors:
        mirrors = [
            "https://docker.xuanyuan.me",
            "https://docker.1ms.run",
            "https://lispy.org",
        ]
    insecure_registries: List[str] = []
    for mirror in mirrors:
        parsed = urlparse(mirror)
        if parsed.scheme == "http":
            host = parsed.netloc or parsed.path
            if host:
                insecure_registries.append(host)
    daemon_payload = {"registry-mirrors": mirrors}
    if insecure_registries:
        daemon_payload["insecure-registries"] = insecure_registries
    daemon_config = json.dumps(daemon_payload, separators=(',', ':'))
    # Use a single-line command and send JSON via stdin (no JSON embedded in the command)
    write_cmd = f"mkdir -p /etc/docker && echo '{daemon_config}' > /etc/docker/daemon.json"
    # Write /etc/docker/daemon.json to the remote machine by sending JSON on stdin
    ssh(
        ip_address,
        user,
        write_cmd,
        max_retries=max_retries,
        retry_delay=retry_delay,
    )

    # Copy the restart-only script and execute it
    local_script = "auxiliary/scripts/remote/inject_dockerhub_mirrors.sh"
    remote_script = "/tmp/inject_dockerhub_mirrors.sh"
    scp(local_script, ip_address, user=user, remote_path=remote_script, max_retries=max_retries, retry_delay=retry_delay)

    return ssh(
        ip_address,
        user,
        f"chmod +x {remote_script} && {remote_script}",
        max_retries=max_retries,
        retry_delay=retry_delay,
    )