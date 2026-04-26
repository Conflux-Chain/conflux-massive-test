from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import os
import socket
import threading

from loguru import logger

from utils.wait_until import WaitUntilTimeoutError, wait_until


SSHCheckTask = tuple[str, str, str]
SSHCheckResult = tuple[str, str, str, bool]


def _read_env_int(name: str, default: int, *, min_value: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return max(min_value, int(raw))
    except ValueError:
        return default


_DEFAULT_SSH_CHECK_PROCESS_WORKERS = max(1, os.cpu_count() or 1)
SSH_CHECK_PROCESS_WORKERS = _read_env_int(
    "SSH_CHECK_PROCESS_WORKERS",
    _DEFAULT_SSH_CHECK_PROCESS_WORKERS,
    min_value=1,
)
SSH_CHECK_THREADS_PER_PROCESS = 128
_DEFAULT_SSH_CHECK_MAX_IN_FLIGHT = SSH_CHECK_PROCESS_WORKERS * SSH_CHECK_THREADS_PER_PROCESS
SSH_CHECK_MAX_IN_FLIGHT = _read_env_int(
    "SSH_CHECK_MAX_IN_FLIGHT",
    _DEFAULT_SSH_CHECK_MAX_IN_FLIGHT,
    min_value=1,
)

_SSH_CHECK_POOL = None
_SSH_CHECK_POOL_LOCK = threading.Lock()
_SSH_CHECK_PROCESS_THREAD_POOL = None


def _initialize_ssh_check_worker_process():
    global _SSH_CHECK_PROCESS_THREAD_POOL
    if _SSH_CHECK_PROCESS_THREAD_POOL is None:
        _SSH_CHECK_PROCESS_THREAD_POOL = ThreadPoolExecutor(
            max_workers=SSH_CHECK_THREADS_PER_PROCESS
        )


def _check_port(ip: str, timeout: int = 5):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)

    try:
        result = sock.connect_ex((ip, 22))
        return result == 0
    except (socket.timeout, socket.error):
        return False
    finally:
        sock.close()


def wait_for_ssh_port_ready(ip: str):
    try:
        wait_until(lambda: _check_port(ip), timeout=180)
        return True
    except WaitUntilTimeoutError:
        logger.warning(f"Cannot connect to IP {ip}")
        return False


def _run_ssh_checks_in_batch(batch: list[SSHCheckTask]) -> list[SSHCheckResult]:
    if not batch:
        return []

    if _SSH_CHECK_PROCESS_THREAD_POOL is None:
        _initialize_ssh_check_worker_process()
    pool = _SSH_CHECK_PROCESS_THREAD_POOL
    if pool is None:
        raise RuntimeError("SSH check worker process thread pool was not initialized")

    futures = [
        (
            instance_id,
            public_ip,
            private_ip,
            pool.submit(wait_for_ssh_port_ready, public_ip),
        )
        for instance_id, public_ip, private_ip in batch
    ]
    results: list[SSHCheckResult] = []
    for instance_id, public_ip, private_ip, future in futures:
        try:
            is_success = future.result()
        except Exception:
            logger.exception(
                f"SSH check worker thread failed for instance {instance_id}, ip {public_ip}"
            )
            is_success = False
        results.append((instance_id, public_ip, private_ip, is_success))
    return results


def get_ssh_check_pool():
    global _SSH_CHECK_POOL
    if _SSH_CHECK_POOL is None:
        with _SSH_CHECK_POOL_LOCK:
            if _SSH_CHECK_POOL is None:
                _SSH_CHECK_POOL = ProcessPoolExecutor(
                    max_workers=SSH_CHECK_PROCESS_WORKERS,
                    initializer=_initialize_ssh_check_worker_process,
                )
    return _SSH_CHECK_POOL


def submit_ssh_check_batch(batch: list[SSHCheckTask]):
    return get_ssh_check_pool().submit(_run_ssh_checks_in_batch, batch)