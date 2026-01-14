"""
Remote Command Execution Utilities

Provides SSH/SFTP utilities for executing commands and transferring files
on remote servers.

This module uses `asyncssh` to support high concurrency without shelling
out to `ssh`/`scp`.
"""

import asyncio
import os
import shlex
import tempfile
from typing import List, Optional, Dict, Any, TypeVar, Coroutine
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor

import asyncssh
from loguru import logger

T = TypeVar("T")


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        return value
    return str(value)


@dataclass
class CommandResult:
    """Result of a remote command execution"""
    host: str
    success: bool
    stdout: str
    stderr: str
    return_code: int


class RemoteExecutor:
    """
    Executes commands on remote servers via SSH (asyncssh).
    
    Supports:
    - Single host execution
    - Parallel execution across multiple hosts
    - File transfer (SFTP)
    """
    
    def __init__(
        self,
        ssh_key_path: Optional[str] = None,
        ssh_user: str = "ubuntu",
        known_hosts: Optional[str] = None,
        connect_timeout: float = 30.0,
        keepalive_interval: float = 30.0,
    ):
        """
        Initialize the remote executor.
        
        Args:
            ssh_key_path: Path to SSH private key
            ssh_user: SSH username
            known_hosts: Path to known_hosts file (or None to disable host key checks)
            connect_timeout: SSH connect timeout seconds
            keepalive_interval: SSH keepalive interval seconds
        """
        self.ssh_key_path = ssh_key_path
        self.ssh_user = ssh_user
        self.known_hosts = known_hosts
        self.connect_timeout = connect_timeout
        self.keepalive_interval = keepalive_interval
    
    def _run_coro(self, coro: Coroutine[Any, Any, T]) -> T:
        """Run coroutine from sync code.

        If called from within an existing event loop, it runs the coroutine in
        a background thread to avoid "Cannot run the event loop while another
        loop is running".
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)

        with ThreadPoolExecutor(max_workers=1) as executor:
            fut = executor.submit(asyncio.run, coro)
            return fut.result()

    async def _connect(self, host: str) -> asyncssh.SSHClientConnection:
        client_keys: Optional[List[str]] = None
        if self.ssh_key_path:
            client_keys = [self.ssh_key_path]

        return await asyncssh.connect(
            host,
            username=self.ssh_user,
            client_keys=client_keys,
            known_hosts=self.known_hosts,
            connect_timeout=self.connect_timeout,
            keepalive_interval=self.keepalive_interval,
        )

    async def _run_one(
        self,
        host: str,
        command: str,
        retry: int,
        timeout: int,
    ) -> CommandResult:
        last_exc: Optional[BaseException] = None
        for attempt in range(retry + 1):
            try:
                async with await self._connect(host) as conn:
                    res = await asyncio.wait_for(conn.run(command, check=False), timeout=timeout)
                    exit_status = res.exit_status if res.exit_status is not None else -1
                    return CommandResult(
                        host=host,
                        success=exit_status == 0,
                        stdout=_as_text(res.stdout),
                        stderr=_as_text(res.stderr),
                        return_code=int(exit_status),
                    )
            except (asyncio.TimeoutError, asyncssh.Error, OSError) as e:
                last_exc = e
                if attempt < retry:
                    await asyncio.sleep(1)
                else:
                    return CommandResult(
                        host=host,
                        success=False,
                        stdout="",
                        stderr=str(e),
                        return_code=-1,
                    )
            except Exception as e:
                last_exc = e
                if attempt < retry:
                    await asyncio.sleep(1)
                else:
                    return CommandResult(
                        host=host,
                        success=False,
                        stdout="",
                        stderr=str(e),
                        return_code=-1,
                    )

        return CommandResult(
            host=host,
            success=False,
            stdout="",
            stderr=str(last_exc) if last_exc else "Unknown error",
            return_code=-1,
        )
    
    def execute_on_host(
        self,
        host: str,
        command: str,
        retry: int = 3,
        timeout: int = 300,
    ) -> CommandResult:
        """
        Execute a command on a single host.
        
        Args:
            host: Host IP or hostname
            command: Command to execute
            retry: Number of retries
            timeout: Timeout in seconds
            
        Returns:
            CommandResult
        """
        return self._run_coro(self._run_one(host, command, retry=retry, timeout=timeout))

    async def _execute_on_all_async(
        self,
        hosts: List[str],
        command: str,
        max_workers: int,
        retry: int,
        timeout: int,
    ) -> Dict[str, CommandResult]:
        sem = asyncio.Semaphore(max_workers)

        async def run_host(h: str) -> CommandResult:
            async with sem:
                return await self._run_one(h, command, retry=retry, timeout=timeout)

        results = await asyncio.gather(*(run_host(h) for h in hosts), return_exceptions=True)
        out: Dict[str, CommandResult] = {}
        for host, res in zip(hosts, results):
            if isinstance(res, BaseException):
                out[host] = CommandResult(host=host, success=False, stdout="", stderr=str(res), return_code=-1)
            else:
                out[host] = res
        return out
    
    def execute_on_all(
        self,
        hosts: List[str],
        command: str,
        max_workers: int = 50,
        retry: int = 3,
        timeout: int = 300,
    ) -> Dict[str, CommandResult]:
        """
        Execute a command on multiple hosts in parallel.
        
        Args:
            hosts: List of host IPs
            command: Command to execute
            max_workers: Maximum parallel workers
            retry: Number of retries per host
            timeout: Timeout per host in seconds
            
        Returns:
            Dict mapping host to CommandResult
        """
        return self._run_coro(
            self._execute_on_all_async(
                hosts=hosts,
                command=command,
                max_workers=max_workers,
                retry=retry,
                timeout=timeout,
            )
        )

    async def _run_commands_on_host_async(
        self,
        host: str,
        commands: Dict[str, str],
        retry: int,
        timeout: int,
    ) -> Dict[str, CommandResult]:
        last_exc: Optional[BaseException] = None
        for attempt in range(retry + 1):
            try:
                async with await self._connect(host) as conn:
                    out: Dict[str, CommandResult] = {}
                    for key, cmd in commands.items():
                        res = await asyncio.wait_for(conn.run(cmd, check=False), timeout=timeout)
                        exit_status = res.exit_status if res.exit_status is not None else -1
                        out[key] = CommandResult(
                            host=host,
                            success=exit_status == 0,
                            stdout=_as_text(res.stdout),
                            stderr=_as_text(res.stderr),
                            return_code=int(exit_status),
                        )
                    return out
            except (asyncio.TimeoutError, asyncssh.Error, OSError) as e:
                last_exc = e
                if attempt < retry:
                    await asyncio.sleep(1)
                else:
                    return {k: CommandResult(host=host, success=False, stdout="", stderr=str(e), return_code=-1) for k in commands}
            except Exception as e:
                last_exc = e
                if attempt < retry:
                    await asyncio.sleep(1)
                else:
                    return {k: CommandResult(host=host, success=False, stdout="", stderr=str(e), return_code=-1) for k in commands}

        err = str(last_exc) if last_exc else "Unknown error"
        return {k: CommandResult(host=host, success=False, stdout="", stderr=err, return_code=-1) for k in commands}

    def execute_commands_on_hosts(
        self,
        host_to_commands: Dict[str, Dict[str, str]],
        max_workers: int = 50,
        retry: int = 0,
        timeout: int = 300,
    ) -> Dict[str, Dict[str, CommandResult]]:
        async def runner() -> Dict[str, Dict[str, CommandResult]]:
            sem = asyncio.Semaphore(max_workers)

            async def run_host(h: str, cmds: Dict[str, str]) -> Dict[str, CommandResult]:
                async with sem:
                    return await self._run_commands_on_host_async(h, cmds, retry=retry, timeout=timeout)

            tasks = [run_host(h, cmds) for h, cmds in host_to_commands.items()]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            out: Dict[str, Dict[str, CommandResult]] = {}
            for (h, cmds), res in zip(host_to_commands.items(), results):
                if isinstance(res, BaseException):
                    out[h] = {k: CommandResult(host=h, success=False, stdout="", stderr=str(res), return_code=-1) for k in cmds}
                else:
                    out[h] = res
            return out

        return self._run_coro(runner())
    
    def copy_file(
        self,
        host: str,
        local_path: str,
        remote_path: str,
        retry: int = 3,
    ) -> bool:
        """
        Copy a file to a remote host.
        
        Args:
            host: Host IP
            local_path: Local file path
            remote_path: Remote file path
            retry: Number of retries
            
        Returns:
            True if successful
        """
        async def do_copy() -> bool:
            last_exc: Optional[BaseException] = None
            for attempt in range(retry + 1):
                try:
                    async with await self._connect(host) as conn:
                        remote_dir = os.path.dirname(remote_path)
                        if remote_dir:
                            await conn.run(f"mkdir -p {shlex.quote(remote_dir)}", check=False)
                        async with conn.start_sftp_client() as sftp:
                            await sftp.put(local_path, remote_path)
                        return True
                except Exception as e:
                    last_exc = e
                    if attempt < retry:
                        await asyncio.sleep(1)
                    else:
                        logger.debug(f"SFTP put failed on {host}: {e}")
                        return False
            if last_exc:
                logger.debug(f"SFTP put failed on {host}: {last_exc}")
            return False

        return bool(self._run_coro(do_copy()))
    
    def copy_file_to_all(
        self,
        hosts: List[str],
        local_path: str,
        remote_path: str,
        max_workers: int = 50,
        retry: int = 3,
    ) -> Dict[str, bool]:
        """
        Copy a file to multiple hosts in parallel.
        
        Args:
            hosts: List of host IPs
            local_path: Local file path
            remote_path: Remote file path
            max_workers: Maximum parallel workers
            retry: Number of retries per host
            
        Returns:
            Dict mapping host to success status
        """
        async def runner() -> Dict[str, bool]:
            sem = asyncio.Semaphore(max_workers)

            async def put_one(h: str) -> bool:
                async with sem:
                    # Inline async put to avoid nested event loops.
                    last_exc: Optional[BaseException] = None
                    for attempt in range(retry + 1):
                        try:
                            async with await self._connect(h) as conn:
                                remote_dir = os.path.dirname(remote_path)
                                if remote_dir:
                                    await conn.run(f"mkdir -p {shlex.quote(remote_dir)}", check=False)
                                async with conn.start_sftp_client() as sftp:
                                    await sftp.put(local_path, remote_path)
                                return True
                        except Exception as e:
                            last_exc = e
                            if attempt < retry:
                                await asyncio.sleep(1)
                            else:
                                logger.debug(f"SFTP put failed on {h}: {e}")
                                return False
                    if last_exc:
                        logger.debug(f"SFTP put failed on {h}: {last_exc}")
                    return False

            results = await asyncio.gather(*(put_one(h) for h in hosts), return_exceptions=True)
            out: Dict[str, bool] = {}
            for host, res in zip(hosts, results):
                out[host] = False if isinstance(res, BaseException) else bool(res)
            return out

        return self._run_coro(runner())

    def download_file(
        self,
        host: str,
        remote_path: str,
        local_path: str,
        retry: int = 3,
    ) -> bool:
        """Download a single remote file via SFTP."""

        async def do_get() -> bool:
            last_exc: Optional[BaseException] = None
            for attempt in range(retry + 1):
                try:
                    async with await self._connect(host) as conn:
                        local_dir = os.path.dirname(local_path)
                        if local_dir:
                            os.makedirs(local_dir, exist_ok=True)
                        async with conn.start_sftp_client() as sftp:
                            await sftp.get(remote_path, local_path)
                        return True
                except Exception as e:
                    last_exc = e
                    if attempt < retry:
                        await asyncio.sleep(1)
                    else:
                        logger.debug(f"SFTP get failed on {host}: {e}")
                        return False
            if last_exc:
                logger.debug(f"SFTP get failed on {host}: {last_exc}")
            return False

        return bool(self._run_coro(do_get()))
    
    def copy_content(
        self,
        host: str,
        content: str,
        remote_path: str,
        retry: int = 3,
    ) -> bool:
        """
        Copy content to a file on a remote host.
        
        Args:
            host: Host IP
            content: Content to write
            remote_path: Remote file path
            retry: Number of retries
            
        Returns:
            True if successful
        """
        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tmp') as f:
            f.write(content)
            temp_path = f.name
        
        try:
            return self.copy_file(host, temp_path, remote_path, retry)
        finally:
            os.unlink(temp_path)

    def copy_contents(
        self,
        host: str,
        remote_path_to_content: Dict[str, str],
        retry: int = 3,
        timeout: int = 300,
    ) -> Dict[str, bool]:
        """Write multiple remote files on a host using a single SSH connection."""

        async def do_write() -> Dict[str, bool]:
            last_exc: Optional[BaseException] = None
            for attempt in range(retry + 1):
                try:
                    async with await self._connect(host) as conn:
                        # Ensure all directories exist
                        dirs = {os.path.dirname(p) for p in remote_path_to_content.keys() if os.path.dirname(p)}
                        for d in sorted(dirs):
                            await conn.run(f"mkdir -p {shlex.quote(d)}", check=False)

                        out: Dict[str, bool] = {}
                        async with conn.start_sftp_client() as sftp:
                            for path, content in remote_path_to_content.items():
                                try:
                                    f = await asyncio.wait_for(sftp.open(path, "w"), timeout=timeout)
                                    try:
                                        await asyncio.wait_for(f.write(content), timeout=timeout)
                                    finally:
                                        await f.close()
                                    out[path] = True
                                except Exception as e:
                                    out[path] = False
                                    logger.debug(f"Failed to write {path} on {host}: {e}")
                        return out
                except Exception as e:
                    last_exc = e
                    if attempt < retry:
                        await asyncio.sleep(1)
                    else:
                        return {p: False for p in remote_path_to_content.keys()}

            if last_exc:
                logger.debug(f"copy_contents failed on {host}: {last_exc}")
            return {p: False for p in remote_path_to_content.keys()}

        return self._run_coro(do_write())

    def copy_contents_on_hosts(
        self,
        host_to_files: Dict[str, Dict[str, str]],
        max_workers: int = 50,
        retry: int = 3,
        timeout: int = 300,
    ) -> Dict[str, Dict[str, bool]]:
        """Write multiple files per host, in parallel across hosts."""

        async def runner() -> Dict[str, Dict[str, bool]]:
            sem = asyncio.Semaphore(max_workers)

            async def write_one(host: str, files: Dict[str, str]) -> Dict[str, bool]:
                async with sem:
                    # Reuse the same logic as copy_contents but stay in async.
                    last_exc: Optional[BaseException] = None
                    for attempt in range(retry + 1):
                        try:
                            async with await self._connect(host) as conn:
                                dirs = {os.path.dirname(p) for p in files.keys() if os.path.dirname(p)}
                                for d in sorted(dirs):
                                    await conn.run(f"mkdir -p {shlex.quote(d)}", check=False)

                                out: Dict[str, bool] = {}
                                async with conn.start_sftp_client() as sftp:
                                    for path, content in files.items():
                                        try:
                                            f = await asyncio.wait_for(sftp.open(path, "w"), timeout=timeout)
                                            try:
                                                await asyncio.wait_for(f.write(content), timeout=timeout)
                                            finally:
                                                await f.close()
                                            out[path] = True
                                        except Exception as e:
                                            out[path] = False
                                            logger.debug(f"Failed to write {path} on {host}: {e}")
                                return out
                        except Exception as e:
                            last_exc = e
                            if attempt < retry:
                                await asyncio.sleep(1)
                            else:
                                logger.debug(f"copy_contents_on_hosts failed on {host}: {e}")
                                return {p: False for p in files.keys()}

                    if last_exc:
                        logger.debug(f"copy_contents_on_hosts failed on {host}: {last_exc}")
                    return {p: False for p in files.keys()}

            tasks = [write_one(h, files) for h, files in host_to_files.items()]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            out: Dict[str, Dict[str, bool]] = {}
            for (h, files), res in zip(host_to_files.items(), results):
                if isinstance(res, BaseException):
                    out[h] = {p: False for p in files.keys()}
                else:
                    out[h] = res
            return out

        return self._run_coro(runner())


