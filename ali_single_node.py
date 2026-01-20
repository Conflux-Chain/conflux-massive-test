import asyncio
import socket
import tarfile
import time
from pathlib import Path
from typing import Optional

import asyncssh
from eth_account import Account
from loguru import logger
import requests

from ali_instances.config import EcsConfig, client, load_credentials, load_endpoint
from ali_instances.instance_prep import InstanceHandle, auth_port, cleanup_instance, provision_instance, wait_ssh
from remote_simulation.config_builder import SingleNodeConfig, single_node_config_text
from utils.wait_until import wait_until


DEFAULT_REGION_ID = "ap-southeast-3"
RPC_SESSION = requests.Session()
RPC_SESSION.trust_env = False
EVM_PRIVATE_KEY = "46b9e861b63d3509c88b7817275a30d22d62c8cd8fa6486ddee35ef0d8e0495f"
EVM_ADDRESS = "0xfbe45681Ac6C53D5a40475F7526baC1FE7590fb8"


def rpc_call(url: str, method: str, params: Optional[list] = None, timeout: int = 5) -> dict:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}
    response = RPC_SESSION.post(url, json=payload, timeout=timeout)
    response.raise_for_status()
    data = response.json()
    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(f"RPC error for {method}: {data['error']}")
    return data


def wait_for_port_open(host: str, port: int, timeout: int = 180) -> None:
    def is_open() -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(2)
            return sock.connect_ex((host, port)) == 0

    wait_until(is_open, timeout=timeout, retry_interval=2)


def wait_for_rpc(host: str, port: int, timeout: int = 180) -> None:
    wait_for_port_open(host, port, timeout=timeout)
    deadline = time.time() + timeout
    last_error: Optional[Exception] = None
    while time.time() < deadline:
        try:
            rpc_call(f"http://{host}:{port}", "cfx_clientVersion")
            return
        except Exception as exc:
            last_error = exc
            time.sleep(2)
    if last_error:
        raise last_error


def _parse_hex_int(value: Optional[str]) -> int:
    if not value:
        raise RuntimeError("missing hex value")
    return int(value, 16)


def get_chain_id(host: str, port: int) -> int:
    url = f"http://{host}:{port}"
    status = rpc_call(url, "cfx_getStatus")
    chain_id = status.get("result", {}).get("chainId")
    return _parse_hex_int(chain_id)


def wait_for_epoch_increase(host: str, port: int, delta: int, timeout: int = 600) -> None:
    url = f"http://{host}:{port}"
    start = _parse_hex_int(rpc_call(url, "cfx_epochNumber", ["latest_mined"]).get("result"))
    target = start + delta
    current_holder: dict[str, int] = {"value": start}

    def has_advanced() -> bool:
        current = _parse_hex_int(rpc_call(url, "cfx_epochNumber", ["latest_mined"]).get("result"))
        current_holder["value"] = current
        return current >= target

    wait_until(has_advanced, timeout=timeout, retry_interval=2)


def check_block_production(host: str, port: int, wait_blocks: int = 5) -> None:
    wait_for_epoch_increase(host, port, wait_blocks, timeout=240)
    logger.info(f"epoch increased by at least {wait_blocks}")


def send_evm_transaction(url: str, chain_id: int) -> str:
    nonce_hex = rpc_call(url, "eth_getTransactionCount", [EVM_ADDRESS, "latest"]).get("result")
    tx = {
        "chainId": chain_id,
        "from": EVM_ADDRESS,
        "to": EVM_ADDRESS,
        "value": 1,
        "gas": 21000,
        "gasPrice": 1,
        "nonce": int(nonce_hex, 16),
    }
    signed = Account.sign_transaction(tx, EVM_PRIVATE_KEY)
    raw_tx = signed.raw_transaction.hex()
    if not raw_tx.startswith("0x"):
        raw_tx = f"0x{raw_tx}"
    tx_hash = rpc_call(url, "eth_sendRawTransaction", [raw_tx]).get("result")
    logger.info(f"sent evm transaction {tx_hash}")
    return tx_hash


def wait_for_evm_receipt(url: str, tx_hash: str, timeout: int = 180, interval: int = 2) -> dict:
    receipt_holder: dict[str, object] = {}

    def has_receipt() -> bool:
        receipt = rpc_call(url, "eth_getTransactionReceipt", [tx_hash]).get("result")
        if receipt:
            receipt_holder["receipt"] = receipt
            return True
        return False

    wait_until(has_receipt, timeout=timeout, retry_interval=interval)
    logger.info("evm transaction receipt found")
    return receipt_holder.get("receipt") or {}


def generate_test_block(url: str) -> None:
    rpc_call(url, "test_generateOneBlockWithDirectTxGen", [20, 200_000, 20, 0], timeout=120)


def check_transaction_processing(
    host: str,
    port: int,
    evm_port: int,
    expected_chain_id: Optional[int],
    evm_chain_id: int,
) -> None:
    url = f"http://{host}:{port}"
    evm_url = f"http://{host}:{evm_port}"
    chain_id = get_chain_id(host, port)
    if expected_chain_id is not None and chain_id != expected_chain_id:
        logger.warning(f"unexpected chain_id: {chain_id}, expected {expected_chain_id}")

    wait_for_epoch_increase(host, port, delta=50, timeout=900)
    block = rpc_call(url, "cfx_getBlockByEpochNumber", ["latest_mined", True]).get("result")
    conflux_txs = block.get("transactions", []) if block else []
    if conflux_txs:
        logger.info(f"transactions in latest block: {len(conflux_txs)}")
        return

    logger.warning("no conflux transactions found; sending evm transaction")
    try:
        tx_hash = send_evm_transaction(evm_url, evm_chain_id)
        receipt = wait_for_evm_receipt(evm_url, tx_hash, timeout=180)
        receipt_status = receipt.get("status")
        receipt_block = receipt.get("blockNumber")
        if receipt_block and receipt_status != "0x0":
            logger.info(f"evm transaction confirmed in block {receipt_block}")
            return
    except RuntimeError as exc:
        if "Method not found" in str(exc):
            logger.warning("evm rpc not available; falling back to test block generation")
        else:
            raise

    generate_test_block(url)
    wait_for_epoch_increase(host, port, delta=2, timeout=180)
    block = rpc_call(url, "cfx_getBlockByEpochNumber", ["latest_mined", True]).get("result")
    conflux_txs = block.get("transactions", []) if block else []
    logger.info(f"transactions in latest block: {len(conflux_txs)}")


def _pos_config_source() -> Path:
    return Path(__file__).resolve().parent / "ref" / "zero-gravity-swap" / "pos_config"


async def deploy_conflux_node(host: str, instance: EcsConfig, node: SingleNodeConfig) -> None:
    await wait_ssh(host, instance.ssh_username, instance.ssh_private_key_path, instance.wait_timeout)
    key_path = str(Path(instance.ssh_private_key_path).expanduser())
    conn = await asyncssh.connect(
        host,
        username=instance.ssh_username,
        client_keys=[key_path],
        known_hosts=None,
    )
    async with conn:
        async def run_remote(cmd: str, check: bool = True) -> None:
            logger.info(f">>> {cmd}")
            result = await conn.run(cmd, check=check)
            if result.stdout:
                logger.info(result.stdout.strip())
            if result.stderr:
                logger.warning(result.stderr.strip())

        await run_remote("sudo mkdir -p /opt/conflux", check=True)
        await run_remote("sudo mkdir -p /opt/conflux/config", check=True)
        await run_remote(f"sudo mkdir -p {node.data_dir}", check=True)
        await run_remote("sudo mkdir -p /opt/conflux/logs", check=True)
        await run_remote("sudo mkdir -p /opt/conflux/pos_config", check=True)
        await run_remote(
            "sudo bash -lc 'set -e; "
            "if [ ! -x "
            f"{node.conflux_bin}"
            " ]; then "
            "if [ -d /opt/conflux/src/conflux-rust ]; then "
            "cd /opt/conflux/src/conflux-rust; "
            "if [ -f $HOME/.cargo/env ]; then . $HOME/.cargo/env; fi; "
            "cargo build --release --bin conflux; "
            "install -m 0755 target/release/conflux /usr/local/bin/conflux; "
            "fi; "
            "fi'",
            check=True,
        )
        await run_remote(f"sudo test -x {node.conflux_bin}", check=True)

        config_text = single_node_config_text(node)
        local_path = f"/tmp/conflux_{int(time.time())}.toml"
        Path(local_path).write_text(config_text)
        remote_path = "/opt/conflux/config/conflux_0.toml"
        await asyncssh.scp(local_path, (conn, remote_path))
        Path(local_path).unlink(missing_ok=True)

        pos_config_local = _pos_config_source()
        if not pos_config_local.exists():
            raise FileNotFoundError(f"pos_config not found: {pos_config_local}")
        pos_archive = Path(f"/tmp/pos_config_{int(time.time())}.tar.gz")
        with tarfile.open(pos_archive, "w:gz") as tar:
            tar.add(pos_config_local, arcname="pos_config")
        remote_archive = f"/tmp/{pos_archive.name}"
        await asyncssh.scp(str(pos_archive), (conn, remote_archive))
        pos_archive.unlink(missing_ok=True)
        await run_remote(f"sudo tar -xzf {remote_archive} -C /opt/conflux/pos_config --strip-components=1", check=True)
        await run_remote(f"sudo rm -f {remote_archive}", check=False)
        await run_remote("sudo mkdir -p /opt/conflux/pos_config/log", check=True)
        await run_remote("sudo mkdir -p /app", check=True)
        await run_remote("sudo ln -sfn /opt/conflux/pos_config /app/pos_config", check=True)

        await run_remote("sudo pkill -f 'conflux_0.toml' 2>/dev/null || true", check=False)
        await run_remote(
            " ".join(
                [
                    f"sudo nohup {node.conflux_bin}",
                    "--config /opt/conflux/config/conflux_0.toml",
                    "> /opt/conflux/logs/conflux_0.log 2>&1 &",
                ]
            ),
            check=True,
        )
        await asyncio.sleep(5)

        info_cmds = [
            f"sudo pgrep -af '{node.conflux_bin}' || true",
            "sudo tail -n 200 /opt/conflux/logs/conflux_0.log || true",
            f"sudo ss -ltnp | grep :{node.rpc_port} || true",
            f'sudo curl -sS -H "Content-Type: application/json" -d \'{{"jsonrpc":"2.0","id":1,"method":"cfx_clientVersion","params":[]}}\' http://127.0.0.1:{node.rpc_port} || true',
            f"sudo ss -ltnp | grep :{node.evm_rpc_port} || true",
            f'sudo curl -sS -H "Content-Type: application/json" -d \'{{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}}\' http://127.0.0.1:{node.evm_rpc_port} || true',
        ]
        for cmd in info_cmds:
            res = await conn.run(cmd, check=False)
            if res.stdout:
                logger.info(f"remote: {cmd}\n{res.stdout.strip()}")
            if res.stderr:
                logger.warning(f"remote stderr: {cmd}\n{res.stderr.strip()}")


async def stop_conflux_node(host: str, instance: EcsConfig) -> None:
    key_path = str(Path(instance.ssh_private_key_path).expanduser())
    async with asyncssh.connect(
        host,
        username=instance.ssh_username,
        client_keys=[key_path],
        known_hosts=None,
    ) as conn:
        await conn.run("sudo pkill -f 'conflux_0.toml' 2>/dev/null || true", check=False)


async def start_docker_conflux_service(host: str, instance: EcsConfig, service_name: str) -> None:
    await wait_ssh(host, instance.ssh_username, instance.ssh_private_key_path, instance.wait_timeout)
    key_path = str(Path(instance.ssh_private_key_path).expanduser())
    async with asyncssh.connect(
        host,
        username=instance.ssh_username,
        client_keys=[key_path],
        known_hosts=None,
    ) as conn:
        setup_cmd = (
            "sudo bash -lc 'set -e; "
            f"if [ ! -f /etc/systemd/system/{service_name}.service ]; then "
            "cat > /usr/local/bin/"
            f"{service_name}-run.sh << \"EOF\"\n"
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            f"docker rm -f {service_name} >/dev/null 2>&1 || true\n"
            "exec docker run --rm \\\n  --name "
            f"{service_name} \\\n  --net=host \\\n  --privileged \\\n  --ulimit nofile=65535:65535 \\\n  --ulimit nproc=65535:65535 \\\n  --ulimit core=-1 \\\n  -v /opt/conflux/config:/opt/conflux/config \\\n  -v /opt/conflux/data:/opt/conflux/data \\\n  -v /opt/conflux/logs:/opt/conflux/logs \\\n  -v /opt/conflux/pos_config:/opt/conflux/pos_config \\\n  -v /opt/conflux/pos_config:/app/pos_config \\\n  -w /opt/conflux/logs \\\n  conflux-single-node:latest \\\n  /root/conflux --config /opt/conflux/config/conflux_0.toml\n"
            "EOF\n"
            f"chmod +x /usr/local/bin/{service_name}-run.sh; "
            "cat > /etc/systemd/system/"
            f"{service_name}.service << \"EOF\"\n"
            "[Unit]\n"
            f"Description=Conflux single node ({service_name})\n"
            "After=docker.service\n"
            "Requires=docker.service\n\n"
            "[Service]\n"
            "Type=simple\n"
            f"ExecStart=/usr/local/bin/{service_name}-run.sh\n"
            f"ExecStop=/usr/bin/docker stop {service_name}\n"
            "Restart=always\n"
            "RestartSec=5\n\n"
            "[Install]\n"
            "WantedBy=multi-user.target\n"
            "EOF\n"
            "systemctl daemon-reload; "
            f"systemctl enable {service_name}.service; "
            "fi'"
        )
        await conn.run(setup_cmd, check=False)
        await conn.run(f"sudo systemctl start {service_name}.service", check=False)
        await conn.run(f"sudo systemctl status {service_name}.service --no-pager", check=False)


async def stop_docker_conflux_service(host: str, instance: EcsConfig, service_name: str) -> None:
    key_path = str(Path(instance.ssh_private_key_path).expanduser())
    async with asyncssh.connect(
        host,
        username=instance.ssh_username,
        client_keys=[key_path],
        known_hosts=None,
    ) as conn:
        await conn.run(f"sudo systemctl stop {service_name}.service", check=False)


def authorize_instance_ports(instance: InstanceHandle, node: SingleNodeConfig) -> None:
    if not instance.config.security_group_id:
        raise RuntimeError("missing security_group_id after provisioning")
    for port in [node.rpc_port, node.ws_port, node.evm_rpc_port, node.evm_ws_port]:
        auth_port(instance.client, instance.config.region_id, instance.config.security_group_id, port)


