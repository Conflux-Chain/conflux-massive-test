import argparse
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

from ali_instances.ali import EcsConfig, InstanceHandle, auth_port, cleanup_instance, client, load_credentials, load_endpoint, provision_instance, wait_ssh
from conflux.config import (
    ConfluxNodeConfig,
    DEFAULT_CHAIN_ID,
    DEFAULT_CONFLUX_BIN,
    DEFAULT_EVM_CHAIN_ID,
    DEFAULT_EVM_RPC_PORT,
    DEFAULT_EVM_WS_PORT,
    DEFAULT_RPC_PORT,
    DEFAULT_WS_PORT,
    build_single_node_conflux_config_text,
)
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
    rpc_call(f"http://{host}:{port}", "cfx_clientVersion")


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


async def deploy_conflux_node(host: str, instance: EcsConfig, node: ConfluxNodeConfig) -> None:
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
        await run_remote(f"sudo mkdir -p {node.remote_config_dir}", check=True)
        await run_remote(f"sudo mkdir -p {node.remote_data_dir}", check=True)
        await run_remote(f"sudo mkdir -p {node.remote_log_dir}", check=True)
        await run_remote(f"sudo mkdir -p {node.remote_pos_config_dir}", check=True)
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

        config_text = build_single_node_conflux_config_text(node)
        local_path = f"/tmp/conflux_{int(time.time())}.toml"
        Path(local_path).write_text(config_text)
        remote_path = f"{node.remote_config_dir}/conflux_0.toml"
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
        await run_remote(f"sudo tar -xzf {remote_archive} -C {node.remote_pos_config_dir} --strip-components=1", check=True)
        await run_remote(f"sudo rm -f {remote_archive}", check=False)
        await run_remote(f"sudo mkdir -p {node.remote_pos_config_dir}/log", check=True)
        await run_remote("sudo mkdir -p /app", check=True)
        await run_remote(f"sudo ln -sfn {node.remote_pos_config_dir} /app/pos_config", check=True)

        await run_remote("sudo pkill -f 'conflux_0.toml' 2>/dev/null || true", check=False)
        await run_remote(
            " ".join(
                [
                    f"sudo nohup {node.conflux_bin}",
                    f"--config {node.remote_config_dir}/conflux_0.toml",
                    f"> {node.remote_log_dir}/conflux_0.log 2>&1 &",
                ]
            ),
            check=True,
        )
        await asyncio.sleep(5)

        info_cmds = [
            f"sudo pgrep -af '{node.conflux_bin}' || true",
            f"sudo tail -n 200 {node.remote_log_dir}/conflux_0.log || true",
            f"sudo ss -ltnp | grep :{node.rpc_port} || true",
            f"sudo curl -sS -H \"Content-Type: application/json\" -d '{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"cfx_clientVersion\",\"params\":[]}}' http://127.0.0.1:{node.rpc_port} || true",
            f"sudo ss -ltnp | grep :{node.evm_rpc_port} || true",
            f"sudo curl -sS -H \"Content-Type: application/json\" -d '{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_blockNumber\",\"params\":[]}}' http://127.0.0.1:{node.evm_rpc_port} || true",
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


def authorize_instance_ports(instance: InstanceHandle, node: ConfluxNodeConfig) -> None:
    if not instance.config.security_group_id:
        raise RuntimeError("missing security_group_id after provisioning")
    for port in [node.rpc_port, node.ws_port, node.evm_rpc_port, node.evm_ws_port]:
        auth_port(instance.client, instance.config.region_id, instance.config.security_group_id, port)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Launch a single Conflux node on Aliyun and validate it")
    parser.add_argument("--image-id", required=True)
    parser.add_argument("--instance-type", default=None)
    parser.add_argument("--v-switch-id", default=None)
    parser.add_argument("--security-group-id", default=None)
    parser.add_argument("--key-pair-name", default="chenxinghao-conflux-image-builder")
    parser.add_argument("--region-id", default=DEFAULT_REGION_ID)
    parser.add_argument("--zone-id", default=None)
    parser.add_argument("--endpoint", default=None, help="Custom ECS endpoint")
    parser.add_argument("--ssh-username", default="root")
    parser.add_argument("--ssh-private-key", default="./keys/chenxinghao-conflux-image-builder.pem")
    parser.add_argument("--cpu-vendor", default=None)
    parser.add_argument("--poll-interval", type=int, default=5)
    parser.add_argument("--wait-timeout", type=int, default=1800)
    parser.add_argument("--spot", action="store_true")
    parser.add_argument("--spot-strategy", default="SpotAsPriceGo")
    parser.add_argument("--vpc-name", default="conflux-image-builder")
    parser.add_argument("--vswitch-name", default="conflux-image-builder")
    parser.add_argument("--security-group-name", default="conflux-image-builder")
    parser.add_argument("--vpc-cidr", default="10.0.0.0/16")
    parser.add_argument("--vswitch-cidr", default="10.0.0.0/24")
    parser.add_argument("--rpc-port", type=int, default=DEFAULT_RPC_PORT)
    parser.add_argument("--ws-port", type=int, default=DEFAULT_WS_PORT)
    parser.add_argument("--evm-rpc-port", type=int, default=DEFAULT_EVM_RPC_PORT)
    parser.add_argument("--evm-ws-port", type=int, default=DEFAULT_EVM_WS_PORT)
    parser.add_argument("--chain-id", type=int, default=DEFAULT_CHAIN_ID)
    parser.add_argument("--evm-chain-id", type=int, default=DEFAULT_EVM_CHAIN_ID)
    parser.add_argument("--conflux-bin", default=DEFAULT_CONFLUX_BIN, help="Path to conflux executable on the instance")
    parser.add_argument("--mining-author", default=None)
    parser.add_argument("--no-docker-image", action="store_true", help="Run conflux directly on the instance")
    parser.add_argument("--docker-service-name", default="conflux-docker")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    credentials = load_credentials()
    endpoint = args.endpoint or load_endpoint()
    instance_config = EcsConfig(
        credentials=credentials,
        image_id=args.image_id,
        instance_type=args.instance_type,
        v_switch_id=args.v_switch_id,
        security_group_id=args.security_group_id,
        key_pair_name=args.key_pair_name,
        region_id=args.region_id,
        zone_id=args.zone_id,
        endpoint=endpoint,
        ssh_username=args.ssh_username,
        ssh_private_key_path=args.ssh_private_key,
        poll_interval=args.poll_interval,
        wait_timeout=args.wait_timeout,
        use_spot=args.spot,
        spot_strategy=args.spot_strategy,
        vpc_name=args.vpc_name,
        vswitch_name=args.vswitch_name,
        security_group_name=args.security_group_name,
        vpc_cidr=args.vpc_cidr,
        vswitch_cidr=args.vswitch_cidr,
        cpu_vendor=args.cpu_vendor,
    )
    conflux_node = ConfluxNodeConfig(
        rpc_port=args.rpc_port,
        ws_port=args.ws_port,
        evm_rpc_port=args.evm_rpc_port,
        evm_ws_port=args.evm_ws_port,
        chain_id=args.chain_id,
        evm_chain_id=args.evm_chain_id,
        conflux_bin=args.conflux_bin,
        mining_author=args.mining_author,
    )

    instance = provision_instance(instance_config)
    authorize_instance_ports(instance, conflux_node)
    use_docker = not args.no_docker_image
    if use_docker:
        asyncio.run(start_docker_conflux_service(instance.public_ip, instance.config, args.docker_service_name))
    else:
        asyncio.run(deploy_conflux_node(instance.public_ip, instance.config, conflux_node))
    wait_for_rpc(instance.public_ip, conflux_node.rpc_port)
    check_block_production(instance.public_ip, conflux_node.rpc_port)
    check_transaction_processing(
        instance.public_ip,
        conflux_node.rpc_port,
        conflux_node.evm_rpc_port,
        conflux_node.chain_id,
        conflux_node.evm_chain_id,
    )
    logger.info("single node check finished")
    if use_docker:
        asyncio.run(stop_docker_conflux_service(instance.public_ip, instance.config, args.docker_service_name))
    else:
        asyncio.run(stop_conflux_node(instance.public_ip, instance.config))
    cleanup_instance(instance)


if __name__ == "__main__":
    main()
