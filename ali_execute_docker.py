import asyncio
import tarfile
import time
from pathlib import Path
from typing import Optional

import asyncssh
from loguru import logger

from ali_instances.config import EcsConfig, client
from ali_instances.image_build import DEFAULT_IMAGE_NAME, create_server_image, find_img, find_ubuntu, prepare_docker_server_image
from ali_instances.instance_prep import auth_port, cleanup_instance, ensure_keypair, provision_instance, wait_ssh
from ali_single_node import check_transaction_processing, wait_for_rpc
from remote_simulation.config_builder import SingleNodeConfig, single_node_config_text

DEFAULT_DOCKER_IMAGE = "2474101468/conflux-single-node:latest"
DEFAULT_SERVICE_NAME = "conflux-docker"
DEFAULT_RPC_PORT = 12537
DEFAULT_WS_PORT = 12538
DEFAULT_EVM_RPC_PORT = 12539
DEFAULT_EVM_WS_PORT = 12540
DEFAULT_CHAIN_ID = 1024
DEFAULT_EVM_CHAIN_ID = 1025


def _pos_config_source() -> Path:
    return Path(__file__).resolve().parent / "ref" / "zero-gravity-swap" / "pos_config"


async def deploy_docker_conflux(host: str, cfg: EcsConfig, node: SingleNodeConfig, image: str, service_name: str) -> None:
    await wait_ssh(host, cfg.ssh_username, cfg.ssh_private_key_path, cfg.wait_timeout)
    key_path = str(Path(cfg.ssh_private_key_path).expanduser())
    async with asyncssh.connect(host, username=cfg.ssh_username, client_keys=[key_path], known_hosts=None) as conn:
        async def run(cmd: str, check: bool = True) -> None:
            logger.info(f"remote: {cmd}")
            r = await conn.run(cmd, check=False)
            if r.stdout:
                logger.info(r.stdout.strip())
            if r.stderr:
                logger.warning(r.stderr.strip())
            if check and r.exit_status != 0:
                raise RuntimeError(f"failed: {cmd}")

        await run("sudo apt-get update -y")
        await run("sudo apt-get install -y docker.io ca-certificates curl tar")
        await run("sudo systemctl enable --now docker")

        for d in ["/opt/conflux/config", node.data_dir, "/opt/conflux/logs", "/opt/conflux/pos_config"]:
            await run(f"sudo mkdir -p {d}")

        config_text = single_node_config_text(node)
        local_cfg = Path(f"/tmp/conflux_{int(time.time())}.toml")
        local_cfg.write_text(config_text)
        await asyncssh.scp(str(local_cfg), (conn, "/opt/conflux/config/conflux_0.toml"))
        local_cfg.unlink(missing_ok=True)

        pos_config = _pos_config_source()
        if not pos_config.exists():
            raise FileNotFoundError(f"pos_config not found: {pos_config}")
        pos_archive = Path(f"/tmp/pos_config_{int(time.time())}.tar.gz")
        with tarfile.open(pos_archive, "w:gz") as tar:
            tar.add(pos_config, arcname="pos_config")
        await asyncssh.scp(str(pos_archive), (conn, f"/tmp/{pos_archive.name}"))
        pos_archive.unlink(missing_ok=True)
        await run(f"sudo tar -xzf /tmp/{pos_archive.name} -C /opt/conflux/pos_config --strip-components=1")
        await run("sudo mkdir -p /opt/conflux/pos_config/log")

        await run(f"sudo docker pull {image}")
        await run(f"sudo docker rm -f {service_name} >/dev/null 2>&1 || true", check=False)
        await run(
            " ".join(
                [
                    "sudo docker run -d",
                    f"--name {service_name}",
                    "--net=host",
                    "--privileged",
                    "--ulimit nofile=65535:65535",
                    "--ulimit nproc=65535:65535",
                    "--ulimit core=-1",
                    "-v /opt/conflux/config:/opt/conflux/config",
                    f"-v {node.data_dir}:{node.data_dir}",
                    "-v /opt/conflux/logs:/opt/conflux/logs",
                    "-v /opt/conflux/pos_config:/opt/conflux/pos_config",
                    "-v /opt/conflux/pos_config:/app/pos_config",
                    "-w /opt/conflux/logs",
                    image,
                    "/root/conflux --config /opt/conflux/config/conflux_0.toml",
                ]
            ),
            check=True,
        )

        await asyncio.sleep(5)
        await run("sudo docker ps --no-trunc | head -n 5", check=False)


def main() -> None:
    cfg = EcsConfig()
    base_client = client(cfg.credentials, cfg.region_id, cfg.endpoint)
    ensure_keypair(base_client, cfg.region_id, cfg.key_pair_name, cfg.ssh_private_key_path)
    if not cfg.base_image_id and not cfg.image_id:
        cfg.base_image_id = find_ubuntu(base_client, cfg.region_id)
    existing_image = find_img(base_client, cfg.region_id, DEFAULT_IMAGE_NAME)
    cfg.image_id = existing_image or create_server_image(cfg, prepare_fn=prepare_docker_server_image)

    node = SingleNodeConfig(
        rpc_port=DEFAULT_RPC_PORT,
        ws_port=DEFAULT_WS_PORT,
        evm_rpc_port=DEFAULT_EVM_RPC_PORT,
        evm_ws_port=DEFAULT_EVM_WS_PORT,
        chain_id=DEFAULT_CHAIN_ID,
        evm_chain_id=DEFAULT_EVM_CHAIN_ID,
        mining_author=None,
    )

    instance: Optional[object] = None
    try:
        instance = provision_instance(cfg)
        if not instance.config.security_group_id:
            raise RuntimeError("missing security_group_id")
        for port in [node.rpc_port, node.ws_port, node.evm_rpc_port, node.evm_ws_port]:
            auth_port(instance.client, instance.config.region_id, instance.config.security_group_id, port)

        asyncio.run(deploy_docker_conflux(instance.public_ip, instance.config, node, DEFAULT_DOCKER_IMAGE, DEFAULT_SERVICE_NAME))
        wait_for_rpc(instance.public_ip, node.rpc_port, timeout=300)
        check_transaction_processing(instance.public_ip, node.rpc_port, node.evm_rpc_port, node.chain_id, node.evm_chain_id)
        logger.info("conflux single-node verification succeeded")
    finally:
        if instance is not None:
            try:
                cleanup_instance(instance)
            except Exception as exc:
                logger.warning(f"cleanup failed: {exc}")


if __name__ == "__main__":
    main()
