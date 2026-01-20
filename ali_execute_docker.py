import argparse
import asyncio
import tarfile
import time
from pathlib import Path
from typing import Optional

import asyncssh
from loguru import logger

from ali_instances.ali import (
    EcsConfig,
    add_args,
    auth_port,
    cleanup_instance,
    client,
    ensure_keypair,
    find_ubuntu,
    load_endpoint,
    provision_instance,
    wait_ssh,
)
from ali_single_node import check_transaction_processing, wait_for_rpc
from remote_simulation.config_builder import SingleNodeConfig, single_node_config_text

DEFAULT_DOCKER_IMAGE = "2474101468/conflux-single-node:latest"
DEFAULT_SERVICE_NAME = "conflux-docker"


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


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Conflux Docker image on Aliyun and verify it")
    add_args(parser)
    parser.add_argument("--rpc-port", type=int, default=12537)
    parser.add_argument("--ws-port", type=int, default=12538)
    parser.add_argument("--evm-rpc-port", type=int, default=12539)
    parser.add_argument("--evm-ws-port", type=int, default=12540)
    parser.add_argument("--chain-id", type=int, default=1024)
    parser.add_argument("--evm-chain-id", type=int, default=1025)
    parser.add_argument("--mining-author", default=None)
    parser.add_argument("--docker-image", default=DEFAULT_DOCKER_IMAGE)
    parser.add_argument("--service-name", default=DEFAULT_SERVICE_NAME)
    return parser


def cfg_from_args(a) -> EcsConfig:
    return EcsConfig(
        credentials=EcsConfig().credentials,
        region_id=a.region_id,
        zone_id=a.zone_id,
        endpoint=a.endpoint or load_endpoint(),
        base_image_id=a.base_image_id,
        instance_type=a.instance_type,
        min_cpu_cores=a.min_cpu_cores,
        min_memory_gb=a.min_memory_gb,
        max_memory_gb=a.max_memory_gb,
        use_spot=a.spot,
        spot_strategy=a.spot_strategy,
        v_switch_id=a.v_switch_id,
        security_group_id=a.security_group_id,
        vpc_name=a.vpc_name,
        vswitch_name=a.vswitch_name,
        security_group_name=a.security_group_name,
        vpc_cidr=a.vpc_cidr,
        vswitch_cidr=a.vswitch_cidr,
        key_pair_name=a.key_pair_name,
        ssh_username=a.ssh_username,
        ssh_private_key_path=a.ssh_private_key,
        conflux_git_ref=a.conflux_git_ref,
        image_prefix=a.image_prefix,
        internet_max_bandwidth_out=a.internet_max_bandwidth_out,
        search_all_regions=a.search_all_regions,
        cleanup_builder_instance=True,
        poll_interval=a.poll_interval,
        wait_timeout=a.wait_timeout,
    )


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    cfg = cfg_from_args(args)

    base_client = client(cfg.credentials, cfg.region_id, cfg.endpoint)
    ensure_keypair(base_client, cfg.region_id, cfg.key_pair_name, cfg.ssh_private_key_path)
    if not cfg.base_image_id and not cfg.image_id:
        cfg.base_image_id = find_ubuntu(base_client, cfg.region_id)

    node = SingleNodeConfig(
        rpc_port=args.rpc_port,
        ws_port=args.ws_port,
        evm_rpc_port=args.evm_rpc_port,
        evm_ws_port=args.evm_ws_port,
        chain_id=args.chain_id,
        evm_chain_id=args.evm_chain_id,
        mining_author=args.mining_author,
    )

    instance: Optional[object] = None
    try:
        instance = provision_instance(cfg)
        if not instance.config.security_group_id:
            raise RuntimeError("missing security_group_id")
        for port in [node.rpc_port, node.ws_port, node.evm_rpc_port, node.evm_ws_port]:
            auth_port(instance.client, instance.config.region_id, instance.config.security_group_id, port)

        asyncio.run(deploy_docker_conflux(instance.public_ip, instance.config, node, args.docker_image, args.service_name))
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
