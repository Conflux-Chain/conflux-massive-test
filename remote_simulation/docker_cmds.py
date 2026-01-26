from remote_simulation.port_allocation import p2p_port, rpc_port, pubsub_port, remote_rpc_port, evm_rpc_port, evm_rpc_ws_port

# REMOTE_IMAGE_TAG = "public.ecr.aws/s9d3x9f5/conflux-massive-test/conflux-node:latest"
REMOTE_IMAGE_TAG = "lylcx2007/conflux-node:latest"
IMAGE_TAG="conflux-node:latest"

CONTAINER_PREFIX = "conflux_node_"

def container_name(index: int) -> str:
    return f"{CONTAINER_PREFIX}{index}"

def collect_log_container_name(index: int) -> str:
    return f"{CONTAINER_PREFIX}{index}"

def launch_node(index: int) -> str:
    port_cmd = (
        f"-p {p2p_port(index)}:{p2p_port(0)}",
        f"-p {rpc_port(index)}:{rpc_port(0)}",
        f"-p {pubsub_port(index)}:{pubsub_port(0)}",
        f"-p {remote_rpc_port(index)}:{remote_rpc_port(0)}",
        f"-p {evm_rpc_port(index)}:{evm_rpc_port(0)}",
        f"-p {evm_rpc_ws_port(index)}:{evm_rpc_ws_port(0)}"
    )

    cmd_startup = (
        f"sudo rm -rf ~/log{index} &&",
        f"mkdir ~/log{index} &&",
        "sudo docker run -d",
        f"--name {container_name(index)}",          
        "-v ~/config.toml:/root/config.toml:ro",
        f"-v ~/log{index}:/root/log",
        "--privileged",                          # 如果需要调整内核参数或高权限
        *port_cmd,
        "-w /root",                              # 设置工作目录
        IMAGE_TAG, 
        "./conflux --config /root/config.toml"
    )

    return " ".join(cmd_startup)

def stop_node_and_collect_log(index: int, *, user = "ubuntu") -> str:
    stop_node = (
        f"sudo docker stop {container_name(index)}",
    )
    collect_logs = (
        f"sudo rm -rf ~/output{index} &&",
        f"mkdir ~/output{index} &&",
        "sudo docker run --rm",
        f"--name {container_name(index)}_collect",          
        f"-v ~/log{index}:/root/log:ro",
        f"-v ~/output{index}:/root/output",
        "-w /root",                       
        IMAGE_TAG, 
        "/bin/bash -c ./collect_logs.sh &&",
        f"sudo chown {user}:{user} ~/output{index}/*"
    )

    return " ".join(stop_node) + " && " + " ".join(collect_logs)

def stop_all_nodes() -> str:
    return f"sudo docker ps -aq --filter name={CONTAINER_PREFIX} | xargs -r sudo docker stop"

def destory_all_nodes() -> str:
    return f"sudo docker ps -aq --filter name={CONTAINER_PREFIX} | xargs -r sudo docker rm -f && sudo rm -rf ~/log* && sudo rm -rf ~/output*"

def pull_image(retries: int = 3, delay_seconds: int = 10) -> str:
    return (
        "set -e; "
        f"for i in $(seq 1 {retries}); do "
        "  if sudo docker info >/dev/null 2>&1; then "
        f"    if sudo docker pull {REMOTE_IMAGE_TAG} && sudo docker tag {REMOTE_IMAGE_TAG} {IMAGE_TAG}; then exit 0; fi; "
        "  fi; "
        "  if command -v systemctl >/dev/null 2>&1; then sudo systemctl restart docker; "
        "  elif command -v service >/dev/null 2>&1; then sudo service docker restart; fi; "
        f"  sleep {delay_seconds}; "
        "done; "
        "exit 1"
    )