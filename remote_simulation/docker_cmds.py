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

    exec_cmd = "./conflux --config /root/config.toml"

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
        exec_cmd
    )

    return " ".join(cmd_startup)


def start_profiler(index: int, duration_s: int = 60) -> str:
    """Return a command string which runs a privileged container that attaches to the
    node container's PID namespace and runs flamegraph against PID 1 for duration_s seconds.
    The profiler writes `/root/log/flame_{index}.svg` and diagnostic files into the node's bound log dir."""
    # Use a two-step approach: write a start marker, perf record attaching to PID 1, then analyze with flamegraph
    # Capture perf stderr and flamegraph stderr, and write an exit code file so we can diagnose failures.
    # Wait for the node container to be present (retry briefly), then run the profiler.
    # If the container never appears, write a diagnostic to the wrapper output to aid debugging.
    script_path = f"~/log{index}/start_profiler_{index}.sh"
    script_lines = [
        "#!/bin/bash",
        "set -euo pipefail",
        f"echo \"SCRIPT_STARTED $(date '+%Y-%m-%d %H:%M:%S')\" >> ~/log{index}/flame_cmd_{index}.out || true",
        f"docker image inspect {IMAGE_TAG} >/dev/null 2>&1 || echo \"IMAGE_MISSING {IMAGE_TAG}\" >> ~/log{index}/flame_cmd_{index}.out || true",
        # Wait longer for node container to appear
        f"for i in $(seq 1 60); do docker ps -q --filter name={container_name(index)} | grep -q . && break || sleep 1; done",
        f"if [ -z \"$(docker ps -q --filter name={container_name(index)})\" ]; then echo \"No such container: {container_name(index)}\" >> ~/log{index}/flame_cmd_{index}.out; exit 0; fi",
        f"echo \"DOCKER_RUN_START $(date '+%Y-%m-%d %H:%M:%S')\" >> ~/log{index}/flame_cmd_{index}.out || true",
        f"docker run --rm --pid=container:{container_name(index)} --privileged -v ~/log{index}:/root/log -w /root {IMAGE_TAG} /bin/bash -lc \"echo 'profiler started $(date '+%Y-%m-%d %H:%M:%S')' > /root/log/flame_start_{index}.txt; perf record -o /root/log/perf.data -g -p 1 -- sleep {duration_s} 2> /root/log/flame_{index}.perf.err || true; flamegraph --perfdata /root/log/perf.data --output /root/log/flame_{index}.svg 2> /root/log/flame_{index}.err || true; echo $? > /root/log/flame_exit_{index}.txt\"",
        f"echo \"DOCKER_RUN_EXIT $(date '+%Y-%m-%d %H:%M:%S')\" >> ~/log{index}/flame_cmd_{index}.out || true",
    ]
    escaped_lines = [line.replace("'", "'\"'\"'") for line in script_lines]
    printf_cmd = "printf '%s\\n' " + " ".join(f"'{line}'" for line in escaped_lines) + f" > {script_path}"

    # Create a host-side script with printf and run it with nohup to avoid heredoc/quoting issues.
    return " ".join((
        f"mkdir -p ~/log{index} &&",
        f"{printf_cmd} &&",
        f"chmod +x {script_path} &&",
        f"nohup bash {script_path} > ~/log{index}/flame_cmd_{index}.out 2>&1 &"
    ))


def stop_node_and_collect_log(index: int, *, user = "ubuntu") -> str:
    # Try to let the process exit cleanly (so flamegraph can finalize), then force-remove.
    stop_node = (
        f"sudo docker kill --signal=SIGINT {container_name(index)} || true && sleep 5 &&",
        f"sudo docker rm -f {container_name(index)} || true",
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

def pull_image():
    return f"sudo docker pull {REMOTE_IMAGE_TAG} && sudo docker tag {REMOTE_IMAGE_TAG} {IMAGE_TAG}"