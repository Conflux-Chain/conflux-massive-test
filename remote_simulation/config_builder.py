from dataclasses import asdict, dataclass
from remote_simulation.port_allocation import p2p_port, rpc_port, pubsub_port, remote_rpc_port, evm_rpc_port, evm_rpc_ws_port
import conflux.config

from loguru import logger
from utils.tempfile import TempFile
from typing import Dict, Any, Optional


@dataclass
class SingleNodeConfig:
    """Configuration for a single Conflux node in dev mode."""
    rpc_port: int = 12537
    ws_port: int = 12538
    evm_rpc_port: int = 12539
    evm_ws_port: int = 12540
    chain_id: int = 1024
    evm_chain_id: int = 1025
    conflux_bin: str = "/root/conflux"
    data_dir: str = "/opt/conflux/data"
    log_file: str = "/opt/conflux/logs/conflux.log"
    metrics_file: str = "/opt/conflux/logs/metrics.log"
    pos_config_path: str = "/opt/conflux/pos_config/pos_config.yaml"
    pos_initial_nodes_path: str = "/opt/conflux/pos_config/initial_nodes.json"
    pos_private_key_path: str = "/opt/conflux/pos_config/pos_key"
    node_type: str = "archive"
    mode: str = "dev"
    dev_block_interval_ms: int = 250
    dev_pos_private_key_encryption_password: str = "CFXV20"
    mining_author: Optional[str] = None
    start_mining: bool = True
    generate_tx: bool = True
    generate_tx_period_us: int = 100000
    txgen_account_count: int = 10
    db_cache_size: int = 128
    ledger_cache_size: int = 1024
    tx_pool_size: int = 500000
    storage_delta_mpts_cache_size: int = 200_000
    storage_delta_mpts_cache_start_size: int = 200_000
    storage_delta_mpts_slab_idle_size: int = 2_000_000
    tanzanite_transition_height: int = 4
    default_transition_time: int = 1
    hydra_transition_number: int = 5
    hydra_transition_height: int = 5
    cip43_init_end_number: int = 5
    pos_reference_enable_height: int = 0
    dao_vote_transition_number: int = 6
    dao_vote_transition_height: int = 6
    sigma_fix_transition_number: int = 6
    cip107_transition_number: int = 7
    cip112_transition_height: int = 7
    cip118_transition_number: int = 7
    cip119_transition_number: int = 7
    base_fee_burn_transition_number: int = 10
    base_fee_burn_transition_height: int = 10
    c2_fix_transition_height: int = 11
    eoa_code_transition_height: int = 12
    check_phase_change_period_ms: int = 100
    enable_discovery: bool = False
    metrics_enabled: bool = True
    session_ip_limits: str = "0,0,0,0"
    mining_type: str = "disable"
    subnet_quota: int = 0
    persist_tx_index: bool = True
    persist_block_number_index: bool = True
    execute_genesis: bool = False
    dev_allow_phase_change_without_peer: bool = True
    check_status_genesis: bool = False
    min_phase_change_normal_peer_count: int = 1
    enable_single_mpt_storage: bool = True
    rpc_enable_metrics: bool = True
    public_rpc_apis: str = "all"
    public_evm_rpc_apis: str = "all"


def _fmt_val(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, str):
        return f"\"{v}\""
    if isinstance(v, int):
        return str(v)
    if isinstance(v, list):
        return "[" + ", ".join(_fmt_val(x) for x in v) + "]"
    return str(v)


def single_node_config_text(cfg: SingleNodeConfig) -> str:
    lines: list[str] = []
    lines.append('bootnodes = ""')
    lines.append('jsonrpc_http_host = "0.0.0.0"')
    lines.append('jsonrpc_ws_host = "0.0.0.0"')
    lines.append('jsonrpc_http_eth_host = "0.0.0.0"')
    lines.append('jsonrpc_ws_eth_host = "0.0.0.0"')
    field_map = {
        "rpc_port": "jsonrpc_http_port",
        "ws_port": "jsonrpc_ws_port",
        "evm_rpc_port": "jsonrpc_http_eth_port",
        "evm_ws_port": "jsonrpc_ws_eth_port",
        "data_dir": "conflux_data_dir",
        "log_file": "log_file",
        "metrics_file": "metrics_output_file",
    }
    skip_fields = {"conflux_bin"}
    for fld in cfg.__dataclass_fields__:
        if fld in skip_fields:
            continue
        val = getattr(cfg, fld)
        if val is None:
            continue
        key = field_map.get(fld, fld)
        lines.append(f"{key} = {_fmt_val(val)}")
    return "\n".join(lines)


@dataclass
class SimulateOptions:
    """Simulation environment configuration."""
    target_tps: int = 1000
    target_nodes: int = 100
    nodes_per_host: int = 3

    bandwidth: int = 20  # Bandwidth in Mbit/s
    connect_peers: int = 3
    enable_flamegraph: bool = False
    # enable_tx_propagation: bool = True
    generation_period_ms: int = 500
    num_blocks: int = 1000
    storage_memory_gb: int = 2



@dataclass
class ConfluxOptions:
    """Options passed directly to the Conflux node."""
    egress_min_throttle: int = 512
    egress_max_throttle: int = 1024
    egress_queue_capacity: int = 2048
    genesis_secrets: str = "./genesis_secrets.txt"
    log_file: str = "./log/conflux.log"
    metrics_output_file: str = "./log/metrics.log"
    send_tx_period_ms: int = 1300
    txgen_account_count: int = 100
    txgen_batch_size: int = 10
    tx_pool_size: int = conflux.config.default_conflux_conf["tx_pool_size"]
    max_block_size_in_bytes: int = 200 * 1024
    execution_prefetch_threads: int = 8
    target_block_gas_limit: int = 30_000_000

    
    # PoS / Transition configurations
    cip1559_transition_height: int = 4294967295
    hydra_transition_number: int = 4294967295
    hydra_transition_height: int = 4294967295
    pos_reference_enable_height: int = 4294967295
    cip43_init_end_number: int = 4294967295
    sigma_fix_transition_number: int = 4294967295
    public_rpc_apis: str = "cfx,debug,test,pubsub,trace"


def generate_config_file(simulation_config: SimulateOptions, node_config: ConfluxOptions) -> TempFile:
    config_dict = _generate_config_dict(simulation_config, node_config)

    config_file = TempFile()

    for k in config_dict:
        config_file.writeline("{}={}".format(k, _normalize_config_value(config_dict[k])))

    return config_file


def _normalize_config_value(v: Any) -> str:
    if type(v) is str:
        if v.startswith("'") and v.endswith("'"):
            v = v[1:-1]
        elif v.startswith('"') and v.endswith('"'):
            v = v[1:-1]
        elif v == "true":
            v = True
        elif v == "false":
            v = False

    if type(v) is bool:
        return str(v).lower()
    elif type(v) is str:
        return f'"{v}"'
    elif type(v) is int:
        return str(v)
    else:
        raise Exception(f"Unrecongnized config type {type(v)} {v}")
    

def _generate_config_dict(simulation_config: SimulateOptions, node_config: ConfluxOptions) -> Dict[str, str]:
    # num_nodes is set to nodes_per_host because setup_chain() generates configs
    # for each node on the same host with different port number.

    # self.num_nodes = self.options.nodes_per_host
    # self.enable_tx_propagation = self.options.enable_tx_propagation
    # self.ips = []

    config_dict = {
        "tcp_port": p2p_port(0),
        "jsonrpc_local_http_port": rpc_port(0),
        "jsonrpc_ws_port": pubsub_port(0),
        "jsonrpc_http_port": remote_rpc_port(0),
        "jsonrpc_http_eth_port": evm_rpc_port(0),
        "jsonrpc_ws_eth_port": evm_rpc_ws_port(0),
        # "pos_config_path": "\'{}\'".format(os.path.join(datadir, "validator_full_node.yaml")),
        # "pos_initial_nodes_path": "\'{}\'".format(os.path.join(dirname, "initial_nodes.json")),
        # "pos_private_key_path": "'{}'".format(os.path.join(datadir, "blockchain_data", "net_config", "pos_key"))
    }
    config_dict.update(conflux.config.small_local_test_conf)
    config_dict.update(_enact_node_config(simulation_config, node_config))

    return config_dict

def _enact_node_config(simulation_config: SimulateOptions, node_config: ConfluxOptions) -> Dict[str, str]:
    conf_parameters = asdict(node_config)

    # Default Conflux memory consumption
    target_memory = 16
    # Overwrite with scaled configs so that Conflux consumes storage_memory_gb rather than target_memory.
    for k in ["db_cache_size", "ledger_cache_size",
              "storage_delta_mpts_cache_size", 
              "storage_delta_mpts_cache_start_size",
              "storage_delta_mpts_slab_idle_size"]:
        conf_parameters[k] = conflux.config.production_conf[k] // target_memory * simulation_config.storage_memory_gb
    conf_parameters["tx_pool_size"] = node_config.tx_pool_size // target_memory * simulation_config.storage_memory_gb

    # Do not keep track of tx index to save CPU/Disk costs because they are not used in the experiments
    conf_parameters["persist_tx_index"] = False

    # if simulation_config.enable_tx_propagation:
    conf_parameters["generate_tx"] = True
    conf_parameters["generate_tx_period_us"] = 1000000 * simulation_config.target_nodes // simulation_config.target_tps
    # Does not support case "enable_tx_propagation = False"
    # else:
    #     # one year to disable txs propagation
    #     conf_parameters["send_tx_period_ms"] = 31536000000
    #     del conf_parameters["genesis_secrets"]
    
    # FIXME: Double check if disabling this improves performance.
    conf_parameters["enable_optimistic_execution"] = False

    return conf_parameters

# class ConfigBuilder:
#     def set_test_params(self):
#         self.rpc_timewait = 600
#         # Have to have a num_nodes due to assert in base class.
#         self.num_nodes = None


