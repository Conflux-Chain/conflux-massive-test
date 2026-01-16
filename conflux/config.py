from dataclasses import dataclass
from typing import Optional

DEFAULT_PY_TEST_CHAIN_ID = 10
DEFAULT_RPC_PORT = 12537
DEFAULT_WS_PORT = 12538
DEFAULT_EVM_RPC_PORT = 12539
DEFAULT_EVM_WS_PORT = 12540
DEFAULT_CHAIN_ID = 1024
DEFAULT_EVM_CHAIN_ID = 1025
DEFAULT_CONFLUX_BIN = "/usr/local/bin/conflux"


@dataclass(frozen=True)
class ConfluxNodeConfig:
    rpc_port: int
    ws_port: int
    evm_rpc_port: int
    evm_ws_port: int
    chain_id: int
    evm_chain_id: int = DEFAULT_EVM_CHAIN_ID
    conflux_bin: str = DEFAULT_CONFLUX_BIN
    mining_author: Optional[str] = None
    remote_config_dir: str = "/opt/conflux/config"
    remote_data_dir: str = "/opt/conflux/data"
    remote_log_dir: str = "/opt/conflux/logs"
    remote_pos_config_dir: str = "/opt/conflux/pos_config"
    pos_config_path: str = "/opt/conflux/pos_config/pos_config.yaml"
    pos_initial_nodes_path: str = "/opt/conflux/pos_config/initial_nodes.json"
    pos_private_key_path: str = "/opt/conflux/pos_config/pos_key"

small_local_test_conf = dict(
    chain_id = DEFAULT_PY_TEST_CHAIN_ID,
    check_phase_change_period_ms = 100,
    enable_discovery = "false",
    log_file = "'./conflux.log'",
    log_level = '"debug"',
    metrics_output_file = "'./metrics.log'",
    metrics_enabled = "true",
    mode = '"test"',
    session_ip_limits = "'0,0,0,0'",
    mining_type = "'disable'",
    storage_delta_mpts_cache_size = 200_000,
    storage_delta_mpts_cache_start_size = 200_000,
    storage_delta_mpts_slab_idle_size = 2_000_000,
    subnet_quota = 0,
    persist_tx_index = "true",
    persist_block_number_index = "true",
    execute_genesis = "false",
    dev_allow_phase_change_without_peer = "true",
    check_status_genesis = "false",
    pos_reference_enable_height = 0,
    hydra_transition_height = 0,
    hydra_transition_number = 0,
    cip43_init_end_number = 2 ** 32 - 1,
    min_phase_change_normal_peer_count = 1,
    dao_vote_transition_number = 2**31,
    dao_vote_transition_height = 2**31,
    enable_single_mpt_storage = "true",
    rpc_enable_metrics = "true",
)

default_conflux_conf = dict(
    chain_id = DEFAULT_PY_TEST_CHAIN_ID,
    db_cache_size = 128,
    ledger_cache_size = 1024,
    storage_delta_mpts_cache_size = 20_000_000,
    storage_delta_mpts_cache_start_size = 2_000_000,
    storage_delta_mpts_slab_idle_size = 2_000_000,
    tx_pool_size = 500_000,
    persist_tx_index = "true",
    persist_block_number_index = "true",
)

production_conf = default_conflux_conf


def _format_config_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        inner = ", ".join(_format_config_value(item) for item in value)
        return f"[{inner}]"
    return str(value)


def build_single_node_conflux_config_text(node_config: ConfluxNodeConfig) -> str:
    config_dict: dict[str, object] = dict(small_local_test_conf)
    config_dict.update(
        {
            "node_type": '"archive"',
            "mode": '"dev"',
            "dev_block_interval_ms": 250,
            "dev_pos_private_key_encryption_password": '"CFXV20"',
            "chain_id": node_config.chain_id,
            "evm_chain_id": node_config.evm_chain_id,
            "enable_discovery": "false",
            "bootnodes": '""',
            "jsonrpc_http_host": '"0.0.0.0"',
            "jsonrpc_http_port": node_config.rpc_port,
            "jsonrpc_ws_host": '"0.0.0.0"',
            "jsonrpc_ws_port": node_config.ws_port,
            "jsonrpc_http_eth_host": '"0.0.0.0"',
            "jsonrpc_http_eth_port": node_config.evm_rpc_port,
            "jsonrpc_ws_eth_host": '"0.0.0.0"',
            "jsonrpc_ws_eth_port": node_config.evm_ws_port,
            "public_rpc_apis": '"all"',
            "public_evm_rpc_apis": '"all"',
            "conflux_data_dir": f'"{node_config.remote_data_dir}"',
            "pos_config_path": f'"{node_config.pos_config_path}"',
            "pos_initial_nodes_path": f'"{node_config.pos_initial_nodes_path}"',
            "pos_private_key_path": f'"{node_config.pos_private_key_path}"',
            "tanzanite_transition_height": 4,
            "default_transition_time": 1,
            "hydra_transition_number": 5,
            "hydra_transition_height": 5,
            "cip43_init_end_number": 5,
            "pos_reference_enable_height": 5,
            "dao_vote_transition_number": 6,
            "dao_vote_transition_height": 6,
            "sigma_fix_transition_number": 6,
            "cip107_transition_number": 7,
            "cip112_transition_height": 7,
            "cip118_transition_number": 7,
            "cip119_transition_number": 7,
            "base_fee_burn_transition_number": 10,
            "base_fee_burn_transition_height": 10,
            "c2_fix_transition_height": 11,
            "eoa_code_transition_height": 12,
            "db_cache_size": 128,
            "ledger_cache_size": 1024,
            "tx_pool_size": 500000,
            "start_mining": "true",
            "generate_tx": "true",
            "generate_tx_period_us": 100000,
            "txgen_account_count": 10,
        }
    )
    if node_config.mining_author:
        config_dict["mining_author"] = f'"{node_config.mining_author}"'
    lines = [f"{key} = {_format_config_value(value)}" for key, value in config_dict.items()]
    return "\n".join(lines)