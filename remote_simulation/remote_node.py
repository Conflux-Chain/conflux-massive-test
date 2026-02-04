from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import random
import time
from typing import Any, Callable, List, Optional, Tuple, TypeVar

import eth_utils
from loguru import logger

from ali_instances.host_spec import HostSpec
from conflux.utils import convert_to_nodeid, encode_int32, int_to_bytes, sha3

from jsonrpcclient.clients.http_client import HTTPClient
from jsonrpcclient.requests import Request
from jsonrpcclient.response import JSONRPCResponse, Response
from jsonrpcclient.exceptions import ReceivedErrorResponseError

from remote_simulation.port_allocation import p2p_port, remote_rpc_port

@dataclass
class RemoteNode:
    host_spec: HostSpec
    index: int
    key: str = None

    def __hash__(self):
        # 返回基于不可变属性的哈希值
        return hash((self.host_spec.ip, self.index))
    

    @property
    def rpc(self) -> 'RemoteNodeRPC':
        port = remote_rpc_port(self.index)
        client = HTTPClient(f"http://{self.host_spec.ip}:{port}")
        return RemoteNodeRPC(host=self.host_spec.ip, port = port, client=client)
    
    @property
    def id(self) -> str:
        return f"{self.host_spec.ip}-{self.index}"
    
    @property
    def p2p_addr(self) -> str:
        port = p2p_port(self.index)
        return f"{self.host_spec.ip}:{port}"
    
    def wait_for_ready(self):
        try:
            self._wait_for_node_id()
            self._wait_for_phase(["NormalSyncPhase"])
            return True
        except Exception as e:
            logger.debug(f"Fail to check node ready for {self.id}, error: {e}")
            return False
        
    def init_tx_gen(self, start_index: int):
        try:
            self.rpc.test_sendUsableGenesisAccounts(start_index)
            return True
        except Exception as e:
            logger.debug(f"Fail to init tx for {self.id}, error: {e}")
            return False


    def _wait_for_node_id(self):
        pubkey, x, y = self._get_node_id()
        self.key = eth_utils.encode_hex(pubkey)
        addr_tmp = bytearray(sha3(encode_int32(x) + encode_int32(y))[12:])
        addr_tmp[0] &= 0x0f
        addr_tmp[0] |= 0x10
        self.addr = addr_tmp
        logger.debug(f"Get nodeid {self.key} for instance {self.host_spec.ip} node {self.index}")

    def _get_node_id(self):
        challenge = random.randint(0, 2**32-1)
        signature = self.rpc.test_getNodeId(int_to_bytes(challenge))
        return convert_to_nodeid(signature, challenge)
    
    def _wait_for_phase(self, phases, wait_time=10):
        sleep_time = 0.1
        retry = 0
        max_retry = wait_time / sleep_time

        while self.rpc.debug_currentSyncPhase() not in phases and retry <= max_retry:
            time.sleep(0.1)
            retry += 1

        if retry > max_retry:
            current_phase = self.rpc.debug_currentSyncPhase()
            raise AssertionError(f"Node did not reach any of {phases} after {wait_time} seconds, current phase is {current_phase}")

T = TypeVar('T')
def for_all_nodes(nodes: List[RemoteNode], execute: Callable[[RemoteNode], T], max_workers: int = 300) -> List[Tuple[str, T]]:
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(lambda node: (node.id, execute(node)), nodes)
        return list(results)


@dataclass
class RemoteNodeRPC:
    host: str
    port: int
    client: HTTPClient
    timeout: int = 60

    def _call(self, method, *args):
        request = Request(method, *args)
        response: Response = self.client.send(request, timeout=self.timeout)
        return response.data.result
    
    @property
    def addr(self):
        return f"{self.host}:{self.port}"

    def debug_currentSyncPhase(self):
        return self._call("debug_currentSyncPhase")

    def test_getNodeId(self, challenge: bytes):
        return self._call("test_getNodeId", list(challenge))

    def test_addNode(self, key: str, peer_addr: str): 
        return self._call("test_addNode", key, peer_addr)

    def test_getPeerInfo(self) -> List[Any]: 
        return self._call("test_getPeerInfo")

    def test_addLatency(self, peer_key: str, latency: int = 0):
        return self._call("test_addLatency", peer_key, latency)
    
    def test_getBlockCount(self):
        return self._call("test_getBlockCount")
    
    def test_sendUsableGenesisAccounts(self, start_index: int):
        return self._call("test_sendUsableGenesisAccounts", start_index)
    
    def test_generateOneBlock(self, num_txs:int, block_size_limit_bytes:int):
        return self._call("test_generateOneBlock", num_txs, block_size_limit_bytes)
    
    def test_getGoodPut(self):
        return self._call("test_getGoodPut")
    
    def cfx_getBestBlockHash(self):
        return self._call("cfx_getBestBlockHash")