from dataclasses import dataclass
from optparse import Option
from typing import List, Optional
from alibabacloud_ecs20140526.models import DescribeKeyPairsResponseBodyKeyPairsKeyPair, DescribeKeyPairsRequest, ImportKeyPairRequest
from alibabacloud_ecs20140526.client import Client as EcsClient

from ali_instances_v2.client_factory import ClientFactory
from utils.wait_until import wait_until

from .crypto import get_fingerprint_from_key, get_public_key_body


@dataclass
class KeyPairInfo:
    finger_print: str
    
    @classmethod
    def from_api_response(cls, rep: DescribeKeyPairsResponseBodyKeyPairsKeyPair):
        assert type(rep.key_pair_finger_print) is str
        return KeyPairInfo(finger_print=rep.key_pair_finger_print)
    
@dataclass
class KeyPairRequestConfig:
    key_path: str
    key_pair_name: str
    
    @property
    def finger_print(self):
        return get_fingerprint_from_key(self.key_path, "md5")
        
    @property
    def public_key(self):
        return get_public_key_body(self.key_path)
    
    


def get_keypairs_in_region(c: ClientFactory, region_id: str, key_pair_name: str) -> Optional[KeyPairInfo]:
    client = c.build(region_id)

    result = []
    
    page_number = 1
    while True:
        rep = client.describe_key_pairs(DescribeKeyPairsRequest(region_id=region_id, key_pair_name=key_pair_name, page_number=page_number, page_size=50))
        result.extend([KeyPairInfo.from_api_response(v_switch) for v_switch in rep.body.key_pairs.key_pair])
        if rep.body.total_count <= page_number * 50:
            break
        page_number += 1
        
    if len(result) == 0:
        return None
    elif len(result) == 1:
        return result[0]
    else:
        raise Exception(f"Unexpected: multiple result for key pair {key_pair_name} in {region_id}")

def create_keypair(c: ClientFactory, region_id: str, key_pair: KeyPairRequestConfig):
    client = c.build(region_id)
    
    client.import_key_pair(ImportKeyPairRequest(region_id=region_id, key_pair_name=key_pair.key_pair_name, public_key_body=key_pair.public_key))
    
    def _available():
        remote_key_pair = get_keypairs_in_region(c, region_id, key_pair.key_pair_name)
        return remote_key_pair is not None and remote_key_pair.finger_print == key_pair.finger_print
    
    wait_until(_available, timeout=10, retry_interval=3)
