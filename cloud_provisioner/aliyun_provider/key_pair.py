# pyright: reportOptionalOperand=false

from typing import List, Optional
from alibabacloud_ecs20140526.models import DescribeKeyPairsResponseBodyKeyPairsKeyPair, DescribeKeyPairsRequest, ImportKeyPairRequest
from alibabacloud_ecs20140526.client import Client

from cloud_provisioner.create_instances.types import KeyPairInfo, KeyPairRequestConfig
from utils.wait_until import wait_until

    
def as_key_pair_info(rep: DescribeKeyPairsResponseBodyKeyPairsKeyPair):
    assert type(rep.key_pair_finger_print) is str
    return KeyPairInfo(finger_print=rep.key_pair_finger_print)
    

def get_keypairs_in_region(client: Client, region_id: str, key_pair_name: str) -> Optional[KeyPairInfo]:
    result = []
    
    page_number = 1
    while True:
        rep = client.describe_key_pairs(DescribeKeyPairsRequest(region_id=region_id, key_pair_name=key_pair_name, page_number=page_number, page_size=50))
        result.extend([as_key_pair_info(v_switch) for v_switch in rep.body.key_pairs.key_pair])
        if rep.body.total_count <= page_number * 50:
            break
        page_number += 1
        
    if len(result) == 0:
        return None
    elif len(result) == 1:
        return result[0]
    else:
        raise Exception(f"Unexpected: multiple result for key pair {key_pair_name} in {region_id}")

def create_keypair(client: Client, region_id: str, key_pair: KeyPairRequestConfig):    
    client.import_key_pair(ImportKeyPairRequest(region_id=region_id, key_pair_name=key_pair.key_pair_name, public_key_body=key_pair.public_key))
    
    def _available():
        remote_key_pair = get_keypairs_in_region(client, region_id, key_pair.key_pair_name)
        return remote_key_pair is not None and remote_key_pair.finger_print == key_pair.finger_print("aliyun")
    
    wait_until(_available, timeout=10, retry_interval=3)
