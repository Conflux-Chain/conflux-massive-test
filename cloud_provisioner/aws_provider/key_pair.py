# pyright: reportTypedDictNotRequiredAccess=false

from typing import Optional
from botocore.exceptions import ClientError

from cloud_provisioner.create_instances.types import KeyPairInfo, KeyPairRequestConfig
from utils.wait_until import wait_until

from mypy_boto3_ec2.client import EC2Client
from mypy_boto3_ec2.type_defs import KeyPairInfoTypeDef

    
def as_key_pair_info(rep: KeyPairInfoTypeDef):
    assert type(rep['KeyFingerprint']) is str
    return KeyPairInfo(finger_print=rep['KeyFingerprint'])
    

def get_keypairs_in_region(client: EC2Client, region_id: str, key_pair_name: str) -> Optional[KeyPairInfo]:
    try:
        response = client.describe_key_pairs(KeyNames=[key_pair_name])
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidKeyPair.NotFound':
            return None
        raise
    
    result = [as_key_pair_info(kp) for kp in response['KeyPairs']]
    
    if len(result) == 0:
        return None
    elif len(result) == 1:
        return result[0]
    else:
        raise Exception(f"Unexpected: multiple result for key pair {key_pair_name} in {region_id}")



def create_keypair(client: EC2Client, region_id: str, key_pair: KeyPairRequestConfig):
    public_key_bytes = key_pair.public_key.encode('utf-8')
    
    client.import_key_pair(
        KeyName=key_pair.key_pair_name,
        PublicKeyMaterial=public_key_bytes
    )
    
    def _available():
        remote_key_pair = get_keypairs_in_region(client, region_id, key_pair.key_pair_name)
        return remote_key_pair is not None and remote_key_pair.finger_print == key_pair.finger_print("aws")
    
    wait_until(_available, timeout=10, retry_interval=3)