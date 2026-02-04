import base64
import hashlib
from typing import List, Optional

from tencentcloud.cvm.v20170312 import models as cvm_models
from tencentcloud.cvm.v20170312.cvm_client import CvmClient

from cloud_provisioner.create_instances.types import KeyPairInfo, KeyPairRequestConfig
from utils.wait_until import wait_until


def _fingerprint_from_public_key(public_key: str) -> str:
    parts = public_key.strip().split()
    if len(parts) < 2:
        return ""
    key_data = base64.b64decode(parts[1])
    fingerprint = hashlib.md5(key_data).hexdigest()
    return ":".join(fingerprint[i:i+2] for i in range(0, len(fingerprint), 2))


def as_key_pair_info(rep: cvm_models.KeyPair) -> KeyPairInfo:
    assert isinstance(rep.PublicKey, str)
    return KeyPairInfo(finger_print=_fingerprint_from_public_key(rep.PublicKey))


def get_keypairs_in_region(client: CvmClient, key_pair_name: str) -> Optional[KeyPairInfo]:
    results: List[KeyPairInfo] = []
    offset = 0
    limit = 100

    filter_name = cvm_models.Filter()
    filter_name.Name = "key-name"
    filter_name.Values = [key_pair_name]

    while True:
        req = cvm_models.DescribeKeyPairsRequest()
        req.Filters = [filter_name]
        req.Offset = offset
        req.Limit = limit

        resp = client.DescribeKeyPairs(req)
        if resp.KeyPairSet:
            results.extend([as_key_pair_info(kp) for kp in resp.KeyPairSet])

        if resp.TotalCount is None or resp.TotalCount <= offset + limit:
            break
        offset += limit

    if len(results) == 0:
        return None
    if len(results) == 1:
        return results[0]
    raise Exception(f"Unexpected: multiple result for key pair {key_pair_name}")


def create_keypair(client: CvmClient, key_pair: KeyPairRequestConfig):
    req = cvm_models.ImportKeyPairRequest()
    req.KeyName = key_pair.key_pair_name
    req.ProjectId = 0
    req.PublicKey = key_pair.public_key
    client.ImportKeyPair(req)

    def _available():
        remote_key_pair = get_keypairs_in_region(client, key_pair.key_pair_name)
        return remote_key_pair is not None and remote_key_pair.finger_print == key_pair.finger_print("tencent")

    wait_until(_available, timeout=10, retry_interval=3)
