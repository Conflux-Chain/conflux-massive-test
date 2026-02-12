from typing import List

from tencentcloud.cvm.v20170312 import models as cvm_models
from tencentcloud.cvm.v20170312.cvm_client import CvmClient


def get_zone_ids_in_region(client: CvmClient) -> List[str]:
    req = cvm_models.DescribeZonesRequest()
    resp = client.DescribeZones(req)
    if not resp.ZoneSet:
        return []
    return [zone.Zone for zone in resp.ZoneSet if zone and zone.Zone]
