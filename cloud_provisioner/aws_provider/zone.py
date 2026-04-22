# pyright: reportTypedDictNotRequiredAccess=false

from typing import List

import boto3
from mypy_boto3_ec2.client import EC2Client


def get_zone_ids_in_region(client: EC2Client) -> List[str]:
    response = client.describe_availability_zones()
    return [
        zone['ZoneName']
        for zone in response['AvailabilityZones']
        if zone.get('ZoneName') and zone.get('State') == 'available'
    ]