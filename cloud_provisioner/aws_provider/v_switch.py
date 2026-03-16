# pyright: reportTypedDictNotRequiredAccess=false

from typing import List
from botocore.exceptions import ClientError
from ..create_instances.types import VSwitchInfo
from ..create_instances.types import ZoneUnavailableError
from .zone_unavailable import is_zone_unavailable_error
from utils.wait_until import wait_until

from mypy_boto3_ec2.client import EC2Client
from mypy_boto3_ec2.type_defs import SubnetTypeDef



def as_vswitch_info(subnet: SubnetTypeDef):
    subnet_id = subnet['SubnetId']
    zone_id = subnet['AvailabilityZone']
    status = subnet['State']
    cidr_block = subnet['CidrBlock']
    
    v_switch_name = ''
    if 'Tags' in subnet:
        for tag in subnet['Tags']:
            if tag['Key'] == 'Name':
                v_switch_name = tag['Value']
                break
    
    assert type(subnet_id) is str
    assert type(v_switch_name) is str
    assert type(zone_id) is str
    assert type(status) is str
    assert type(cidr_block) is str
    
    return VSwitchInfo(
        v_switch_id=subnet_id,
        v_switch_name=v_switch_name,
        zone_id=zone_id,
        status=status,
        cidr_block=cidr_block
    )


def get_v_switchs_in_region(client: EC2Client, vpc_id: str) -> List[VSwitchInfo]:
    result = []
    
    next_token = None
    while True:
        kwargs = dict()
        if next_token:
            kwargs['NextToken'] = next_token
            
        response = client.describe_subnets(Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}], MaxResults=1000, **kwargs)
        result.extend([as_vswitch_info(subnet) for subnet in response['Subnets']])
        
        next_token = response.get('NextToken')
        if not next_token:
            break
    
    return result


def create_v_switch(client: EC2Client, zone_id: str, vpc_id: str, v_switch_name: str, cidr_block: str):
    try:
        response = client.create_subnet(
            VpcId=vpc_id,
            CidrBlock=cidr_block,
            AvailabilityZone=zone_id,
            TagSpecifications=[
                {
                    'ResourceType': 'subnet',
                    'Tags': [
                        {'Key': 'Name', 'Value': v_switch_name}
                    ]
                }
            ]
        )
    except ClientError as exc:
        if is_zone_unavailable_error(exc):
            raise ZoneUnavailableError(zone_id, str(exc)) from exc
        raise
    
    subnet_id = response['Subnet']['SubnetId']
    
    assert type(subnet_id) is str
    
    def _available():
        resp = client.describe_subnets(SubnetIds=[subnet_id])
        subnets = resp['Subnets']
        return len(subnets) > 0 and subnets[0]['State'] == 'available'
    
    wait_until(_available, timeout=120, retry_interval=3)
    
    return subnet_id