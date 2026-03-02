# pyright: reportTypedDictNotRequiredAccess=false
from typing import List

from ..create_instances.types import VpcInfo
from utils.wait_until import wait_until

from mypy_boto3_ec2.client import EC2Client
from mypy_boto3_ec2.type_defs import VpcTypeDef


def as_vpc_info(vpc_dict: VpcTypeDef):
    vpc_id = vpc_dict['VpcId'] 
    
    vpc_name = ''
    if 'Tags' in vpc_dict:
        for tag in vpc_dict['Tags']:
            if tag['Key'] == 'Name':
                vpc_name = tag['Value']
                break
    
    state = vpc_dict['State']
    
    assert type(vpc_id) is str
    assert type(vpc_name) is str
    assert type(state) is str
    
    return VpcInfo(vpc_id=vpc_id, vpc_name=vpc_name)

def get_vpcs_in_region(client: EC2Client) -> List[VpcInfo]:
    result = []
    
    next_token = None
    while True:
        if next_token:
            response = client.describe_vpcs(MaxResults=1000, NextToken=next_token)
        else:
            response = client.describe_vpcs(MaxResults=1000)
        
        result.extend([as_vpc_info(vpc) for vpc in response['Vpcs']])
        
        next_token = response.get('NextToken')
        if not next_token:
            break
    
    return result

def create_vpc(client: EC2Client, vpc_name: str, cidr_block: str):
    response = client.create_vpc(CidrBlock=cidr_block)
    vpc_id = response['Vpc']['VpcId']
    
    assert type(vpc_id) is str
    
    client.create_tags(Resources=[vpc_id], Tags=[{'Key': 'Name', 'Value': vpc_name}])
    
    def _available() -> bool:
        resp = client.describe_vpcs(VpcIds=[vpc_id])
        vpcs = resp['Vpcs']
        return len(vpcs) > 0 and vpcs[0]['State'] == 'available'
    
    wait_until(_available, timeout=120, retry_interval=3)
    
    # 配置互联网访问
    igw_response = client.create_internet_gateway()
    igw_id = igw_response['InternetGateway']['InternetGatewayId']
    client.create_tags(Resources=[igw_id], Tags=[{'Key': 'Name', 'Value': f'{vpc_name}-igw'}])
    client.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
    
    # 配置主路由表
    route_tables = client.describe_route_tables(Filters=[
        {'Name': 'vpc-id', 'Values': [vpc_id]},
        {'Name': 'association.main', 'Values': ['true']}
    ])
    main_route_table_id = route_tables['RouteTables'][0]['RouteTableId']
    client.create_route(
        RouteTableId=main_route_table_id,
        DestinationCidrBlock='0.0.0.0/0',
        GatewayId=igw_id
    )
    
    
    return vpc_id


from loguru import logger


def delete_vpc(client: EC2Client, vpc_id: str):
    logger.info(f"Starting dependency cleanup for VPC {vpc_id}")

    # Detach and delete internet gateways
    logger.debug("Listing internet gateways attached to VPC")
    igw_resp = client.describe_internet_gateways(Filters=[{'Name': 'attachment.vpc-id', 'Values': [vpc_id]}])
    for igw in igw_resp.get('InternetGateways', []):
        igw_id = igw['InternetGatewayId']
        logger.info(f"Detaching IGW {igw_id} from VPC {vpc_id}")
        client.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
        logger.info(f"Deleting IGW {igw_id}")
        client.delete_internet_gateway(InternetGatewayId=igw_id)

    # Delete subnets
    logger.debug("Listing subnets in VPC")
    response = client.describe_subnets(Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}], MaxResults=1000)
    for subnet in response.get('Subnets', []):
        subnet_id = subnet['SubnetId']
        logger.info(f"Deleting subnet {subnet_id} in VPC {vpc_id}")
        client.delete_subnet(SubnetId=subnet_id)

    # Disassociate and delete route tables
    logger.debug("Listing route tables in VPC")
    rts = client.describe_route_tables(Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}])
    for rt in rts.get('RouteTables', []):
        rt_id = rt['RouteTableId']
        logger.debug(f"Processing route table {rt_id}")
        for assoc in rt.get('Associations', []):
            if not assoc.get('Main'):
                assoc_id = assoc['RouteTableAssociationId']
                logger.info(f"Disassociating route table association {assoc_id}")
                client.disassociate_route_table(AssociationId=assoc_id)
        logger.info(f"Deleting route table {rt_id}")
        client.delete_route_table(RouteTableId=rt_id)

    # Delete network interfaces
    logger.debug("Listing network interfaces in VPC")
    nis = client.describe_network_interfaces(Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}])
    for ni in nis.get('NetworkInterfaces', []):
        ni_id = ni['NetworkInterfaceId']
        logger.info(f"Detaching and deleting network interface {ni_id}")
        if 'Attachment' in ni and 'AttachmentId' in ni['Attachment']:
            client.detach_network_interface(AttachmentId=ni['Attachment']['AttachmentId'], Force=True)
        client.delete_network_interface(NetworkInterfaceId=ni_id)

    # Delete security groups (skip default)
    logger.debug("Listing security groups in VPC")
    sgs = client.describe_security_groups(Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}])
    for sg in sgs.get('SecurityGroups', []):
        if sg.get('GroupName') != 'default':
            sg_id = sg['GroupId']
            logger.info(f"Deleting security group {sg_id}")
            client.delete_security_group(GroupId=sg_id)

    # Final attempt to delete VPC
    logger.info(f"Deleting VPC {vpc_id}")
    client.delete_vpc(VpcId=vpc_id)

