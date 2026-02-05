# pyright: reportTypedDictNotRequiredAccess=false

import time
import traceback
from typing import List, Tuple

from botocore.exceptions import ClientError
from loguru import logger

from cloud_provisioner.cleanup_instances.types import InstanceInfoWithTag

from ..create_instances.types import CreateInstanceError, InstanceStatus, RegionInfo, ZoneInfo, InstanceType
from ..create_instances.instance_config import InstanceConfig, DEFAULT_COMMON_TAG_KEY, DEFAULT_COMMON_TAG_VALUE

from mypy_boto3_ec2.client import EC2Client


def _instance_tags(cfg: InstanceConfig) -> List[dict]:
    return [
        {'Key': DEFAULT_COMMON_TAG_KEY, 'Value': DEFAULT_COMMON_TAG_VALUE},
        {'Key': cfg.user_tag_key, 'Value': cfg.user_tag_value}
    ]
    
def as_instance_info_with_tag(instance):
    if instance.get('Tags'):
        tags = {tag['Key']: tag['Value'] for tag in instance['Tags']}
    else:
        tags = dict()
    
    instance_name = tags.get('Name', '')
    
    return InstanceInfoWithTag(
        instance_id=instance['InstanceId'], 
        instance_name=instance_name, 
        tags=tags
    )

def create_instances_in_zone(
    client: EC2Client,
    cfg: InstanceConfig,
    region_info: RegionInfo,
    zone_info: ZoneInfo,
    instance_type: InstanceType,
    max_amount: int,
    min_amount: int,
) -> Tuple[list[str], CreateInstanceError]:   
    name = f"{cfg.instance_name_prefix}-{int(time.time())}"
    
    tags = _instance_tags(cfg) + [{'Key': 'Name', 'Value': name}]
    
    try:
        response = client.run_instances(
            ImageId=region_info.image_id,
            MinCount=min_amount,
            MaxCount=max_amount,
            KeyName=region_info.key_pair_name,
            InstanceType=instance_type.name, # pyright: ignore[reportArgumentType]
            NetworkInterfaces=[{
                'AssociatePublicIpAddress': True,
                'DeviceIndex': 0,
                'SubnetId': zone_info.v_switch_id,
                'Groups': [region_info.security_group_id]
            }],
            Placement={'AvailabilityZone': zone_info.id},
            BlockDeviceMappings=[{
                'DeviceName': '/dev/sda1',
                'Ebs': {
                    'VolumeSize': cfg.disk_size,
                    'VolumeType': 'gp3',
                    'Iops': 3000,
                    'Throughput': 300,
                    'DeleteOnTermination': True
                }
            }],
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': tags
            }] # pyright: ignore[reportArgumentType]
        )
        ids = [instance['InstanceId'] for instance in response['Instances']]
        assert ids is not None
        logger.success(f"Create instances at {region_info.id}/{zone_info.id}: instance_type={instance_type.name}, amount={len(ids)}, ids={ids}, request={min_amount}~{max_amount}")
        return ids, CreateInstanceError.Nil
    except ClientError as exc:
        code = exc.response['Error']['Code']
        error_type = CreateInstanceError.Others
        
        if code == "InsufficientInstanceCapacity":
            logger.warning(f"No stock for {region_info.id}/{zone_info.id}, instance_type={instance_type.name}, amount={min_amount}~{max_amount}")
            error_type = CreateInstanceError.NoStock
            
        elif code == "Unsupported":
            logger.warning(f"Unsupported configuration for {region_info.id}/{zone_info.id}, instance_type={instance_type.name}, amount={min_amount}~{max_amount}")
            error_type = CreateInstanceError.NoInstanceType
            
        else:
            logger.error(f"run_instances failed for {region_info.id}/{zone_info.id}: {exc}")
            logger.error(f"{exc.__dict__}")
            logger.error(traceback.format_exc())
            
        return [], error_type
    except Exception as exc:
        logger.error(f"run_instances failed for {region_info.id}/{zone_info.id}: {exc}")
        logger.error(traceback.format_exc())
        return [], CreateInstanceError.Others
    
def describe_instance_status(client: EC2Client, instance_ids: List[str]):
    running_instances = dict()
    pending_instances = set()
    
    for i in range(0, len(instance_ids), 1000):
        query_chunk = instance_ids[i: i+1000]
        
        response = client.describe_instances(InstanceIds=query_chunk)
        
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                instance_id = instance['InstanceId']
                status = instance['State']['Name']
                
                if status == 'running':
                    running_instances[instance_id] = instance['PublicIpAddress']
                elif status in ['pending', 'stopped']:
                    pending_instances.add(instance_id)
        
        time.sleep(0.5)
    
    return InstanceStatus(running_instances=running_instances, pending_instances=pending_instances)

def get_instances_with_tag(client: EC2Client) -> List[InstanceInfoWithTag]:
    instances = []
    next_token = None
    
    while True:
        params = {}
        if next_token:
            params['NextToken'] = next_token
        
        response = client.describe_instances(**params)
        
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                # AWS API 会返回已经销毁的实例，状态为 terminated，应该被忽略
                if instance['State']['Name'] == 'terminated':
                    continue
                
                instances.append(as_instance_info_with_tag(instance))
        
        next_token = response.get('NextToken')
        if not next_token:
            break
    
    return instances


def delete_instances(client, instances_ids: List[str]):
    for i in range(0, len(instances_ids), 1000):
        chunks = instances_ids[i:i+1000]
        while True:
            try:
                client.terminate_instances(InstanceIds=chunks)
                break
            except Exception as e:
                logger.error(f"Cannot delete: {e.__dict__}")
                # 下面的逻辑移植自阿里云，但 aws 好像没有这个问题：禁止删除 initialize 阶段的 instance
                # response = getattr(e, 'response', {})
                # error_code = response.get('Error', {}).get('Code')
                # if error_code == 'IncorrectInstanceState':
                #     logger.warning(f"Some instances in region {region_id} is still initializing, waiting_retry")
            time.sleep(5)