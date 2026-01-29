import subprocess
import json
import time
import os
from typing import Optional
import boto3
from loguru import logger


def create_slave_image(
    image_prefix: str,
    master_ip: str,
    master_instance_id: str,
    setup_script_path: Optional[str] = None,
    branch: str = "master",
    repo: str = "https://github.com/Conflux-Chain/conflux-rust",
    aws_region: str = "us-west-2",
    poll_interval: int = 5
) -> str:
    """
    从主实例创建 slave AMI 镜像。
    
    该函数执行以下操作：
    1. 将设置脚本复制到主实例
    2. 在远程实例上执行设置脚本
    3. 从实例创建 AMI 镜像
    4. 等待镜像创建完成
    
    Args:
        image_prefix: 镜像命名前缀
        master_ip: 主实例的 IP 地址
        master_instance_id: 主实例的 EC2 实例 ID
        setup_script_path: 要复制和执行的设置脚本路径
        branch: 要使用的 Git 分支（默认: "master"）
        repo: Git 仓库 URL（默认: "https://github.com/Conflux-Chain/conflux-rust"）
        aws_region: AWS 区域（默认: "us-west-2"）
        poll_interval: 镜像状态检查间隔秒数（默认: 5）
    
    Returns:
        创建的 AMI 镜像 ID
        
    Raises:
        subprocess.CalledProcessError: 如果任何命令执行失败
        json.JSONDecodeError: 如果无法解析 AWS CLI 输出
    """
    ec2_client = boto3.client('ec2', region_name=aws_region)
    
    logger.info("setup before making slave image ...")
    
    # 将设置脚本复制到远程实例
    _initialize_remote_image(setup_script_path, master_ip, branch, repo)
    
    # 创建 slave 镜像
    logger.info("create slave image ...")
    image_name = f'{image_prefix}_slave_image'
    response = ec2_client.create_image(
        InstanceId=master_instance_id,
        Name=image_name
    )
    image_id = response['ImageId']
    logger.info(f"slave image created: {image_id}")
    
    # 等待镜像变为可用状态
    while not _check_image_ready(ec2_client, image_id):
        time.sleep(poll_interval)
    
    return image_id

def _check_image_ready(ec2_client, image_id: str):
    response = ec2_client.describe_images(ImageIds=[image_id])
        
    if not response['Images']:
        raise RuntimeError(f"Image {image_id} not found")
    
    image_status = response['Images'][0]['State']
    logger.info(f"image is {image_status}")
    
    return image_status != "pending"

def _initialize_remote_image(setup_script_path: Optional[str], master_ip: str, branch: str, repo: str):
    if setup_script_path is None:
        return
    
    setup_script_name = os.path.basename(setup_script_path)
    scp_cmd = [
        'scp',
        '-o', 'StrictHostKeyChecking=no',
        setup_script_path,
        f'ubuntu@{master_ip}:~'
    ]
    subprocess.run(scp_cmd, check=True)
    
    # 在远程实例上执行设置脚本
    # -tt 标志确保实时行缓冲输出
    ssh_cmd = [
        'ssh',
        '-tt',
        f'ubuntu@{master_ip}',
        f'./{setup_script_name}',
        branch,
        repo
    ]
    subprocess.run(ssh_cmd, check=True)

def deregister_image_and_snapshots(image_id: str, aws_region: str = "us-west-2"):
    """
    注销 AMI 镜像并删除关联的快照
    
    参数:
        image_id: AMI 镜像 ID
        aws_region: AWS 区域
    """
    ec2_client = boto3.client('ec2', region_name=aws_region)
    
    # 获取镜像信息
    logger.info(f"获取镜像 {image_id} 的信息...")
    try:
        response = ec2_client.describe_images(ImageIds=[image_id])
    except Exception as e:
        logger.error(f"获取镜像信息失败: {e}")
        return
    
    if not response['Images']:
        logger.warning(f"未找到镜像 {image_id}")
        return
    
    # 提取快照 ID
    snapshot_ids = []
    for mapping in response['Images'][0].get('BlockDeviceMappings', []):
        if 'Ebs' in mapping:
            snapshot_id = mapping['Ebs'].get('SnapshotId')
            if snapshot_id:
                snapshot_ids.append(snapshot_id)
    
    # 注销镜像
    logger.info(f"注销镜像 {image_id}")
    ec2_client.deregister_image(ImageId=image_id)
    
    # 删除快照
    for snapshot_id in snapshot_ids:
        logger.info(f"删除快照 {snapshot_id}")
        try:
            ec2_client.delete_snapshot(SnapshotId=snapshot_id)
        except Exception as e:
            logger.error(f"删除快照 {snapshot_id} 失败: {e}")
    
    logger.info(f"已注销镜像 {image_id} 并删除 {len(snapshot_ids)} 个快照")

if __name__ == "__main__":
    image_id = create_slave_image("massive_test", "52.34.236.74", "i-076aadde942d663b2")