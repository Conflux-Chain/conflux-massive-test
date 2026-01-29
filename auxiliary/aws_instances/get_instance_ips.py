
# Original: ip.sh

from typing import List, Literal
import boto3
import subprocess
from loguru import logger
from concurrent.futures import ThreadPoolExecutor


def get_ec2_instance_ips(
    instance_ids: List[str],
    ip_type: Literal["private", "public"] = "private",
    aws_region: str = "us-west-2"
) -> List[str]:
    """
    从 AWS EC2 获取实例 IP 地址
    
    参数:
        instance_ids: EC2 实例 ID 列表
        ip_type: IP 地址类型，"private" 获取私有 IP，"public" 获取公共 IP
        aws_region: AWS 区域
    
    返回:
        去重后的 IP 地址列表
    """
    # 创建 EC2 客户端
    ec2_client = boto3.client('ec2', region_name=aws_region)
    
    # 查询实例信息
    response = ec2_client.describe_instances(InstanceIds=instance_ids)
    
    # 提取 IP 地址
    ips: List[str] = []
    ip_field: str = "PrivateIpAddress" if ip_type == "private" else "PublicIpAddress"
    
    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            ip = instance.get(ip_field)
            if ip:
                ips.append(ip)
    
    # 去重并保持顺序
    seen = set()
    unique_ips: List[str] = []
    for ip in ips:
        if ip not in seen:
            seen.add(ip)
            unique_ips.append(ip)
    
    return unique_ips


def test_ssh_connections(
    ips: List[str],
    ssh_user: str = "ubuntu"
) -> None:
    """
    并行测试 SSH 连接（无论是否成功）
    
    参数:
        ips: IP 地址列表
        ssh_user: SSH 用户名
    """
    def test_single_connection(ip: str) -> None:
        """测试单个 SSH 连接"""
        subprocess.run(
            [
                "ssh",
                "-o", "StrictHostKeyChecking=no",
                f"{ssh_user}@{ip}",
                "exit"
            ],
            check=False,
            capture_output=True
        )
    
    # 并行执行 SSH 测试
    with ThreadPoolExecutor() as executor:
        list(executor.map(test_single_connection, ips))
    
    logger.info(f"GET {len(ips)} IPs")


# 使用示例
if __name__ == "__main__":
    # 直接传入实例 ID 列表
    instance_ids = ["i-076aadde942d663b2"]
    
    # 获取 IP 地址
    ips = get_ec2_instance_ips(
        instance_ids=instance_ids,
        ip_type="public",  # 或 "public"
        aws_region="us-west-2"
    )
    
    # 测试 SSH 连接
    test_ssh_connections(ips=ips, ssh_user="ubuntu")
    
    # ips 就是结果，可以直接使用
    logger.info(ips)