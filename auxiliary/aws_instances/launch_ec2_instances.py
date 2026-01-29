
# Original file: launch-on-demand.sh

import argparse
import os

from dotenv import load_dotenv

import traceback
from typing import List, Optional, Literal
from dataclasses import dataclass
import boto3
import subprocess
import time
import utils.shell_cmds as shell_cmds
from concurrent.futures import ThreadPoolExecutor
from aws_instances.get_instance_ips import get_ec2_instance_ips
from loguru import logger
from utils.counter import AtomicCounter

import pickle

SSH_CONNECT_CHECK_POOL = ThreadPoolExecutor(max_workers=400)

@dataclass
class LaunchConfig:
    """EC2 实例启动配置"""
    instance_count: int
    keypair: str
    role: str
    image_id: str = "ami-09614fb0a0d1c1c5e"
    instance_type: str = "m5.4xlarge"
    use_public_ip: bool = False
    aws_region: str = "us-west-2"
    security_group_id: str = "sg-050516f8dcd5470c6"
    subnet_id: str = "subnet-a5cfe3dc"
    volume_size: int = 250
    ssh_user: str = "ubuntu"

@dataclass
class Instances:
    """EC2 实例启动结果"""

    config: LaunchConfig
    instance_ids: List[str]
    status: Literal["created"] | Literal["running"] | Literal["stopping"]
    ip_addresses: Optional[List[str]] = None

    @classmethod
    def launch(cls, config: LaunchConfig) -> 'Instances':
        """
        创建一组 EC2 实例
        
        参数:
            config: 实例启动配置
        
        返回:
            Instances: 追踪 AWS 实例组的对象
        """
        # 创建 EC2 客户端
        ec2_client = boto3.client('ec2', region_name=config.aws_region)
        
        # 启动实例并确保实例已经运行
        logger.info(f"启动 {config.instance_count} 个 EC2 实例...")
        response = ec2_client.run_instances(
            ImageId=config.image_id,
            MinCount=config.instance_count,
            MaxCount=config.instance_count,
            KeyName=config.keypair,
            InstanceType=config.instance_type,
            SecurityGroupIds=[config.security_group_id],
            SubnetId=config.subnet_id,
            
            BlockDeviceMappings=[
                {
                    'DeviceName': '/dev/sda1',
                    'Ebs': {
                        'VolumeSize': config.volume_size,
                        'VolumeType': 'gp3',
                        'Iops': 3000,
                        # 4. gp3 单卷吞吐量最大值 (MiB/s)
                        'Throughput': 300,
                        # 5. 随实例终止删除，避免残留扣费
                        'DeleteOnTermination': True
                    },
                }
            ],
            TagSpecifications=[
                {
                    'ResourceType': 'instance',
                    'Tags': [
                        {'Key': 'role', 'Value': config.role},
                        {'Key': 'Name', 'Value': f"{config.instance_type}-{config.image_id}"}
                    ]
                }
            ]
        )
        
        # 提取实例 ID
        instance_ids =  [instance['InstanceId'] for instance in response['Instances']]
        
        # 验证创建的实例数量
        if len(instance_ids) != config.instance_count:
            raise ValueError(
                f"实例创建数量不足: 需要 {config.instance_count}, 实际创建 {len(instance_ids)}"
            )
        
        return Instances(instance_ids=instance_ids, status="created", config=config)
    

    def wait_for_all_running(self, check_interval = 3):
        ec2_client = boto3.client('ec2', region_name=self.config.aws_region)

        if self.status == "running":
            return
        elif self.status != "created":
            raise Exception(f"Incorrect status {self.status} while waiting for instance launch")
        
        while True:
            response = ec2_client.describe_instances(
                Filters=[
                    {'Name': 'tag:role', 'Values': [config.role]},
                    {'Name': 'instance-state-name', 'Values': ['running']}
                ]
            )
            
            running_count = sum(
                len(reservation['Instances'])
                for reservation in response['Reservations']
            )
            
            logger.info(f"{running_count} 个实例正在运行...")
            
            if running_count >= config.instance_count:
                break
            
            time.sleep(check_interval)

    
    def check_ssh_connection(
        self,
        ssh_user: Optional[str] = None,
        check_interval: int = 3,
        timeout: int = 5,
        max_retries: int = 20,
    ):
        """
        等待实例可以通过 SSH 连接
        
        参数:
            instance_ids: 实例 ID 列表
            ip_type: IP 类型
            aws_region: AWS 区域
            ssh_user: SSH 用户名
            check_interval: 检查间隔（秒）
        
        返回:
            可连接的 IP 地址列表
        """
        if self.ip_addresses is not None:
            logger.warning("已经进行过 SSH 链接检查")
            return

        logger.debug("等待实例可以通过 SSH 连接...")

        ip_type = "public" if config.use_public_ip else "private"

        if ssh_user is None:
            ssh_user = self.config.ssh_user

        ips = get_ec2_instance_ips(
            instance_ids=self.instance_ids,
            ip_type=ip_type,
            aws_region=self.config.aws_region
        )

        logger.debug("取得所有实例的 ip")

        def test_single_connection(ip: str) -> bool:
            """测试单个 SSH 连接，返回是否成功"""
            result = subprocess.run(
                [
                    "ssh",
                    "-o", "StrictHostKeyChecking=no",
                    "-o", "UserKnownHostsFile=/dev/null",
                    "-o", f"ConnectTimeout={timeout}",
                    "-o", "BatchMode=yes",
                    f"{ssh_user}@{ip}",
                    "exit"
                ],
                check=False,
                capture_output=True,
                text=True
            )
            return result.returncode == 0
        
        def try_connect_single_ip(ip: str, counter: AtomicCounter) -> bool:
            retries = 0
            while not test_single_connection(ip):
                retries += 1
                logger.debug(f"实例 {ip} 连接测试失败，重试 {retries} / {max_retries}")
                if retries >= max_retries:
                    return False
                time.sleep(check_interval)

            counter_idx = counter.increment()
            logger.info(f"实例 {ip} 连接测试成功 ({counter_idx}/{len(ips)})")
            return True
        
        counter = AtomicCounter()

        # 并行执行 SSH 测试
        success_count = sum(SSH_CONNECT_CHECK_POOL.map(lambda ip: try_connect_single_ip(ip, counter), ips))
        
        if success_count < len(ips):
            raise Exception(f"部分实例无法链接，实例组 {self}")

        logger.info(f"完成 {len(ips)} 个实例的 ip 检查")
        
        self.ip_addresses = ips

    def remote_execute(self, script_path: str, args: List[str] = None):
        if args is None:
            args = list()

        if self.ip_addresses is None:
            logger.warning("尚未进行过 SSH 链接检查")
            return
        
        setup_script_name = os.path.basename(script_path)

        def execute_one(ip_address: str, counter: AtomicCounter):
            try:
                shell_cmds.scp(script_path, ip_address, self.config.ssh_user)
                
                shell_cmds.ssh(ip_address, self.config.ssh_user, command=[f'./{setup_script_name}', *args])

                counter_idx = counter.increment()
                logger.info(f"{ip_address} 执行脚本成功 ({counter_idx}/{len(self.ip_addresses)})")

                return 0
            except Exception as e:
                logger.warning(f"{ip_address} 执行脚本失败: {e}")
                return 1
            
        counter = AtomicCounter()
        failure_count = sum(SSH_CONNECT_CHECK_POOL.map(lambda ip: execute_one(ip, counter), self.ip_addresses))
        
        if failure_count > 0:
            raise Exception(f"部分实例执行失败，实例组 {self}")

        logger.info(f"完成 {len(self.ip_addresses)} 个实例的脚本执行")

    def terminate(self):
        """终止所有实例"""
        if self.status == "stopping":
            logger.warning("实例已经在终止中")
            return
        
        ec2_client = boto3.client('ec2', region_name=self.config.aws_region)
        
        logger.info(f"终止 {len(self.instance_ids)} 个实例...")
        ec2_client.terminate_instances(InstanceIds=self.instance_ids)
        
        self.status = "stopping"
        logger.info(f"发送终止请求: {self.instance_ids}")

def launch(config: LaunchConfig, dump_file: str):
    # 启动实例
    instances = Instances.launch(config)
    
    try:
        instances.wait_for_all_running()
        instances.check_ssh_connection()

        instances.remote_execute("./setup_image.sh")
        
        # 使用结果
        logger.success(f"完成 {config.instance_count} 个实例创建")
        logger.info(f"实例 ID: {instances.instance_ids}")
        logger.info(f"IP 地址: {instances.ip_addresses}")

        with open(dump_file, 'wb') as file: 
            pickle.dump(instances, file)
    except Exception:
        traceback.print_exc() 
        logger.warning(f"遇到错误，销毁所有实例")
        instances.terminate()
        logger.success(f"完成实例销毁")

def arg_parser():
    parser = argparse.ArgumentParser(description="Manage EC2 instances")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # create subcommand
    create = subparsers.add_parser("create", help="Create instances", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    create.add_argument("-f", "--dump-file", default="aws_servers.json", help="Path to the instance file")
    create.add_argument("-n", "--instance-count", default=3, type=int, help="Number of instances")
    create.add_argument("-t", "--instance-type", default="m6i.2xlarge", help="Instance Type")
    create.add_argument("-w", "--instance-weight", default=1, type=int, help="Instance Weight")
    
    # destroy subcommand
    destroy = subparsers.add_parser("destroy", help="Destroy instances", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    destroy.add_argument("-f", "--dump-file", default="instances.pkl", help="Path to the instance file")

    return parser


# ========== 使用示例 ==========

if __name__ == "__main__":
    load_dotenv()

    keypair = os.getenv("CONFLUX_MASSIVE_TEST_AWS_KEY_PAIR", "chenxing-st")

    parser = arg_parser()
    args = parser.parse_args()
    
    if args.command == "create":
        # 配置启动参数
        config = LaunchConfig(
            instance_count=args.instance_count,
            keypair=keypair,
            role="massive-test",
            image_id="ami-088f930e18817a524",  # 可选：指定自定义镜像
            instance_type=args.instance_type,          # 可选：指定实例类型
            use_public_ip=True,                  # True 使用公网 IP
            aws_region="us-west-2"
        )
        launch(config, args.dump_file)
    elif args.command == "destroy":
        with open(args.dump_file, "rb") as f:
            instances: Instances = pickle.load(f)
        instances.terminate()
