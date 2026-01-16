import asyncio
import ipaddress
import json
import os
import time
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Sequence

import asyncssh
from alibabacloud_ecs20140526.client import Client as EcsClient
from alibabacloud_ecs20140526 import models as ecs_models
from alibabacloud_tea_openapi.models import Config as AliyunConfig
from dotenv import load_dotenv
from loguru import logger

DEFAULT_ENDPOINT = "cloudcontrol.aliyuncs.com"


@dataclass(frozen=True)
class AliCredentials:
	access_key_id: str
	access_key_secret: str


def load_ali_credentials() -> AliCredentials:
	load_dotenv()
	access_key_id = os.getenv("ALI_ACCESS_KEY_ID", "").strip()
	access_key_secret = os.getenv("ALI_ACCESS_KEY_SECRET", "").strip()
	if not access_key_id or not access_key_secret:
		raise ValueError("Missing ALI_ACCESS_KEY_ID or ALI_ACCESS_KEY_SECRET in .env")
	return AliCredentials(access_key_id=access_key_id, access_key_secret=access_key_secret)


def load_endpoint() -> Optional[str]:
	value = os.getenv("ALI_ECS_ENDPOINT", "").strip()
	return value or DEFAULT_ENDPOINT


def normalize_endpoint(region_id: str, endpoint: Optional[str]) -> Optional[str]:
	if not endpoint:
		return None
	if "cloudcontrol.aliyuncs.com" in endpoint:
		return f"ecs.{region_id}.aliyuncs.com"
	return endpoint


def create_client(credentials: AliCredentials, region_id: str, endpoint: Optional[str] = None) -> EcsClient:
	endpoint = normalize_endpoint(region_id, endpoint)
	config = AliyunConfig(
		access_key_id=credentials.access_key_id,
		access_key_secret=credentials.access_key_secret,
		region_id=region_id,
		endpoint=endpoint,
	)
	return EcsClient(config)


def pick_zone_id(client: EcsClient, region_id: str) -> str:
	request = ecs_models.DescribeZonesRequest(region_id=region_id)
	response = client.describe_zones(request)
	zones = response.body.zones.zone if response.body and response.body.zones else []
	if not zones:
		raise RuntimeError(f"no zones available in region {region_id}")
	return zones[0].zone_id


def pick_system_disk_category(client: EcsClient, region_id: str, zone_id: str) -> Optional[str]:
	request = ecs_models.DescribeZonesRequest(region_id=region_id)
	response = client.describe_zones(request)
	zones = response.body.zones.zone if response.body and response.body.zones else []
	for zone in zones:
		if zone.zone_id != zone_id:
			continue
		available = zone.available_resources.resources_info if zone.available_resources else []
		for info in available:
			categories = info.system_disk_categories.supported_system_disk_category if info.system_disk_categories else []
			if categories:
				for preferred in ["cloud_essd", "cloud_ssd", "cloud_efficiency", "cloud"]:
					if preferred in categories:
						return preferred
				return categories[0]
	return None


def wait_instance_status(
	client: EcsClient,
	region_id: str,
	instance_id: str,
	desired_statuses: Sequence[str],
	poll_interval: int,
	timeout: int,
) -> str:
	start_time = time.time()
	while True:
		request = ecs_models.DescribeInstancesRequest(
			region_id=region_id,
			instance_ids=json.dumps([instance_id]),
		)
		response = client.describe_instances(request)
		instances = response.body.instances.instance if response.body and response.body.instances else []
		if instances:
			status = instances[0].status
			if status in desired_statuses:
				return status
		if time.time() - start_time > timeout:
			raise TimeoutError(f"instance {instance_id} not in {desired_statuses} after {timeout}s")
		time.sleep(poll_interval)


def get_instance_public_ip(client: EcsClient, region_id: str, instance_id: str) -> Optional[str]:
	request = ecs_models.DescribeInstancesRequest(
		region_id=region_id,
		instance_ids=json.dumps([instance_id]),
	)
	response = client.describe_instances(request)
	instances = response.body.instances.instance if response.body and response.body.instances else []
	if not instances:
		return None
	public_ips = instances[0].public_ip_address.ip_address if instances[0].public_ip_address else []
	return public_ips[0] if public_ips else None


def wait_instance_running(client: EcsClient, region_id: str, instance_id: str, poll_interval: int, timeout: int) -> str:
	start_time = time.time()
	while True:
		request = ecs_models.DescribeInstancesRequest(
			region_id=region_id,
			instance_ids=json.dumps([instance_id]),
		)
		response = client.describe_instances(request)
		instances = response.body.instances.instance if response.body and response.body.instances else []
		if instances:
			status = instances[0].status
			public_ip = get_instance_public_ip(client, region_id, instance_id)
			logger.info(f"instance {instance_id} status: {status}, public_ip: {public_ip}")
			if status == "Running" and public_ip:
				return public_ip
		if time.time() - start_time > timeout:
			raise TimeoutError(f"instance {instance_id} not ready after {timeout}s")
		time.sleep(poll_interval)


async def wait_for_ssh_ready(
	host: str,
	username: str,
	private_key_path: str,
	timeout: int,
	interval: int = 3,
) -> None:
	start_time = time.time()
	key_path = str(Path(private_key_path).expanduser())
	while True:
		try:
			conn = await asyncssh.connect(
				host,
				username=username,
				client_keys=[key_path],
				known_hosts=None,
			)
			conn.close()
			await conn.wait_closed()
			return
		except (OSError, asyncssh.Error):
			if time.time() - start_time > timeout:
				raise TimeoutError(f"SSH not ready for {host} after {timeout}s")
			await asyncio.sleep(interval)


def start_instance(client: EcsClient, instance_id: str) -> None:
	request = ecs_models.StartInstanceRequest(instance_id=instance_id)
	client.start_instance(request)


def stop_instance(client: EcsClient, instance_id: str, stopped_mode: Optional[str] = None) -> None:
	request = ecs_models.StopInstanceRequest(
		instance_id=instance_id,
		force_stop=True,
		stopped_mode=stopped_mode,
	)
	client.stop_instance(request)


def allocate_public_ip(
	client: EcsClient,
	region_id: str,
	instance_id: str,
	poll_interval: int = 3,
	timeout: int = 120,
) -> Optional[str]:
	start_time = time.time()
	while True:
		try:
			request = ecs_models.AllocatePublicIpAddressRequest(instance_id=instance_id)
			response = client.allocate_public_ip_address(request)
			return response.body.ip_address if response.body else None
		except Exception as exc:
			if "IncorrectInstanceStatus" not in str(exc):
				raise
			if time.time() - start_time > timeout:
				raise
			wait_instance_status(
				client,
				region_id,
				instance_id,
				["Running", "Stopped"],
				poll_interval,
				timeout,
			)
			time.sleep(poll_interval)


def delete_instance(
	client: EcsClient,
	region_id: str,
	instance_id: str,
	poll_interval: int = 5,
	timeout: int = 300,
) -> None:
	request = ecs_models.StopInstanceRequest(instance_id=instance_id, force_stop=True, stopped_mode="StopCharging")
	try:
		client.stop_instance(request)
	except Exception as exc:
		if "IncorrectInstanceStatus" not in str(exc):
			pass
		else:
			wait_instance_status(client, region_id, instance_id, ["Running", "Stopped"], poll_interval, timeout)
			try:
				client.stop_instance(request)
			except Exception:
				pass
	delete_request = ecs_models.DeleteInstanceRequest(instance_id=instance_id, force=True, force_stop=True)
	for _ in range(5):
		try:
			client.delete_instance(delete_request)
			return
		except Exception as exc:
			message = str(exc)
			if "IncorrectInstanceStatus" not in message and "InvalidOperation.Conflict" not in message:
				raise
			wait_instance_status(client, region_id, instance_id, ["Running", "Stopped"], poll_interval, timeout)
			time.sleep(poll_interval)


def wait_vpc_available(client: EcsClient, region_id: str, vpc_id: str, timeout: int = 120) -> None:
	start_time = time.time()
	while True:
		request = ecs_models.DescribeVpcsRequest(region_id=region_id, vpc_id=vpc_id)
		response = client.describe_vpcs(request)
		vpcs = response.body.vpcs.vpc if response.body and response.body.vpcs else []
		status = vpcs[0].status if vpcs else None
		if status == "Available":
			return
		if time.time() - start_time > timeout:
			raise TimeoutError(f"VPC {vpc_id} not available after {timeout}s")
		time.sleep(3)


def ensure_vpc(client: EcsClient, region_id: str, vpc_name: str, cidr_block: str) -> str:
	request = ecs_models.DescribeVpcsRequest(region_id=region_id, page_size=50)
	response = client.describe_vpcs(request)
	vpcs = response.body.vpcs.vpc if response.body and response.body.vpcs else []
	for vpc in vpcs:
		if vpc.vpc_name == vpc_name:
			return vpc.vpc_id
	create_request = ecs_models.CreateVpcRequest(region_id=region_id, vpc_name=vpc_name, cidr_block=cidr_block)
	create_response = client.create_vpc(create_request)
	if not create_response.body or not create_response.body.vpc_id:
		raise RuntimeError("failed to create VPC")
	vpc_id = create_response.body.vpc_id
	wait_vpc_available(client, region_id, vpc_id)
	return vpc_id


def wait_vswitch_available(client: EcsClient, region_id: str, vswitch_id: str, timeout: int = 120) -> None:
	start_time = time.time()
	while True:
		request = ecs_models.DescribeVSwitchesRequest(region_id=region_id, v_switch_id=vswitch_id)
		response = client.describe_vswitches(request)
		vswitches = response.body.v_switches.v_switch if response.body and response.body.v_switches else []
		status = vswitches[0].status if vswitches else None
		if status == "Available":
			return
		if time.time() - start_time > timeout:
			raise TimeoutError(f"VSwitch {vswitch_id} not available after {timeout}s")
		time.sleep(3)


def pick_available_vswitch_cidr(existing_cidrs: list[str], vpc_cidr: str) -> str:
	vpc_net = ipaddress.ip_network(vpc_cidr)
	used = {ipaddress.ip_network(cidr) for cidr in existing_cidrs if cidr}
	for subnet in vpc_net.subnets(new_prefix=24):
		if all(not subnet.overlaps(u) for u in used):
			return str(subnet)
	raise RuntimeError("no available /24 CIDR in VPC")


def ensure_vswitch(client: EcsClient, region_id: str, vpc_id: str, zone_id: str, name: str, cidr_block: str, vpc_cidr: str) -> str:
	request = ecs_models.DescribeVSwitchesRequest(region_id=region_id, vpc_id=vpc_id, page_size=50)
	response = client.describe_vswitches(request)
	vswitches = response.body.v_switches.v_switch if response.body and response.body.v_switches else []
	for vswitch in vswitches:
		if vswitch.v_switch_name == name and vswitch.zone_id == zone_id:
			return vswitch.v_switch_id
	create_request = ecs_models.CreateVSwitchRequest(
		region_id=region_id,
		vpc_id=vpc_id,
		zone_id=zone_id,
		v_switch_name=name,
		cidr_block=cidr_block,
	)
	try:
		create_response = client.create_vswitch(create_request)
	except Exception as exc:
		if "InvalidCidrBlock.Overlapped" not in str(exc):
			raise
		cidr_block = pick_available_vswitch_cidr([v.cidr_block for v in vswitches], vpc_cidr)
		create_request.cidr_block = cidr_block
		create_response = client.create_vswitch(create_request)
	if not create_response.body or not create_response.body.v_switch_id:
		raise RuntimeError("failed to create VSwitch")
	vswitch_id = create_response.body.v_switch_id
	wait_vswitch_available(client, region_id, vswitch_id)
	return vswitch_id


def ensure_security_group(client: EcsClient, region_id: str, vpc_id: str, name: str, description: str) -> str:
	request = ecs_models.DescribeSecurityGroupsRequest(region_id=region_id, vpc_id=vpc_id, page_size=50)
	response = client.describe_security_groups(request)
	groups = response.body.security_groups.security_group if response.body and response.body.security_groups else []
	for group in groups:
		if group.security_group_name == name:
			return group.security_group_id
	create_request = ecs_models.CreateSecurityGroupRequest(
		region_id=region_id,
		vpc_id=vpc_id,
		security_group_name=name,
		description=description,
	)
	create_response = client.create_security_group(create_request)
	if not create_response.body or not create_response.body.security_group_id:
		raise RuntimeError("failed to create security group")
	return create_response.body.security_group_id


def authorize_security_group_port(client: EcsClient, region_id: str, security_group_id: str, port: int) -> None:
	try:
		request = ecs_models.AuthorizeSecurityGroupRequest(
			region_id=region_id,
			security_group_id=security_group_id,
			ip_protocol="tcp",
			port_range=f"{port}/{port}",
			source_cidr_ip="0.0.0.0/0",
		)
		client.authorize_security_group(request)
	except Exception as exc:
		logger.warning(f"authorize security group failed or rule exists: {exc}")


def ensure_network_resources(
	client: EcsClient,
	*,
	region_id: str,
	zone_id: Optional[str],
	v_switch_id: Optional[str],
	security_group_id: Optional[str],
	vpc_name: str,
	vpc_cidr: str,
	vswitch_name: str,
	vswitch_cidr: str,
	security_group_name: str,
	security_group_desc: str,
	open_ports: Sequence[int] = (),
) -> tuple[str, str, str]:
	selected_zone_id = zone_id or pick_zone_id(client, region_id)
	vpc_id = ensure_vpc(client, region_id, vpc_name, vpc_cidr)
	selected_vswitch_id = v_switch_id or ensure_vswitch(client, region_id, vpc_id, selected_zone_id, vswitch_name, vswitch_cidr, vpc_cidr)
	selected_security_group_id = security_group_id or ensure_security_group(client, region_id, vpc_id, security_group_name, security_group_desc)
	authorize_security_group_port(client, region_id, selected_security_group_id, 22)
	for port in open_ports:
		authorize_security_group_port(client, region_id, selected_security_group_id, port)
	return selected_zone_id, selected_vswitch_id, selected_security_group_id
