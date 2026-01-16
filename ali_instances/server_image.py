import argparse
import asyncio
import json
import ipaddress
import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

import asyncssh
from dotenv import load_dotenv
from loguru import logger
from alibabacloud_ecs20140526.client import Client as EcsClient
from alibabacloud_ecs20140526 import models as ecs_models
from alibabacloud_tea_openapi.models import Config as AliyunConfig


DEFAULT_REGION_ID = "ap-southeast-3"
DEFAULT_CONFLUX_GIT_REF = "v3.0.2"
DEFAULT_REMOTE_CONFIG_DIR = "/opt/conflux/config"
DEFAULT_VPC_NAME = "conflux-image-builder"
DEFAULT_VSWITCH_NAME = "conflux-image-builder"
DEFAULT_SECURITY_GROUP_NAME = "conflux-image-builder"
DEFAULT_VPC_CIDR = "10.0.0.0/16"
DEFAULT_VSWITCH_CIDR = "10.0.0.0/24"
DEFAULT_KEYPAIR_NAME = "chenxinghao-conflux-image-builder"
DEFAULT_SSH_PRIVATE_KEY = "./keys/chenxinghao-conflux-image-builder.pem"
DEFAULT_ENDPOINT = "cloudcontrol.aliyuncs.com"


@dataclass(frozen=True)
class AliCredentials:
	access_key_id: str
	access_key_secret: str


@dataclass(frozen=True)
class ImageSource:
	region_id: str
	image_id: str


@dataclass(frozen=True)
class ImageBuildConfig:
	credentials: AliCredentials
	base_image_id: Optional[str]
	instance_type: Optional[str]
	v_switch_id: Optional[str]
	security_group_id: Optional[str]
	conflux_git_ref: str = DEFAULT_CONFLUX_GIT_REF
	min_cpu_cores: int = 2
	min_memory_gb: float = 2.0
	max_memory_gb: float = 4.0
	cpu_vendor: Optional[str] = None
	key_pair_name: str = DEFAULT_KEYPAIR_NAME
	region_id: str = DEFAULT_REGION_ID
	zone_id: Optional[str] = None
	endpoint: Optional[str] = DEFAULT_ENDPOINT
	image_prefix: str = "conflux-massive-test"
	instance_name_prefix: str = "conflux-image-builder"
	internet_max_bandwidth_out: int = 10
	ssh_username: str = "root"
	ssh_private_key_path: str = DEFAULT_SSH_PRIVATE_KEY
	poll_interval: int = 5
	wait_timeout: int = 1800
	cleanup_builder_instance: bool = True
	search_all_regions: bool = False
	use_spot: bool = True
	spot_strategy: str = "SpotAsPriceGo"
	vpc_name: str = DEFAULT_VPC_NAME
	vswitch_name: str = DEFAULT_VSWITCH_NAME
	security_group_name: str = DEFAULT_SECURITY_GROUP_NAME
	vpc_cidr: str = DEFAULT_VPC_CIDR
	vswitch_cidr: str = DEFAULT_VSWITCH_CIDR


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


def build_image_name(image_prefix: str, conflux_git_ref: str) -> str:
	safe_ref = conflux_git_ref.replace("/", "-").replace(":", "-")
	return f"{image_prefix}-conflux-{safe_ref}"


def list_regions(client: EcsClient) -> Sequence[str]:
	request = ecs_models.DescribeRegionsRequest()
	response = client.describe_regions(request)
	regions = response.body.regions.region if response.body and response.body.regions else []
	return [region.region_id for region in regions if region.region_id]


def pick_zone_id(client: EcsClient, region_id: str) -> str:
	request = ecs_models.DescribeZonesRequest(region_id=region_id)
	response = client.describe_zones(request)
	zones = response.body.zones.zone if response.body and response.body.zones else []
	if not zones:
		raise RuntimeError(f"no zones available in region {region_id}")
	return zones[0].zone_id


def pick_zone_and_instance_type(
	client: EcsClient,
	region_id: str,
	min_cpu_cores: int,
	min_memory_gb: float,
	spot_strategy: Optional[str],
) -> tuple[str, str]:
	request = ecs_models.DescribeAvailableResourceRequest(
		region_id=region_id,
		destination_resource="InstanceType",
		resource_type="instance",
		instance_charge_type="PostPaid",
		spot_strategy=spot_strategy,
		cores=min_cpu_cores,
		memory=min_memory_gb,
	)
	response = client.describe_available_resource(request)
	if not response.body or not response.body.available_zones:
		raise RuntimeError("no available zones for instance type")
	for zone in response.body.available_zones.available_zone or []:
		if zone.status_category not in {"WithStock", "ClosedWithStock"}:
			continue
		resources = zone.available_resources.available_resource if zone.available_resources else []
		for resource in resources:
			if resource.type != "InstanceType":
				continue
			supported = resource.supported_resources.supported_resource if resource.supported_resources else []
			for item in supported:
				if item.status_category in {"WithStock", "ClosedWithStock"}:
					return zone.zone_id, item.value
	raise RuntimeError("no in-stock instance type found")


def is_no_stock_error(exc: Exception) -> bool:
	return "OperationDenied.NoStock" in str(exc)


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


def ensure_vswitch(client: EcsClient, region_id: str, vpc_id: str, zone_id: str, name: str, cidr_block: str) -> str:
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
		cidr_block = pick_available_vswitch_cidr([v.cidr_block for v in vswitches], DEFAULT_VPC_CIDR)
		create_request.cidr_block = cidr_block
		create_response = client.create_vswitch(create_request)
	if not create_response.body or not create_response.body.v_switch_id:
		raise RuntimeError("failed to create VSwitch")
	vswitch_id = create_response.body.v_switch_id
	wait_vswitch_available(client, region_id, vswitch_id)
	return vswitch_id


def pick_available_vswitch_cidr(existing_cidrs: list[str], vpc_cidr: str) -> str:
	vpc_net = ipaddress.ip_network(vpc_cidr)
	used = {ipaddress.ip_network(cidr) for cidr in existing_cidrs if cidr}
	for subnet in vpc_net.subnets(new_prefix=24):
		if all(not subnet.overlaps(u) for u in used):
			return str(subnet)
	raise RuntimeError("no available /24 CIDR in VPC")


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


def ensure_security_group(client: EcsClient, region_id: str, vpc_id: str, name: str) -> str:
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
		description="conflux image builder",
	)
	create_response = client.create_security_group(create_request)
	if not create_response.body or not create_response.body.security_group_id:
		raise RuntimeError("failed to create security group")
	security_group_id = create_response.body.security_group_id
	try:
		authorize_request = ecs_models.AuthorizeSecurityGroupRequest(
			region_id=region_id,
			security_group_id=security_group_id,
			ip_protocol="tcp",
			port_range="22/22",
			source_cidr_ip="0.0.0.0/0",
		)
		client.authorize_security_group(authorize_request)
	except Exception as exc:
		logger.warning(f"authorize security group failed or rule exists: {exc}")
	return security_group_id


def ensure_network_resources(client: EcsClient, config: ImageBuildConfig) -> ImageBuildConfig:
	zone_id = config.zone_id or pick_zone_id(client, config.region_id)
	vpc_id = ensure_vpc(client, config.region_id, config.vpc_name, config.vpc_cidr)
	vswitch_id = config.v_switch_id or ensure_vswitch(client, config.region_id, vpc_id, zone_id, config.vswitch_name, config.vswitch_cidr)
	security_group_id = config.security_group_id or ensure_security_group(client, config.region_id, vpc_id, config.security_group_name)
	return ImageBuildConfig(
		credentials=config.credentials,
		base_image_id=config.base_image_id,
		instance_type=config.instance_type,
		min_cpu_cores=config.min_cpu_cores,
		min_memory_gb=config.min_memory_gb,
		max_memory_gb=config.max_memory_gb,
		cpu_vendor=config.cpu_vendor,
		v_switch_id=vswitch_id,
		security_group_id=security_group_id,
		key_pair_name=config.key_pair_name,
		conflux_git_ref=config.conflux_git_ref,
		region_id=config.region_id,
		zone_id=zone_id,
		endpoint=config.endpoint,
		image_prefix=config.image_prefix,
		instance_name_prefix=config.instance_name_prefix,
		internet_max_bandwidth_out=config.internet_max_bandwidth_out,
		ssh_username=config.ssh_username,
		ssh_private_key_path=config.ssh_private_key_path,
		poll_interval=config.poll_interval,
		wait_timeout=config.wait_timeout,
		cleanup_builder_instance=config.cleanup_builder_instance,
		search_all_regions=config.search_all_regions,
		use_spot=config.use_spot,
		spot_strategy=config.spot_strategy,
		vpc_name=config.vpc_name,
		vswitch_name=config.vswitch_name,
		security_group_name=config.security_group_name,
		vpc_cidr=config.vpc_cidr,
		vswitch_cidr=config.vswitch_cidr,
	)


def ensure_key_pair(client: EcsClient, region_id: str, key_pair_name: str, private_key_path: str) -> None:
	request = ecs_models.DescribeKeyPairsRequest(region_id=region_id, key_pair_name=key_pair_name)
	response = client.describe_key_pairs(request)
	keys = response.body.key_pairs.key_pair if response.body and response.body.key_pairs else []
	if keys:
		return
	key_path = ensure_private_key(private_key_path)
	result = subprocess.run(["ssh-keygen", "-y", "-f", key_path], capture_output=True, text=True, check=True)
	public_key = result.stdout.strip()
	import_request = ecs_models.ImportKeyPairRequest(
		region_id=region_id,
		key_pair_name=key_pair_name,
		public_key_body=public_key,
	)
	client.import_key_pair(import_request)


def ensure_private_key(private_key_path: str) -> str:
	key_path = Path(private_key_path).expanduser().resolve()
	if key_path.exists():
		result = subprocess.run(["ssh-keygen", "-y", "-f", str(key_path)], capture_output=True, text=True)
		if result.returncode == 0 and result.stdout.strip():
			return str(key_path)
		key_path.unlink()
	key_path.parent.mkdir(parents=True, exist_ok=True)
	result = subprocess.run(
		[
			"ssh-keygen",
			"-t",
			"rsa",
			"-b",
			"2048",
			"-m",
			"PEM",
			"-f",
			str(key_path),
			"-N",
			"",
		],
		capture_output=True,
		text=True,
		check=True,
	)
	if result.stderr:
		logger.warning(result.stderr.strip())
	return str(key_path)


def find_latest_ubuntu_image(client: EcsClient, region_id: str) -> str:
	request = ecs_models.DescribeImagesRequest(
		region_id=region_id,
		image_owner_alias="system",
		ostype="linux",
		architecture="x86_64",
		page_size=100,
	)
	response = client.describe_images(request)
	images = response.body.images.image if response.body and response.body.images else []
	ubuntu_images = [image for image in images if image.image_name and image.image_name.startswith("ubuntu_20_04")]
	if not ubuntu_images:
		raise RuntimeError("no Ubuntu 20.04 public image found")
	ubuntu_images.sort(key=lambda item: item.creation_time or "", reverse=True)
	return ubuntu_images[0].image_id


def find_existing_image(client: EcsClient, region_id: str, image_name: str) -> Optional[str]:
	request = ecs_models.DescribeImagesRequest(
		region_id=region_id,
		image_name=image_name,
		image_owner_alias="self",
	)
	response = client.describe_images(request)
	images = response.body.images.image if response.body and response.body.images else []
	for image in images:
		if image.image_name == image_name:
			return image.image_id
	return None


def _extract_instance_types(response: ecs_models.DescribeAvailableResourceResponse) -> list[str]:
	result: list[str] = []
	if not response.body or not response.body.available_zones:
		return result
	zones = response.body.available_zones.available_zone or []
	for zone in zones:
		resources = zone.available_resources.available_resource if zone.available_resources else []
		for resource in resources:
			if resource.type != "InstanceType":
				continue
			supported = resource.supported_resources.supported_resource if resource.supported_resources else []
			for item in supported:
				if item.status_category in {"WithStock", "ClosedWithStock"}:
					result.append(item.value)
	return result


def _match_cpu_vendor(model: Optional[str], vendor: Optional[str]) -> bool:
	if not vendor:
		return True
	if not model:
		return False
	vendor_lower = vendor.lower()
	model_lower = model.lower()
	return vendor_lower in model_lower


def _load_instance_type_map(client: EcsClient, instance_type_ids: list[str]) -> dict[str, ecs_models.DescribeInstanceTypesResponseBodyInstanceTypesInstanceType]:
	if not instance_type_ids:
		return {}
	request = ecs_models.DescribeInstanceTypesRequest(instance_types=instance_type_ids)
	response = client.describe_instance_types(request)
	items = response.body.instance_types.instance_type if response.body and response.body.instance_types else []
	return {item.instance_type_id: item for item in items if item.instance_type_id}


def pick_zone_and_instance_type(
	client: EcsClient,
	region_id: str,
	min_cpu_cores: int,
	min_memory_gb: float,
	max_memory_gb: float,
	spot_strategy: Optional[str],
	cpu_vendor: Optional[str],
) -> tuple[str, str, Optional[str]]:
	request = ecs_models.DescribeAvailableResourceRequest(
		region_id=region_id,
		destination_resource="InstanceType",
		resource_type="instance",
		instance_charge_type="PostPaid",
		spot_strategy=spot_strategy,
		cores=min_cpu_cores,
		memory=min_memory_gb,
	)
	response = client.describe_available_resource(request)
	zones = response.body.available_zones.available_zone if response.body and response.body.available_zones else []
	for zone in zones:
		if zone.status_category not in {"WithStock", "ClosedWithStock"}:
			continue
		resources = zone.available_resources.available_resource if zone.available_resources else []
		instance_types: list[str] = []
		for resource in resources:
			if resource.type != "InstanceType":
				continue
			supported = resource.supported_resources.supported_resource if resource.supported_resources else []
			for item in supported:
				if item.status_category in {"WithStock", "ClosedWithStock"}:
					instance_types.append(item.value)
		type_map = _load_instance_type_map(client, instance_types)
		candidates = [
			item for item in type_map.values()
			if item.cpu_core_count == min_cpu_cores
			and item.memory_size is not None
			and min_memory_gb <= item.memory_size <= max_memory_gb
			and _match_cpu_vendor(item.physical_processor_model, cpu_vendor)
		]
		if not candidates:
			continue
		candidates.sort(key=lambda item: (item.memory_size, item.instance_type_id))
		selected = candidates[0]
		vendor = "intel" if "intel" in (selected.physical_processor_model or "").lower() else (
			"amd" if "amd" in (selected.physical_processor_model or "").lower() else None
		)
		return zone.zone_id, selected.instance_type_id, vendor
	raise RuntimeError("no in-stock instance type found")


def pick_instance_type(
	client: EcsClient,
	region_id: str,
	zone_id: str,
	requested_type: str,
	min_cpu_cores: int,
	min_memory_gb: float,
	spot_strategy: Optional[str],
) -> str:
	request = ecs_models.DescribeAvailableResourceRequest(
		region_id=region_id,
		zone_id=zone_id,
		destination_resource="InstanceType",
		resource_type="instance",
		instance_charge_type="PostPaid",
		spot_strategy=spot_strategy,
		instance_type=requested_type,
	)
	response = client.describe_available_resource(request)
	available = _extract_instance_types(response)
	if requested_type in available:
		return requested_type
	request = ecs_models.DescribeAvailableResourceRequest(
		region_id=region_id,
		zone_id=zone_id,
		destination_resource="InstanceType",
		resource_type="instance",
		instance_charge_type="PostPaid",
		spot_strategy=spot_strategy,
		cores=min_cpu_cores,
		memory=min_memory_gb,
	)
	response = client.describe_available_resource(request)
	available = _extract_instance_types(response)
	if not available:
		return requested_type
	return available[0]


def find_image_across_regions(credentials: AliCredentials, target_region_id: str, image_name: str) -> Optional[ImageSource]:
	seed_client = create_client(credentials, target_region_id)
	region_ids = list_regions(seed_client)
	for region_id in region_ids:
		if region_id == target_region_id:
			continue
		client = create_client(credentials, region_id)
		image_id = find_existing_image(client, region_id, image_name)
		if image_id:
			return ImageSource(region_id=region_id, image_id=image_id)
	return None


def wait_for_image_available(client: EcsClient, region_id: str, image_id: str, poll_interval: int, timeout: int) -> None:
	start_time = time.time()
	while True:
		request = ecs_models.DescribeImagesRequest(region_id=region_id, image_id=image_id)
		response = client.describe_images(request)
		images = response.body.images.image if response.body and response.body.images else []
		if images:
			status = images[0].status
			logger.info(f"image {image_id} status: {status}")
			if status == "Available":
				return
			if status in {"CreateFailed", "UnAvailable", "Deprecated"}:
				raise RuntimeError(f"image {image_id} failed with status: {status}")
		if time.time() - start_time > timeout:
			raise TimeoutError(f"image {image_id} not ready after {timeout}s")
		time.sleep(poll_interval)


def copy_image(credentials: AliCredentials, source: ImageSource, destination_region_id: str, destination_name: str) -> str:
	client = create_client(credentials, source.region_id)
	request = ecs_models.CopyImageRequest(
		region_id=source.region_id,
		destination_region_id=destination_region_id,
		image_id=source.image_id,
		destination_image_name=destination_name,
	)
	response = client.copy_image(request)
	if not response.body or not response.body.image_id:
		raise RuntimeError("failed to copy image")
	return response.body.image_id


def create_builder_instance(client: EcsClient, config: ImageBuildConfig) -> str:
	if not config.instance_type:
		raise ValueError("instance_type is required for builder instance creation")
	system_disk_category = pick_system_disk_category(client, config.region_id, config.zone_id)
	system_disk = ecs_models.CreateInstanceRequestSystemDisk(category=system_disk_category, size=40) if system_disk_category else None
	instance_name = f"{config.instance_name_prefix}-{int(time.time())}"
	request = ecs_models.CreateInstanceRequest(
		region_id=config.region_id,
		zone_id=config.zone_id,
		image_id=config.base_image_id,
		instance_type=config.instance_type,
		security_group_id=config.security_group_id,
		v_switch_id=config.v_switch_id,
		key_pair_name=config.key_pair_name,
		instance_name=instance_name,
		internet_max_bandwidth_out=config.internet_max_bandwidth_out,
		internet_charge_type="PayByTraffic",
		instance_charge_type="PostPaid",
		spot_strategy=config.spot_strategy if config.use_spot else None,
		system_disk=system_disk,
	)
	try:
		response = client.create_instance(request)
	except Exception as exc:
		if "InvalidVSwitchId.NotFound" not in str(exc):
			raise
		config = ImageBuildConfig(
			credentials=config.credentials,
			base_image_id=config.base_image_id,
			instance_type=config.instance_type,
			min_cpu_cores=config.min_cpu_cores,
			min_memory_gb=config.min_memory_gb,
			max_memory_gb=config.max_memory_gb,
			cpu_vendor=config.cpu_vendor,
			v_switch_id=None,
			security_group_id=None,
			conflux_git_ref=config.conflux_git_ref,
			key_pair_name=config.key_pair_name,
			region_id=config.region_id,
			zone_id=config.zone_id,
			endpoint=config.endpoint,
			image_prefix=config.image_prefix,
			instance_name_prefix=config.instance_name_prefix,
			internet_max_bandwidth_out=config.internet_max_bandwidth_out,
			ssh_username=config.ssh_username,
			ssh_private_key_path=config.ssh_private_key_path,
			poll_interval=config.poll_interval,
			wait_timeout=config.wait_timeout,
			cleanup_builder_instance=config.cleanup_builder_instance,
			search_all_regions=config.search_all_regions,
			use_spot=config.use_spot,
			spot_strategy=config.spot_strategy,
			vpc_name=config.vpc_name,
			vswitch_name=config.vswitch_name,
			security_group_name=config.security_group_name,
			vpc_cidr=config.vpc_cidr,
			vswitch_cidr=config.vswitch_cidr,
		)
		config = ensure_network_resources(client, config)
		request.v_switch_id = config.v_switch_id
		request.security_group_id = config.security_group_id
		response = client.create_instance(request)
	if not response.body or not response.body.instance_id:
		raise RuntimeError("failed to create instance")
	return response.body.instance_id


def start_instance(client: EcsClient, instance_id: str) -> None:
	request = ecs_models.StartInstanceRequest(instance_id=instance_id)
	client.start_instance(request)


def stop_instance(client: EcsClient, instance_id: str, stopped_mode: Optional[str]) -> None:
	request = ecs_models.StopInstanceRequest(
		instance_id=instance_id,
		force_stop=True,
		stopped_mode=stopped_mode,
	)
	client.stop_instance(request)


def allocate_public_ip(client: EcsClient, instance_id: str) -> Optional[str]:
	request = ecs_models.AllocatePublicIpAddressRequest(instance_id=instance_id)
	response = client.allocate_public_ip_address(request)
	return response.body.ip_address if response.body else None


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


async def prepare_instance(host: str, config: ImageBuildConfig) -> None:
	key_path = str(Path(config.ssh_private_key_path).expanduser())
	start_time = time.time()
	while True:
		try:
			conn = await asyncssh.connect(
				host,
				username=config.ssh_username,
				client_keys=[key_path],
				known_hosts=None,
			)
			break
		except (OSError, asyncssh.Error):
			if time.time() - start_time > config.wait_timeout:
				raise
			await asyncio.sleep(3)
	async with conn:
		commands = [
			"sudo apt-get update -y",
			(
				"sudo apt-get install -y "
				"build-essential clang cmake pkg-config libssl-dev git curl ca-certificates"
			),
			"sudo mkdir -p /opt/conflux/src",
			f"sudo mkdir -p {DEFAULT_REMOTE_CONFIG_DIR}",
			f"sudo chmod 755 {DEFAULT_REMOTE_CONFIG_DIR}",
			(
				"if [ ! -d /opt/conflux/src/conflux-rust ]; then "
				"sudo git clone --depth 1 https://github.com/Conflux-Chain/conflux-rust.git /opt/conflux/src/conflux-rust; "
				"fi"
			),
			(
				"sudo bash -lc 'set -e; "
				"cd /opt/conflux/src/conflux-rust; "
				f"git fetch --depth 1 origin {config.conflux_git_ref} || true; "
				f"git checkout {config.conflux_git_ref} || git checkout FETCH_HEAD; "
				"git submodule update --init --recursive; "
				"curl https://sh.rustup.rs -sSf | sh -s -- -y; "
				"source $HOME/.cargo/env; "
				"cargo --version; "
				"cargo build --release --bin conflux; "
				"install -m 0755 target/release/conflux /usr/local/bin/conflux'"
			),
			"sudo bash -lc 'echo \"conflux config will be injected later\" > /opt/conflux/config/README.txt'",
		]
		for cmd in commands:
			logger.info(f"remote: {cmd}")
			result = await conn.run(cmd, check=False)
			if result.stdout:
				logger.info(result.stdout.strip())
			if result.stderr:
				logger.warning(result.stderr.strip())
			if result.exit_status != 0:
				raise RuntimeError(f"remote command failed: {cmd}")


async def inject_conflux_config(
	host: str,
	local_config_path: str,
	ssh_username: str,
	ssh_private_key_path: str,
	remote_config_dir: str = DEFAULT_REMOTE_CONFIG_DIR,
) -> None:
	key_path = str(Path(ssh_private_key_path).expanduser())
	local_path = Path(local_config_path).expanduser()
	if not local_path.exists():
		raise FileNotFoundError(f"config path not found: {local_path}")
	async with asyncssh.connect(
		host,
		username=ssh_username,
		client_keys=[key_path],
		known_hosts=None,
	) as conn:
		await conn.run(f"sudo mkdir -p {remote_config_dir}", check=True)
		await asyncssh.scp(local_path, (conn, remote_config_dir), recursive=True)


def delete_instance(client: EcsClient, region_id: str, instance_id: str) -> None:
	status = wait_instance_status(
		client,
		region_id,
		instance_id,
		["Stopped", "Running"],
		poll_interval=3,
		timeout=180,
	)
	if status == "Running":
		request = ecs_models.StopInstanceRequest(instance_id=instance_id, force_stop=True, stopped_mode="StopCharging")
		client.stop_instance(request)
		wait_instance_status(
			client,
			region_id,
			instance_id,
			["Stopped"],
			poll_interval=3,
			timeout=180,
		)
	request = ecs_models.DeleteInstanceRequest(instance_id=instance_id, force=True, force_stop=True)
	try:
		client.delete_instance(request)
	except Exception as exc:
		if "IncorrectInstanceStatus" not in str(exc):
			raise
		time.sleep(5)
		client.delete_instance(request)


def create_server_image(config: ImageBuildConfig, dry_run: bool = False) -> str:
	image_name = build_image_name(config.image_prefix, config.conflux_git_ref)
	client = create_client(config.credentials, config.region_id, config.endpoint)
	if not config.base_image_id:
		config = ImageBuildConfig(
			credentials=config.credentials,
			base_image_id=find_latest_ubuntu_image(client, config.region_id),
			instance_type=config.instance_type,
			v_switch_id=config.v_switch_id,
			security_group_id=config.security_group_id,
			key_pair_name=config.key_pair_name,
			conflux_git_ref=config.conflux_git_ref,
			region_id=config.region_id,
			zone_id=config.zone_id,
			endpoint=config.endpoint,
			image_prefix=config.image_prefix,
			instance_name_prefix=config.instance_name_prefix,
			internet_max_bandwidth_out=config.internet_max_bandwidth_out,
			ssh_username=config.ssh_username,
			ssh_private_key_path=config.ssh_private_key_path,
			poll_interval=config.poll_interval,
			wait_timeout=config.wait_timeout,
			cleanup_builder_instance=config.cleanup_builder_instance,
			search_all_regions=config.search_all_regions,
			use_spot=config.use_spot,
			spot_strategy=config.spot_strategy,
			vpc_name=config.vpc_name,
			vswitch_name=config.vswitch_name,
			security_group_name=config.security_group_name,
			vpc_cidr=config.vpc_cidr,
			vswitch_cidr=config.vswitch_cidr,
		)

	existing_image_id = find_existing_image(client, config.region_id, image_name)
	if existing_image_id:
		logger.info(f"image already exists in {config.region_id}: {existing_image_id}")
		if dry_run:
			return f"dry-run:{existing_image_id}"
		wait_for_image_available(client, config.region_id, existing_image_id, config.poll_interval, config.wait_timeout)
		return existing_image_id

	if config.search_all_regions:
		source = find_image_across_regions(config.credentials, config.region_id, image_name)
		if source:
			if dry_run:
				logger.info(
					"dry-run: would copy image %s from %s to %s",
					source.image_id,
					source.region_id,
					config.region_id,
				)
				return f"dry-run:copy:{source.image_id}"
			logger.info(f"copying image {source.image_id} from {source.region_id} to {config.region_id}")
			image_id = copy_image(config.credentials, source, config.region_id, image_name)
			logger.info(f"image copy started: {image_id}")
			wait_for_image_available(client, config.region_id, image_id, config.poll_interval, config.wait_timeout)
			return image_id

	if dry_run:
		logger.info(
			"dry-run: would create image %s using base image %s, vSwitch %s, security group %s",
			image_name,
			config.base_image_id,
			config.v_switch_id or config.vswitch_name,
			config.security_group_id or config.security_group_name,
		)
		return f"dry-run:{image_name}"

	try:
		zone_id, selected_type, vendor = pick_zone_and_instance_type(
			client,
			config.region_id,
			config.min_cpu_cores,
			2.0,
			2.0,
			config.spot_strategy if config.use_spot else None,
			config.cpu_vendor,
		)
	except RuntimeError:
		zone_id, selected_type, vendor = pick_zone_and_instance_type(
			client,
			config.region_id,
			config.min_cpu_cores,
			4.0,
			4.0,
			config.spot_strategy if config.use_spot else None,
			config.cpu_vendor,
		)
	config = ImageBuildConfig(
		credentials=config.credentials,
		base_image_id=config.base_image_id,
		instance_type=selected_type,
		min_cpu_cores=config.min_cpu_cores,
		min_memory_gb=config.min_memory_gb,
		max_memory_gb=config.max_memory_gb,
		cpu_vendor=vendor or config.cpu_vendor,
		v_switch_id=config.v_switch_id,
		security_group_id=config.security_group_id,
		key_pair_name=config.key_pair_name,
		conflux_git_ref=config.conflux_git_ref,
		region_id=config.region_id,
		zone_id=zone_id,
		endpoint=config.endpoint,
		image_prefix=config.image_prefix,
		instance_name_prefix=config.instance_name_prefix,
		internet_max_bandwidth_out=config.internet_max_bandwidth_out,
		ssh_username=config.ssh_username,
		ssh_private_key_path=config.ssh_private_key_path,
		poll_interval=config.poll_interval,
		wait_timeout=config.wait_timeout,
		cleanup_builder_instance=config.cleanup_builder_instance,
		search_all_regions=config.search_all_regions,
		use_spot=config.use_spot,
		spot_strategy=config.spot_strategy,
		vpc_name=config.vpc_name,
		vswitch_name=config.vswitch_name,
		security_group_name=config.security_group_name,
		vpc_cidr=config.vpc_cidr,
		vswitch_cidr=config.vswitch_cidr,
	)
	config = ensure_network_resources(client, config)
	ensure_key_pair(client, config.region_id, config.key_pair_name, config.ssh_private_key_path)

	try:
		instance_id = create_builder_instance(client, config)
	except Exception as exc:
		if not (config.use_spot and is_no_stock_error(exc)):
			raise
		zone_id, selected_type = pick_zone_and_instance_type(
			client,
			config.region_id,
			config.min_cpu_cores,
			config.min_memory_gb,
			None,
		)
		config = ImageBuildConfig(
			credentials=config.credentials,
			base_image_id=config.base_image_id,
			instance_type=selected_type,
			min_cpu_cores=config.min_cpu_cores,
			min_memory_gb=config.min_memory_gb,
			v_switch_id=config.v_switch_id,
			security_group_id=config.security_group_id,
			conflux_git_ref=config.conflux_git_ref,
			key_pair_name=config.key_pair_name,
			region_id=config.region_id,
			zone_id=zone_id,
			endpoint=config.endpoint,
			image_prefix=config.image_prefix,
			instance_name_prefix=config.instance_name_prefix,
			internet_max_bandwidth_out=config.internet_max_bandwidth_out,
			ssh_username=config.ssh_username,
			ssh_private_key_path=config.ssh_private_key_path,
			poll_interval=config.poll_interval,
			wait_timeout=config.wait_timeout,
			cleanup_builder_instance=config.cleanup_builder_instance,
			search_all_regions=config.search_all_regions,
			use_spot=False,
			spot_strategy=config.spot_strategy,
			vpc_name=config.vpc_name,
			vswitch_name=config.vswitch_name,
			security_group_name=config.security_group_name,
			vpc_cidr=config.vpc_cidr,
			vswitch_cidr=config.vswitch_cidr,
		)
		instance_id = create_builder_instance(client, config)
	logger.info(f"builder instance created: {instance_id}")
	try:
		status = wait_instance_status(
			client,
			config.region_id,
			instance_id,
			["Stopped", "Running"],
			config.poll_interval,
			config.wait_timeout,
		)
		if status == "Stopped":
			start_instance(client, instance_id)
		wait_instance_status(
			client,
			config.region_id,
			instance_id,
			["Running"],
			config.poll_interval,
			config.wait_timeout,
		)
		allocate_public_ip(client, instance_id)
		public_ip = wait_instance_running(client, config.region_id, instance_id, config.poll_interval, config.wait_timeout)
		logger.info(f"builder instance ready: {public_ip}")

		asyncio.run(prepare_instance(public_ip, config))
		logger.info("stopping builder instance before image creation")
		stop_instance(client, instance_id, "StopCharging")
		wait_instance_status(
			client,
			config.region_id,
			instance_id,
			["Stopped"],
			config.poll_interval,
			config.wait_timeout,
		)

		create_request = ecs_models.CreateImageRequest(
			region_id=config.region_id,
			instance_id=instance_id,
			image_name=image_name,
		)
		try:
			create_response = client.create_image(create_request)
		except Exception:
			logger.warning("image creation failed with StopCharging; retrying with normal stop")
			stop_instance(client, instance_id, None)
			wait_instance_status(
				client,
				config.region_id,
				instance_id,
				["Stopped"],
				config.poll_interval,
				config.wait_timeout,
			)
			create_response = client.create_image(create_request)
		if not create_response.body or not create_response.body.image_id:
			raise RuntimeError("failed to create image")
		image_id = create_response.body.image_id
		logger.info(f"image creation started: {image_id}")

		wait_for_image_available(client, config.region_id, image_id, config.poll_interval, config.wait_timeout)
		return image_id
	finally:
		if config.cleanup_builder_instance and instance_id:
			delete_instance(client, config.region_id, instance_id)
			logger.info(f"builder instance deleted: {instance_id}")


def build_arg_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(description="Create Alibaba Cloud image for Conflux nodes")
	parser.add_argument("--conflux-git-ref", default=DEFAULT_CONFLUX_GIT_REF, help="Conflux git ref to build on the image")
	parser.add_argument("--base-image-id", default=None, help="Base system image ID (Ubuntu public image if empty)")
	parser.add_argument("--instance-type", default=None, help="ECS instance type (auto-pick if empty)")
	parser.add_argument("--min-cpu-cores", type=int, default=2)
	parser.add_argument("--min-memory-gb", type=float, default=4.0)
	parser.add_argument("--v-switch-id", default=None, help="VSwitch ID (auto-create if empty)")
	parser.add_argument("--security-group-id", default=None, help="Security group ID (auto-create if empty)")
	parser.add_argument("--key-pair-name", default=DEFAULT_KEYPAIR_NAME, help="Key pair name for SSH")
	parser.add_argument("--region-id", default=DEFAULT_REGION_ID)
	parser.add_argument("--zone-id", default=None)
	parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help="Custom ECS endpoint")
	parser.add_argument("--image-prefix", default="conflux")
	parser.add_argument("--ssh-username", default="root")
	parser.add_argument("--ssh-private-key", default=DEFAULT_SSH_PRIVATE_KEY)
	parser.add_argument("--internet-max-bandwidth-out", type=int, default=10)
	parser.add_argument("--poll-interval", type=int, default=5)
	parser.add_argument("--wait-timeout", type=int, default=1800)
	parser.add_argument("--search-all-regions", action="store_true")
	parser.add_argument("--spot", action="store_true")
	parser.add_argument("--spot-strategy", default="SpotAsPriceGo")
	parser.add_argument("--vpc-name", default=DEFAULT_VPC_NAME)
	parser.add_argument("--vswitch-name", default=DEFAULT_VSWITCH_NAME)
	parser.add_argument("--security-group-name", default=DEFAULT_SECURITY_GROUP_NAME)
	parser.add_argument("--vpc-cidr", default=DEFAULT_VPC_CIDR)
	parser.add_argument("--vswitch-cidr", default=DEFAULT_VSWITCH_CIDR)
	parser.add_argument("--no-cleanup", action="store_true")
	parser.add_argument("--dry-run", action="store_true")
	return parser


def main() -> None:
	parser = build_arg_parser()
	args = parser.parse_args()
	credentials = load_ali_credentials()
	endpoint = args.endpoint or load_endpoint()
	config = ImageBuildConfig(
		credentials=credentials,
		base_image_id=args.base_image_id,
		instance_type=args.instance_type,
		min_cpu_cores=args.min_cpu_cores,
		min_memory_gb=args.min_memory_gb,
		v_switch_id=args.v_switch_id,
		security_group_id=args.security_group_id,
		key_pair_name=args.key_pair_name,
		conflux_git_ref=args.conflux_git_ref,
		region_id=args.region_id,
		zone_id=args.zone_id,
		endpoint=endpoint,
		image_prefix=args.image_prefix,
		ssh_username=args.ssh_username,
		ssh_private_key_path=args.ssh_private_key,
		internet_max_bandwidth_out=args.internet_max_bandwidth_out,
		poll_interval=args.poll_interval,
		wait_timeout=args.wait_timeout,
		cleanup_builder_instance=not args.no_cleanup,
		search_all_regions=args.search_all_regions,
		use_spot=args.spot,
		spot_strategy=args.spot_strategy,
		vpc_name=args.vpc_name,
		vswitch_name=args.vswitch_name,
		security_group_name=args.security_group_name,
		vpc_cidr=args.vpc_cidr,
		vswitch_cidr=args.vswitch_cidr,
	)
	image_id = create_server_image(config, dry_run=args.dry_run)
	logger.info(f"image id: {image_id}")


if __name__ == "__main__":
	main()