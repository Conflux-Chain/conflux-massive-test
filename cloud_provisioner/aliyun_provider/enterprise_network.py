from dataclasses import dataclass
from itertools import combinations
from typing import Dict, Iterable, List, Optional, Tuple

import alibabacloud_cbn20170912.models as cbn_models
import alibabacloud_ecs20140526.models as ecs_models
from alibabacloud_tea_openapi.exceptions import ClientException
from alibabacloud_tea_openapi.models import OpenApiRequest, Params
from alibabacloud_tea_util.models import RuntimeOptions
from loguru import logger

from cloud_provisioner.host_spec import HostSpec
from cloud_provisioner.create_instances.provision_config import CloudConfig
from cloud_provisioner.create_instances.regional_infra import InfraProvider
from utils.wait_until import wait_until


@dataclass(frozen=True)
class AliyunEnterpriseNetworkInfo:
    cen_id: str
    transit_router_ids: Dict[str, str]
    vpc_attachment_ids: Dict[str, str]
    peer_attachment_ids: Dict[Tuple[str, str], str]


class AliyunEnterpriseNetworkPermissionError(RuntimeError):
    pass


def ensure_enterprise_network(client, cloud_config: CloudConfig, infra_provider: InfraProvider) -> AliyunEnterpriseNetworkInfo:
    if not cloud_config.enterprise_network.enabled:
        raise ValueError("enterprise network is disabled")

    _ensure_transit_router_service(client)

    cen_name = cloud_config.get_enterprise_network_name()
    cen_description = cloud_config.get_enterprise_network_description()
    cen_id = _ensure_cen(client, cen_name, cen_description, cloud_config.user_tag)

    transit_router_ids: Dict[str, str] = {}
    for region_id in sorted(infra_provider.regions):
        transit_router_ids[region_id] = _ensure_transit_router(
            client,
            region_id=region_id,
            cen_id=cen_id,
            transit_router_name=f"{cen_name}-{region_id}-tr",
            description=f"Transit router for {cen_name} in {region_id}",
        )

    vpc_attachment_ids: Dict[str, str] = {}
    for region_id, region_info in infra_provider.regions.items():
        zone_mapping = _pick_zone_mapping(region_info)
        vpc_attachment_ids[region_id] = _ensure_vpc_attachment(
            client,
            region_id=region_id,
            cen_id=cen_id,
            transit_router_id=transit_router_ids[region_id],
            vpc_id=region_info.vpc_id,
            attachment_name=f"{cen_name}-{region_id}-vpc",
            zone_mapping=zone_mapping,
        )

    peer_attachment_ids: Dict[Tuple[str, str], str] = {}
    region_pairs = combinations(sorted(transit_router_ids), 2)
    for region_a, region_b in region_pairs:
        pair_key = (region_a, region_b)
        peer_attachment_ids[pair_key] = _ensure_peer_attachment(
            client,
            region_id=region_a,
            cen_id=cen_id,
            transit_router_id=transit_router_ids[region_a],
            peer_transit_router_id=transit_router_ids[region_b],
            peer_region_id=region_b,
            attachment_name=f"{cen_name}-{region_a}-{region_b}-peer",
            bandwidth=cloud_config.enterprise_network.peer_bandwidth_mbps,
            bandwidth_type=cloud_config.enterprise_network.bandwidth_type,
        )

    return AliyunEnterpriseNetworkInfo(
        cen_id=cen_id,
        transit_router_ids=transit_router_ids,
        vpc_attachment_ids=vpc_attachment_ids,
        peer_attachment_ids=peer_attachment_ids,
    )


def assign_instance_private_mesh_ips(client, hosts: List[HostSpec], cloud_config: CloudConfig) -> List[HostSpec]:
    if not cloud_config.enterprise_network.enabled or not cloud_config.enterprise_network.allocate_instance_private_ip:
        return hosts

    updated_hosts: List[HostSpec] = []
    for host in hosts:
        if host.provider != "aliyun":
            updated_hosts.append(host)
            continue

        p2p_ip = _ensure_instance_secondary_private_ip(
            client,
            region_id=host.region,
            instance_id=host.instance_id,
            fallback_private_ip=host.private_ip,
        )
        updated_hosts.append(
            HostSpec(
                ip=host.ip,
                nodes_per_host=host.nodes_per_host,
                ssh_user=host.ssh_user,
                ssh_key_path=host.ssh_key_path,
                provider=host.provider,
                region=host.region,
                zone=host.zone,
                instance_id=host.instance_id,
                private_ip=host.private_ip,
                p2p_ip=p2p_ip,
            )
        )

    return updated_hosts


def _ensure_transit_router_service(client) -> None:
    cbn_client = client.build_cbn()
    if _is_transit_router_service_enabled(cbn_client):
        return

    logger.info("Transit router service is disabled, opening service...")
    try:
        cbn_client.open_transit_router_service(cbn_models.OpenTransitRouterServiceRequest())
    except ClientException as exc:
        if _is_missing_cen_service_role(exc):
            _ensure_cen_service_linked_role(client)
            cbn_client.open_transit_router_service(cbn_models.OpenTransitRouterServiceRequest())
        elif not _is_transit_router_service_opening_conflict(exc):
            raise

    wait_until(
        lambda: _is_transit_router_service_enabled(cbn_client),
        timeout=300,
        retry_interval=5,
    )


def _ensure_cen_service_linked_role(client) -> None:
    logger.info("CEN service-linked role is missing, creating it through RAM...")
    ram_client = client.build_ram()
    try:
        ram_client.call_api(
            Params(
                action="CreateServiceLinkedRole",
                version="2015-05-01",
                protocol="HTTPS",
                pathname="/",
                method="POST",
                auth_type="AK",
                body_type="json",
                req_body_type="formData",
                style="RPC",
            ),
            OpenApiRequest(query={"ServiceName": "cen.aliyuncs.com"}),
            RuntimeOptions(),
        )
    except ClientException as exc:
        if _is_service_linked_role_already_exists(exc):
            return
        if getattr(exc, "code", "") == "NoPermission":
            raise AliyunEnterpriseNetworkPermissionError(
                "Aliyun credentials cannot create the required CEN service-linked role. "
                "Grant ram:CreateServiceLinkedRole or pre-create the CEN service-linked role in the account, then retry provisioning."
            ) from exc
        raise


def _is_transit_router_service_enabled(cbn_client) -> bool:
    try:
        response = cbn_client.check_transit_router_service(cbn_models.CheckTransitRouterServiceRequest())
    except ClientException as exc:
        if _is_transit_router_service_not_open(exc):
            return False
        raise

    return bool(getattr(response.body, "enabled", False))


def _is_transit_router_service_not_open(exc: ClientException) -> bool:
    return getattr(exc, "code", "") == "USER_NOT_OPEN_TR_SERVICE"


def _is_missing_cen_service_role(exc: ClientException) -> bool:
    return getattr(exc, "code", "") == "NoPermission.AliyunServiceRoleForCEN"


def _is_service_linked_role_already_exists(exc: ClientException) -> bool:
    code = getattr(exc, "code", "")
    if "AlreadyExists" in code:
        return True

    message = str(exc).lower()
    return "service linked role" in message and "already" in message


def _is_transit_router_service_opening_conflict(exc: ClientException) -> bool:
    code = getattr(exc, "code", "")
    if code == "USER_NOT_OPEN_TR_SERVICE":
        return False
    return code in {"INCORRECT_STATUS.TransitRouterService", "OperationDenied.TransitRouterService"}


def _ensure_cen(client, cen_name: str, description: str, user_tag: str) -> str:
    cbn_client = client.build_cbn()
    cens = _describe_cens(cbn_client, cen_name)
    for cen in cens:
        if cen.get("Name") == cen_name:
            cen_id = cen.get("CenId")
            if cen_id:
                logger.info(f"Get CEN {cen_name}: {cen_id}")
                return cen_id

    response = cbn_client.create_cen(
        cbn_models.CreateCenRequest(
            name=cen_name,
            description=description,
            tag=_build_tags(user_tag, scope="enterprise-network"),
        )
    )
    cen_id = response.body.cen_id
    if not cen_id:
        raise RuntimeError(f"Create CEN {cen_name} returned empty cen_id")

    wait_until(lambda: _has_named_cen(cbn_client, cen_name, cen_id), timeout=300, retry_interval=5)
    logger.info(f"Created CEN {cen_name}: {cen_id}")
    return cen_id


def _ensure_transit_router(client, *, region_id: str, cen_id: str, transit_router_name: str, description: str) -> str:
    cbn_client = client.build_cbn()
    routers = _list_transit_routers(cbn_client, region_id=region_id, cen_id=cen_id, transit_router_name=transit_router_name)
    for router in routers:
        if router.get("TransitRouterName") == transit_router_name:
            transit_router_id = router.get("TransitRouterId")
            if transit_router_id:
                logger.info(f"Get transit router {transit_router_name}: {transit_router_id}")
                return transit_router_id

    response = cbn_client.create_transit_router(
        cbn_models.CreateTransitRouterRequest(
            cen_id=cen_id,
            region_id=region_id,
            transit_router_name=transit_router_name,
            transit_router_description=description,
            support_multicast=False,
            tag=_build_named_tags(cbn_models.CreateTransitRouterRequestTag, transit_router_name),
        )
    )
    transit_router_id = response.body.transit_router_id
    if not transit_router_id:
        raise RuntimeError(f"Create transit router {transit_router_name} returned empty transit_router_id")

    wait_until(
        lambda: _has_transit_router(cbn_client, region_id=region_id, cen_id=cen_id, transit_router_id=transit_router_id),
        timeout=300,
        retry_interval=5,
    )
    logger.info(f"Created transit router {transit_router_name}: {transit_router_id}")
    return transit_router_id


def _ensure_vpc_attachment(
    client,
    *,
    region_id: str,
    cen_id: str,
    transit_router_id: str,
    vpc_id: str,
    attachment_name: str,
    zone_mapping: cbn_models.CreateTransitRouterVpcAttachmentRequestZoneMappings,
) -> str:
    cbn_client = client.build_cbn()
    attachments = _list_vpc_attachments(
        cbn_client,
        region_id=region_id,
        cen_id=cen_id,
        transit_router_id=transit_router_id,
        vpc_id=vpc_id,
    )
    for attachment in attachments:
        if attachment.get("VpcId") == vpc_id:
            attachment_id = attachment.get("TransitRouterAttachmentId")
            if attachment_id:
                logger.info(f"Get transit router VPC attachment {attachment_name}: {attachment_id}")
                return attachment_id

    response = cbn_client.create_transit_router_vpc_attachment(
        cbn_models.CreateTransitRouterVpcAttachmentRequest(
            auto_publish_route_enabled=True,
            cen_id=cen_id,
            region_id=region_id,
            transit_router_attachment_name=attachment_name,
            transit_router_attachment_description=f"Attach VPC {vpc_id} to enterprise network",
            transit_router_id=transit_router_id,
            vpc_id=vpc_id,
            zone_mappings=[zone_mapping],
            tag=_build_named_tags(cbn_models.CreateTransitRouterVpcAttachmentRequestTag, attachment_name),
        )
    )
    attachment_id = response.body.transit_router_attachment_id
    if not attachment_id:
        raise RuntimeError(f"Create VPC attachment {attachment_name} returned empty attachment_id")

    wait_until(
        lambda: _has_vpc_attachment(
            cbn_client,
            region_id=region_id,
            cen_id=cen_id,
            transit_router_id=transit_router_id,
            attachment_id=attachment_id,
        ),
        timeout=300,
        retry_interval=5,
    )
    logger.info(f"Created transit router VPC attachment {attachment_name}: {attachment_id}")
    return attachment_id


def _ensure_peer_attachment(
    client,
    *,
    region_id: str,
    cen_id: str,
    transit_router_id: str,
    peer_transit_router_id: str,
    peer_region_id: str,
    attachment_name: str,
    bandwidth: int,
    bandwidth_type: str,
) -> str:
    cbn_client = client.build_cbn()
    attachments = _list_peer_attachments(
        cbn_client,
        region_id=region_id,
        cen_id=cen_id,
        transit_router_id=transit_router_id,
    )
    for attachment in attachments:
        if (
            attachment.get("PeerTransitRouterId") == peer_transit_router_id
            and attachment.get("PeerTransitRouterRegionId") == peer_region_id
        ):
            attachment_id = attachment.get("TransitRouterAttachmentId")
            if attachment_id:
                logger.info(f"Get transit router peer attachment {attachment_name}: {attachment_id}")
                return attachment_id

    response = cbn_client.create_transit_router_peer_attachment(
        cbn_models.CreateTransitRouterPeerAttachmentRequest(
            auto_publish_route_enabled=True,
            bandwidth=bandwidth,
            bandwidth_type=bandwidth_type,
            cen_id=cen_id,
            peer_transit_router_id=peer_transit_router_id,
            peer_transit_router_region_id=peer_region_id,
            region_id=region_id,
            transit_router_attachment_name=attachment_name,
            transit_router_attachment_description=f"Mesh peer {region_id}<->{peer_region_id}",
            transit_router_id=transit_router_id,
            tag=_build_named_tags(cbn_models.CreateTransitRouterPeerAttachmentRequestTag, attachment_name),
        )
    )
    attachment_id = response.body.transit_router_attachment_id
    if not attachment_id:
        raise RuntimeError(f"Create peer attachment {attachment_name} returned empty attachment_id")

    wait_until(
        lambda: _has_peer_attachment(
            cbn_client,
            region_id=region_id,
            cen_id=cen_id,
            transit_router_id=transit_router_id,
            attachment_id=attachment_id,
        ),
        timeout=300,
        retry_interval=5,
    )
    logger.info(f"Created transit router peer attachment {attachment_name}: {attachment_id}")
    return attachment_id


def _ensure_instance_secondary_private_ip(client, *, region_id: str, instance_id: str, fallback_private_ip: str) -> str:
    ecs_client = client.build(region_id)
    network_interface = _get_primary_network_interface(ecs_client, region_id, instance_id)
    if network_interface is None:
        logger.warning(f"Cannot find primary network interface for {region_id}/{instance_id}, fallback to primary private IP")
        return fallback_private_ip

    existing_secondary_ips = _extract_secondary_private_ips(network_interface)
    if existing_secondary_ips:
        return existing_secondary_ips[0]

    network_interface_id = network_interface.get("NetworkInterfaceId")
    if not network_interface_id:
        logger.warning(f"Primary network interface for {region_id}/{instance_id} has no network_interface_id, fallback to primary private IP")
        return fallback_private_ip

    response = ecs_client.assign_private_ip_addresses(
        ecs_models.AssignPrivateIpAddressesRequest(
            region_id=region_id,
            network_interface_id=network_interface_id,
            secondary_private_ip_address_count=1,
        )
    )
    body = response.body.to_map() if response.body else {}
    ip_set = ((body.get("AssignedPrivateIpAddressesSet") or {}).get("PrivateIpSet") or [])
    assigned_ip = ip_set[0] if ip_set else None
    if not assigned_ip:
        refreshed = _get_primary_network_interface(ecs_client, region_id, instance_id)
        if refreshed is None:
            return fallback_private_ip
        refreshed_secondary_ips = _extract_secondary_private_ips(refreshed)
        if refreshed_secondary_ips:
            return refreshed_secondary_ips[0]
        return fallback_private_ip

    logger.info(f"Assigned enterprise-network private IP for {region_id}/{instance_id}: {assigned_ip}")
    return assigned_ip


def _get_primary_network_interface(ecs_client, region_id: str, instance_id: str) -> Optional[dict]:
    response = ecs_client.describe_network_interfaces(
        ecs_models.DescribeNetworkInterfacesRequest(
            region_id=region_id,
            instance_id=instance_id,
            page_size=50,
        )
    )
    body = response.body.to_map() if response.body else {}
    network_interfaces = ((body.get("NetworkInterfaceSets") or {}).get("NetworkInterfaceSet") or [])
    if not network_interfaces:
        return None

    for interface in network_interfaces:
        attachment = interface.get("Attachment") or {}
        if attachment.get("DeviceIndex") == 0:
            return interface

    return network_interfaces[0]


def _extract_secondary_private_ips(network_interface: dict) -> List[str]:
    primary_ip = network_interface.get("PrivateIpAddress")
    private_ip_sets = ((network_interface.get("PrivateIpSets") or {}).get("PrivateIpSet") or [])
    result: List[str] = []
    for item in private_ip_sets:
        private_ip = item.get("PrivateIpAddress")
        is_primary = bool(item.get("Primary"))
        if private_ip and not is_primary and private_ip != primary_ip:
            result.append(private_ip)
    return result


def _describe_cens(cbn_client, cen_name: str) -> List[dict]:
    response = cbn_client.describe_cens(
        cbn_models.DescribeCensRequest(
            filter=[cbn_models.DescribeCensRequestFilter(key="Name", value=[cen_name])],
            page_size=50,
            page_number=1,
        )
    )
    body = response.body.to_map() if response.body else {}
    return ((body.get("Cens") or {}).get("Cen") or [])


def _list_transit_routers(cbn_client, *, region_id: str, cen_id: str, transit_router_name: Optional[str] = None, transit_router_id: Optional[str] = None) -> List[dict]:
    response = cbn_client.list_transit_routers(
        cbn_models.ListTransitRoutersRequest(
            region_id=region_id,
            cen_id=cen_id,
            transit_router_name=transit_router_name,
            transit_router_id=transit_router_id,
            page_number=1,
            page_size=50,
        )
    )
    body = response.body.to_map() if response.body else {}
    return body.get("TransitRouters") or []


def _list_vpc_attachments(cbn_client, *, region_id: str, cen_id: str, transit_router_id: str, vpc_id: Optional[str] = None, attachment_id: Optional[str] = None) -> List[dict]:
    response = cbn_client.list_transit_router_vpc_attachments(
        cbn_models.ListTransitRouterVpcAttachmentsRequest(
            region_id=region_id,
            cen_id=cen_id,
            transit_router_id=transit_router_id,
            vpc_id=vpc_id,
            transit_router_attachment_id=attachment_id,
            max_results=100,
        )
    )
    body = response.body.to_map() if response.body else {}
    return body.get("TransitRouterAttachments") or []


def _list_peer_attachments(cbn_client, *, region_id: str, cen_id: str, transit_router_id: str, attachment_id: Optional[str] = None) -> List[dict]:
    response = cbn_client.list_transit_router_peer_attachments(
        cbn_models.ListTransitRouterPeerAttachmentsRequest(
            region_id=region_id,
            cen_id=cen_id,
            transit_router_id=transit_router_id,
            transit_router_attachment_id=attachment_id,
            max_results=100,
        )
    )
    body = response.body.to_map() if response.body else {}
    return body.get("TransitRouterAttachments") or []


def _has_named_cen(cbn_client, cen_name: str, cen_id: str) -> bool:
    for cen in _describe_cens(cbn_client, cen_name):
        if cen.get("Name") == cen_name and cen.get("CenId") == cen_id:
            return True
    return False


def _has_transit_router(cbn_client, *, region_id: str, cen_id: str, transit_router_id: str) -> bool:
    routers = _list_transit_routers(cbn_client, region_id=region_id, cen_id=cen_id, transit_router_id=transit_router_id)
    return any(router.get("TransitRouterId") == transit_router_id for router in routers)


def _has_vpc_attachment(cbn_client, *, region_id: str, cen_id: str, transit_router_id: str, attachment_id: str) -> bool:
    attachments = _list_vpc_attachments(
        cbn_client,
        region_id=region_id,
        cen_id=cen_id,
        transit_router_id=transit_router_id,
        attachment_id=attachment_id,
    )
    return any(attachment.get("TransitRouterAttachmentId") == attachment_id for attachment in attachments)


def _has_peer_attachment(cbn_client, *, region_id: str, cen_id: str, transit_router_id: str, attachment_id: str) -> bool:
    attachments = _list_peer_attachments(
        cbn_client,
        region_id=region_id,
        cen_id=cen_id,
        transit_router_id=transit_router_id,
        attachment_id=attachment_id,
    )
    return any(attachment.get("TransitRouterAttachmentId") == attachment_id for attachment in attachments)


def _pick_zone_mapping(region_info) -> cbn_models.CreateTransitRouterVpcAttachmentRequestZoneMappings:
    zone = next(iter(sorted(region_info.zones.values(), key=lambda item: item.id)))
    return cbn_models.CreateTransitRouterVpcAttachmentRequestZoneMappings(
        zone_id=zone.id,
        v_switch_id=zone.v_switch_id,
    )


def _build_tags(user_tag: str, *, scope: str) -> List[cbn_models.CreateCenRequestTag]:
    return [
        cbn_models.CreateCenRequestTag(key="managed_by", value="conflux-massive-test"),
        cbn_models.CreateCenRequestTag(key="user_tag", value=user_tag),
        cbn_models.CreateCenRequestTag(key="scope", value=scope),
    ]


def _build_named_tags(tag_cls, name: str):
    return [
        tag_cls(key="managed_by", value="conflux-massive-test"),
        tag_cls(key="name", value=name),
    ]