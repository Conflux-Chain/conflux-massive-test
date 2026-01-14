from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Dict, List, Tuple

import pytest

from conflux_deployer.configs.types import (
    CloudCredentials,
    CloudProvider,
    DeploymentConfig,
    InstanceInfo,
    InstanceState,
    RegionConfig,
)
from conflux_deployer.resource_cleanup.manager import ResourceCleanupManager
from conflux_deployer.configs.loader import StateManager


@dataclass
class FakeProvider:
    provider: CloudProvider
    region_id: str

    stopped: List[List[str]]
    terminated: List[List[str]]
    name_prefix_queries: List[str]

    instances_by_prefix: Dict[str, List[InstanceInfo]]

    def __init__(self, provider: CloudProvider, region_id: str):
        self.provider = provider
        self.region_id = region_id
        self.stopped = []
        self.terminated = []
        self.name_prefix_queries = []
        self.instances_by_prefix = {}

    def stop_instances(self, instance_ids: List[str]) -> Dict[str, bool]:
        self.stopped.append(list(instance_ids))
        return {i: True for i in instance_ids}

    def terminate_instances(self, instance_ids: List[str]) -> Dict[str, bool]:
        self.terminated.append(list(instance_ids))
        return {i: True for i in instance_ids}

    def list_instances_by_name_prefix(self, name_prefix: str) -> List[InstanceInfo]:
        self.name_prefix_queries.append(name_prefix)
        return self.instances_by_prefix.get(name_prefix, [])


class FakeFactory:
    def __init__(self):
        self.providers: Dict[Tuple[CloudProvider, str], FakeProvider] = {}

    def get_provider(self, provider: CloudProvider, credentials: CloudCredentials, region_id: str):
        key = (provider, region_id)
        if key not in self.providers:
            self.providers[key] = FakeProvider(provider, region_id)
        return self.providers[key]


def _make_config(tmp_path) -> DeploymentConfig:
    creds = {
        CloudProvider.AWS: CloudCredentials(access_key_id="x", secret_access_key="y"),
        CloudProvider.ALIBABA: CloudCredentials(access_key_id="a", secret_access_key="b"),
    }

    regions = [
        RegionConfig(
            provider=CloudProvider.AWS,
            region_id="us-west-2",
            location_name="us-west-2",
            instance_count=1,
            instance_type="m5.large",
            nodes_per_instance=1,
        ),
        RegionConfig(
            provider=CloudProvider.ALIBABA,
            region_id="cn-hangzhou",
            location_name="cn-hangzhou",
            instance_count=1,
            instance_type="ecs.g6.large",
            nodes_per_instance=1,
        ),
    ]

    return DeploymentConfig(
        deployment_id="deploy-abc",
        instance_name_prefix="mytest",
        credentials=creds,
        regions=regions,
        state_file_path=str(tmp_path / "deployment_state.json"),
    )


def test_force_cleanup_mode_state_file(monkeypatch, tmp_path):
    config = _make_config(tmp_path)

    # Write a state file; instances inside should NOT be used for deletion.
    state_file = tmp_path / "state.json"
    state_payload = {
        "deployment_id": config.deployment_id,
        "phase": "running",
        "created_at": "now",
        "updated_at": "now",
        "instances": [
            InstanceInfo(
                instance_id="i-aws-1",
                provider=CloudProvider.AWS,
                region_id="us-west-2",
                location_name="us-west-2",
                instance_type="m5.large",
                state=InstanceState.RUNNING,
                name="conflux-deployer-mytest-deploy-abc-us-west-2-0",
            ).to_dict(),
            InstanceInfo(
                instance_id="i-ali-1",
                provider=CloudProvider.ALIBABA,
                region_id="cn-hangzhou",
                location_name="cn-hangzhou",
                instance_type="ecs.g6.large",
                state=InstanceState.RUNNING,
                name="conflux-deployer-mytest-deploy-abc-cn-hangzhou-0",
            ).to_dict(),
        ],
        "nodes": [],
        "images": {},
        "security_groups": {},
        "errors": [],
        "test_results": {},
    }
    state_file.write_text(json.dumps(state_payload), encoding="utf-8")

    fake_factory = FakeFactory()

    # Patch the cleanup manager instance to use our fake factory
    cleanup_mgr = ResourceCleanupManager(config, StateManager(str(tmp_path / "unused.json")))
    cleanup_mgr.factory = fake_factory

    # Mock "get all servers" to return both matching and non-matching instances.
    from conflux_deployer.utils.naming import build_instance_name_prefix

    aws_prefix = build_instance_name_prefix(
        deployment_id=config.deployment_id or "",
        region_id="us-west-2",
        user_prefix=config.instance_name_prefix,
    )
    ali_prefix = build_instance_name_prefix(
        deployment_id=config.deployment_id or "",
        region_id="cn-hangzhou",
        user_prefix=config.instance_name_prefix,
    )

    aws_all = [
        InstanceInfo(
            instance_id="i-aws-match",
            provider=CloudProvider.AWS,
            region_id="us-west-2",
            location_name="us-west-2",
            instance_type="m5.large",
            state=InstanceState.RUNNING,
            name=f"{aws_prefix}-0",
        ),
        InstanceInfo(
            instance_id="i-aws-other",
            provider=CloudProvider.AWS,
            region_id="us-west-2",
            location_name="us-west-2",
            instance_type="m5.large",
            state=InstanceState.RUNNING,
            name="some-other-prefix-0",
        ),
    ]

    ali_all = [
        InstanceInfo(
            instance_id="i-ali-match",
            provider=CloudProvider.ALIBABA,
            region_id="cn-hangzhou",
            location_name="cn-hangzhou",
            instance_type="ecs.g6.large",
            state=InstanceState.RUNNING,
            name=f"{ali_prefix}-0",
        ),
        InstanceInfo(
            instance_id="i-ali-other",
            provider=CloudProvider.ALIBABA,
            region_id="cn-hangzhou",
            location_name="cn-hangzhou",
            instance_type="ecs.g6.large",
            state=InstanceState.RUNNING,
            name="totally-unrelated",
        ),
    ]

    def fake_list_all_instances(cloud):
        if getattr(cloud, "provider", None) == CloudProvider.AWS:
            return aws_all
        return ali_all

    monkeypatch.setattr(cleanup_mgr, "_list_all_instances_for_cleanup", fake_list_all_instances)

    results = cleanup_mgr.force_stop_and_delete_instances_from_state_file(str(state_file))

    assert results == {"i-aws-match": True, "i-ali-match": True}

    aws = fake_factory.providers[(CloudProvider.AWS, "us-west-2")]
    ali = fake_factory.providers[(CloudProvider.ALIBABA, "cn-hangzhou")]

    assert aws.stopped == [["i-aws-match"]]
    assert aws.terminated == [["i-aws-match"]]

    assert ali.stopped == [["i-ali-match"]]
    assert ali.terminated == [["i-ali-match"]]


def test_force_cleanup_mode_pattern(monkeypatch, tmp_path):
    config = _make_config(tmp_path)

    fake_factory = FakeFactory()

    cleanup_mgr = ResourceCleanupManager(config, StateManager(config.state_file_path))
    cleanup_mgr.factory = fake_factory

    # Populate matches for each region prefix
    from conflux_deployer.utils.naming import build_instance_name_prefix

    aws_prefix = build_instance_name_prefix(
        deployment_id=config.deployment_id or "",
        region_id="us-west-2",
        user_prefix=config.instance_name_prefix,
    )
    ali_prefix = build_instance_name_prefix(
        deployment_id=config.deployment_id or "",
        region_id="cn-hangzhou",
        user_prefix=config.instance_name_prefix,
    )

    fake_factory.providers[(CloudProvider.AWS, "us-west-2")] = FakeProvider(CloudProvider.AWS, "us-west-2")
    fake_factory.providers[(CloudProvider.ALIBABA, "cn-hangzhou")] = FakeProvider(CloudProvider.ALIBABA, "cn-hangzhou")

    fake_factory.providers[(CloudProvider.AWS, "us-west-2")].instances_by_prefix[aws_prefix] = [
        InstanceInfo(
            instance_id="i-aws-2",
            provider=CloudProvider.AWS,
            region_id="us-west-2",
            location_name="us-west-2",
            instance_type="m5.large",
            state=InstanceState.RUNNING,
            name=f"{aws_prefix}-0",
        )
    ]
    fake_factory.providers[(CloudProvider.ALIBABA, "cn-hangzhou")].instances_by_prefix[ali_prefix] = [
        InstanceInfo(
            instance_id="i-ali-2",
            provider=CloudProvider.ALIBABA,
            region_id="cn-hangzhou",
            location_name="cn-hangzhou",
            instance_type="ecs.g6.large",
            state=InstanceState.RUNNING,
            name=f"{ali_prefix}-0",
        )
    ]

    results = cleanup_mgr.force_stop_and_delete_instances_by_naming_pattern()

    assert results == {"i-aws-2": True, "i-ali-2": True}

    aws = fake_factory.providers[(CloudProvider.AWS, "us-west-2")]
    ali = fake_factory.providers[(CloudProvider.ALIBABA, "cn-hangzhou")]

    assert aws.name_prefix_queries == [aws_prefix]
    assert ali.name_prefix_queries == [ali_prefix]

    assert aws.stopped == [["i-aws-2"]]
    assert aws.terminated == [["i-aws-2"]]

    assert ali.stopped == [["i-ali-2"]]
    assert ali.terminated == [["i-ali-2"]]


def test_force_cleanup_mode_state_file_dry_run(monkeypatch, tmp_path):
    config = _make_config(tmp_path)

    state_file = tmp_path / "state.json"
    state_payload = {
        "deployment_id": config.deployment_id,
        "phase": "running",
        "created_at": "now",
        "updated_at": "now",
        "instances": [],
        "nodes": [],
        "images": {},
        "security_groups": {},
        "errors": [],
        "test_results": {},
    }
    state_file.write_text(json.dumps(state_payload), encoding="utf-8")

    fake_factory = FakeFactory()
    cleanup_mgr = ResourceCleanupManager(config, StateManager(str(tmp_path / "unused.json")))
    cleanup_mgr.factory = fake_factory

    from conflux_deployer.utils.naming import build_instance_name_prefix

    aws_prefix = build_instance_name_prefix(
        deployment_id=config.deployment_id or "",
        region_id="us-west-2",
        user_prefix=config.instance_name_prefix,
    )
    ali_prefix = build_instance_name_prefix(
        deployment_id=config.deployment_id or "",
        region_id="cn-hangzhou",
        user_prefix=config.instance_name_prefix,
    )

    def fake_list_all_instances(cloud):
        if getattr(cloud, "provider", None) == CloudProvider.AWS:
            return [
                InstanceInfo(
                    instance_id="i-aws-match",
                    provider=CloudProvider.AWS,
                    region_id="us-west-2",
                    location_name="us-west-2",
                    instance_type="m5.large",
                    state=InstanceState.RUNNING,
                    name=f"{aws_prefix}-0",
                )
            ]
        return [
            InstanceInfo(
                instance_id="i-ali-match",
                provider=CloudProvider.ALIBABA,
                region_id="cn-hangzhou",
                location_name="cn-hangzhou",
                instance_type="ecs.g6.large",
                state=InstanceState.RUNNING,
                name=f"{ali_prefix}-0",
            )
        ]

    monkeypatch.setattr(cleanup_mgr, "_list_all_instances_for_cleanup", fake_list_all_instances)

    results = cleanup_mgr.force_stop_and_delete_instances_from_state_file(
        str(state_file),
        dry_run=True,
    )

    assert results == {"i-aws-match": True, "i-ali-match": True}

    aws = fake_factory.providers[(CloudProvider.AWS, "us-west-2")]
    ali = fake_factory.providers[(CloudProvider.ALIBABA, "cn-hangzhou")]
    assert aws.stopped == []
    assert aws.terminated == []
    assert ali.stopped == []
    assert ali.terminated == []


def test_force_cleanup_mode_pattern_dry_run(monkeypatch, tmp_path):
    config = _make_config(tmp_path)

    fake_factory = FakeFactory()
    cleanup_mgr = ResourceCleanupManager(config, StateManager(config.state_file_path))
    cleanup_mgr.factory = fake_factory

    from conflux_deployer.utils.naming import build_instance_name_prefix

    aws_prefix = build_instance_name_prefix(
        deployment_id=config.deployment_id or "",
        region_id="us-west-2",
        user_prefix=config.instance_name_prefix,
    )
    ali_prefix = build_instance_name_prefix(
        deployment_id=config.deployment_id or "",
        region_id="cn-hangzhou",
        user_prefix=config.instance_name_prefix,
    )

    fake_factory.providers[(CloudProvider.AWS, "us-west-2")] = FakeProvider(CloudProvider.AWS, "us-west-2")
    fake_factory.providers[(CloudProvider.ALIBABA, "cn-hangzhou")] = FakeProvider(CloudProvider.ALIBABA, "cn-hangzhou")

    fake_factory.providers[(CloudProvider.AWS, "us-west-2")].instances_by_prefix[aws_prefix] = [
        InstanceInfo(
            instance_id="i-aws-2",
            provider=CloudProvider.AWS,
            region_id="us-west-2",
            location_name="us-west-2",
            instance_type="m5.large",
            state=InstanceState.RUNNING,
            name=f"{aws_prefix}-0",
        )
    ]
    fake_factory.providers[(CloudProvider.ALIBABA, "cn-hangzhou")].instances_by_prefix[ali_prefix] = [
        InstanceInfo(
            instance_id="i-ali-2",
            provider=CloudProvider.ALIBABA,
            region_id="cn-hangzhou",
            location_name="cn-hangzhou",
            instance_type="ecs.g6.large",
            state=InstanceState.RUNNING,
            name=f"{ali_prefix}-0",
        )
    ]

    results = cleanup_mgr.force_stop_and_delete_instances_by_naming_pattern(dry_run=True)
    assert results == {"i-aws-2": True, "i-ali-2": True}

    aws = fake_factory.providers[(CloudProvider.AWS, "us-west-2")]
    ali = fake_factory.providers[(CloudProvider.ALIBABA, "cn-hangzhou")]
    assert aws.stopped == []
    assert aws.terminated == []
    assert ali.stopped == []
    assert ali.terminated == []
