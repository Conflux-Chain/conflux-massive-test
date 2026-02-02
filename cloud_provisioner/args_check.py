from loguru import logger
import tomllib
import sys

from cloud_provisioner.create_instances.provision_config import ProvisionConfig
from cloud_provisioner.create_instances.instance_config import DEFAULT_COMMON_TAG_KEY, DEFAULT_COMMON_TAG_VALUE


def check_user_prefix_with_config_file(config_file: str, user_prefix: str, assume_yes: bool):
    try:
        with open(config_file, "rb") as f:
            data = tomllib.load(f)
            config = ProvisionConfig(**data)
    except FileNotFoundError:
        logger.error(f"{config_file} not found in cwd, aborting")
        sys.exit(1)

    mismatches = []

    if not config.aliyun.user_tag.startswith(user_prefix):
        mismatches.append(("aliyun", config.aliyun.user_tag))

    if not config.aws.user_tag.startswith(user_prefix):
        mismatches.append(("aws", config.aws.user_tag))

    if mismatches:
        logger.warning(f"Provided user prefix '{user_prefix}' is not a prefix of the following user_tag(s) from config toml:")
        for prov, tag in mismatches:
            logger.warning(f" - {prov}: '{tag}'")

        if not assume_yes:
            resp = input("Proceed anyway? [y/N]: ").strip().lower()
            if resp not in ("y", "yes"):
                logger.info("Aborting cleanup due to user cancellation")
                sys.exit(1)
        else:
            logger.info("Proceeding despite mismatched prefix due to --yes flag")


def check_empty_user_prefix(user_prefix: str, assume_yes: bool, warning_msg: str):
    # Destructive confirmation when empty prefix is provided (matches all instances subject to common tag)
    # python -m cloud_provisioner.cleanup_instances --user-prefix ""

    if user_prefix == "":
        logger.warning(
            warning_msg
        )
        if not assume_yes:
            resp = input("Proceed anyway? [y/N]: ").strip().lower()
            if resp not in ("y", "yes"):
                logger.info("Aborting cleanup due to user cancellation")
                sys.exit(1)
        else:
            logger.info("Proceeding with empty prefix due to --yes flag")
