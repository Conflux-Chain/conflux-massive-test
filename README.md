# Conflux Massive Test

A distributed testing framework for Conflux blockchain nodes across cloud providers.

## Quick Start

### Prerequisites

- Python 3.11+
- Aliyun account with ECS access (or AWS for legacy runs)
- SSH private key available locally (optional): set `SSH_KEY_PATH` to point to your SSH private key. If `SSH_KEY_PATH` is not set and the repository contains `keys/ssh-key.pem`, that key will be used automatically.

### Installation

```bash
pip install -r requirements.txt
```

### Provisioning with `cloud_provisioner`

1. Configure `request_config.toml` (see `request_config.toml` for a full example). Example snippet:

```toml
[aliyun]
provider = "aliyun"
default_user_name = "root"
user_tag = "myname"
image_name = "conflux-docker-base"
ssh_key_path = "./keys/ssh-key.pem"

[[aliyun.regions]]
name = "ap-southeast-3"
count = 10

[[aliyun.instance_types]]
name = "ecs.g8i.2xlarge"
nodes = 1

[aws]
provider = "aws"
default_user_name = "ubuntu"
user_tag = "myname"
image_name = "massive-test-seed"
ssh_key_path = "./keys/ssh-key.pem"

[[aws.regions]]
name = "us-west-2"
count = 10

[[aws.instance_types]]
name = "m6i.2xlarge"
nodes = 1
```

Credentials: Aliyun credentials are read from environment variables (`ALI_ACCESS_KEY_ID` / `ALI_ACCESS_KEY_SECRET`, or from `.env`). AWS credentials are read from the standard AWS environment/credentials configuration.

2. Provision servers (creates/ensures network infra and instances; writes `hosts.json` by default):

```bash
python -m cloud_provisioner.create_instances --allow-create --request-config request_config.toml --output-json hosts.json
```

- `--allow-create` allows creating missing network infra (VPC, subnets, security groups).
- `--network-only` will only ensure network infra and will skip instance creation.

3. Run the simulation (reads the `hosts.json` inventory produced by the provisioner):

```bash
python -m remote_simulation --host-spec hosts.json --log-path logs/<timestamp>
```

4. Cleanup cloud resources tagged by `request_config.toml`'s `user_tag`:

```bash
python -m cloud_provisioner.cleanup_instances --user-prefix <your_user_tag_prefix> --yes
```

Note: `cleanup_instances` filters resources by the project common tag and the `user_tag` prefix. Passing an empty `--user-prefix ""` is destructive (matches all tagged instances).

---

## Project Structure

```
├── remote_simulation/       # Core simulation logic (launch, topology, block gen, log collection)
├── cloud_provisioner/       # Provisioning & management (create_instances, cleanup_instances, providers)
├── request_config.toml      # Cloud provisioning configuration (regions, types, image name, ssh key)
├── auxiliary/               # Remote helper scripts used during image build and log collection
└── logs/                    # Collected simulation logs
```

---

