# Conflux Massive Test

A distributed testing framework for Conflux blockchain nodes across cloud providers.

## Quick Start

### Prerequisites

- Python 3.10+
- Aliyun account with ECS access (or AWS for legacy runs)

### Installation

```bash
pip install -r requirements.txt
```

### Running on Aliyun

1. Configure `instance-region.json` (see configuration section below).

2. Provision servers (writes `ali_servers.json` to the repo root and an inventory under `logs/{timestamp}`):

```bash
./.venv/bin/python -m ali_instances.create_servers --config instance-region.json --hardware config/hardware.json
```

3. Run the simulation (reads `ali_servers.json`, does NOT create or delete instances):

```bash
python remote_simulate_ali.py
```

4. Cleanup the specific instances created earlier with the inventory JSON:

```bash
./.venv/bin/python -m ali_instances.cleanup_resources --instances-json ali_servers.json
```

Or, to cleanup all tagged resources across regions (legacy behavior):

```bash
./.venv/bin/python -m ali_instances.cleanup_resources
```

Notes:
- The provisioning step tags resources and writes a machine-readable inventory (`ali_servers.json`) which `remote_simulate_ali.py` uses to run experiments.
- Logs for a run are stored under `logs/{timestamp}` as created by the provisioning step (or the experiment will create a `logs/{timestamp}` folder if none is specified).

### Running on AWS (Legacy)

```bash
python remote_simulate.py
```

Requires pre-created instances stored in `instances.pkl`.

---

## instance-region.json Configuration

### Aliyun

```json
{
  "aliyun": [
    {
      "access_key_id": "",
      "access_key_secret": "",
      "user_tag": "myname",
      "regions": [
        {
          "name": "ap-southeast-3",
          "count": 10
        }
      ]
    }
  ]
}
```

#### Account fields

| Field | Required | Description |
|-------|----------|-------------|
| `access_key_id` | No | If empty, uses `ALI_ACCESS_KEY_ID` env var |
| `access_key_secret` | No | If empty, uses `ALI_ACCESS_KEY_SECRET` env var |
| `user_tag` | No | Defaults to `chenxinghao`. Used for resource naming |
| `type` | No | Preferred instance types (see below) |
| `regions` | Yes | List of regions to deploy |

#### Region fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Region ID, e.g. `ap-southeast-3` |
| `count` | Yes | **Total number of Conflux nodes** (instead of server instances) to start in this region |
| `image` | No | Custom AMI id. If omitted, uses pre-built `conflux-docker-base` |
| `security_group_id` | No | Existing security group. Created if omitted |
| `zones` | No | Preferred availability zones |

#### Instance type selection

```json
{
  "type": [
    {"name": "ecs.g8i.xlarge", "nodes": 1},
    {"name": "ecs.g7.xlarge"}
  ]
}
```

 - `name`: Instance type name
 - `nodes`: Nodes per host. Falls back to `config/hardware.json`, then `1`

Rules:
1. `regions[].count` is the total **node** count (not instance count).
2. `regions[].type` is optional. If omitted, the account-level `type` list is used.
3. The system tries instance types in order. It keeps creating instances of the first type with stock until the region’s node count is satisfied; then it falls back to the next type.
4. Nodes-per-host determines how many nodes run on each instance. The instance count is $\lceil \frac{\text{count}}{\text{nodes_per_host}} \rceil$.

### Tags & Resource Cleanup

All Aliyun resources are tagged with:
- `conflux-massive-test=true`
- `user=<user_tag>`

Resource names use prefix `conflux-massive-test-<user_tag>`.

**Resources are NOT automatically cleaned up after the simulation. For manual cleanup:**

```bash
./.venv/bin/python -m ali_instances.cleanup_resources
```

---

## Project Structure

```
├── remote_simulate_ali.py   # Main entry for Aliyun simulation
├── remote_simulate.py       # Legacy AWS simulation
├── instance-region.json     # Cloud configuration
├── ali_instances/           # Aliyun provisioning & management
│   ├── multi_region_runner.py    # Multi-region provisioning
│   ├── instance_prep.py    # Instance lifecycle management
│   ├── image_build.py      # Server image creation
│   └── cleanup_resources.py
├── remote_simulation/       # Core simulation logic
│   ├── launch_conflux_node.py
│   ├── network_connector.py
│   ├── block_generator.py
│   ├── tools.py
│   └── ssh_utils.py        # Async SSH utilities
├── config/
│   └── hardware.json       # Default nodes-per-host by instance type
└── logs/                   # Collected simulation logs
```

---

## Advanced Usage

### Building a custom server image

The simulation requires a pre-built server image with Docker installed. If the image doesn't exist in your region:

```python
from ali_instances.image_build import create_server_image, prepare_docker_server_image
from ali_instances.config import EcsConfig

cfg = EcsConfig(region_id="ap-southeast-3")
cfg.base_image_id = "<ubuntu-image-id>"
image_id = create_server_image(cfg, prepare_fn=prepare_docker_server_image)
```

Then add the image ID to your region config:

```json
{
  "name": "ap-southeast-3",
  "count": 10,
  "image": "<image-id>"
}
```
