# Conflux Massive Test

This repository provisions large numbers of cloud servers across multiple regions, launches Conflux-Rust on those servers, runs a global-scale experiment, collects the resulting logs, and then tears the cloud resources down.

The normal manual workflow has three stages:

1. Create or reuse cloud network infrastructure and launch instances.
2. Run the remote simulation against the generated host inventory and collect logs.
3. Clean up the tagged cloud instances after the run.

## Prerequisites

You must have Python >= 3.12 installed, valid cloud credentials for any providers you will target, and an SSH private key corresponding to the public key baked into the server image. Create a request configuration file (usually by copying `request_config.example.toml`) and set the `USER_TAG_PREFIX` environment variable or place it in `.env`. Once those items are in place, install the Python dependencies:

```bash
pip install -r requirements.txt
```

## Configure A Run

Copy the example request file and adjust it for your test:

```bash
cp request_config.example.toml request_config.toml
```

The most important fields in `request_config.toml` are `user_tag` (the tag applied to instances and later used for cleanup), `image_name` (name of the pre‑built cloud image containing the runtime), `ssh_key_path` (local path to the SSH private key), plus the per‑provider `[[<provider>.regions]]` blocks that list regions and counts and `[[<provider>.instance_types]]` blocks that name instance types and nodes-per-host.

The provisioner validates that each configured `user_tag` starts with `USER_TAG_PREFIX`. That check is a safety guard to reduce accidental deletion of someone else's resources later.

## Server Images

This repository expects cloud instances to boot from a custom server image rather than from a plain Ubuntu image every time. The image-building flow is implemented separately for Tencent Cloud, AWS, and Aliyun:

- `auxiliary/tencent_instances/image_build.py`
- `auxiliary/aws_instances/image_build.py`
- `auxiliary/ali_instances/image_build.py`

All three flows follow the same high‑level pattern:

1. Pick a public Ubuntu base image.
2. Launch a temporary builder instance from that base.
3. SSH into the builder and run a preparation script.
4. Snapshot or stop the builder and create a reusable private image.
5. Delete the temporary instance.
 
 
### What Gets Baked Into The Server Image

The builder scripts install a minimal runtime needed for large-scale simulation. The preparation scripts are:

- `scripts/remote/prepare_docker_server_image.sh`
- `scripts/remote/prepare_docker_server_image_tencent.sh`

These scripts perform the following tasks on the builder instance:

1. Install Docker and support tools (`curl`, `p7zip-full`).
2. Make `7zz` available (symlinking to `7z` if necessary).
3. Start Docker and enable it at boot.
4. Launch a local registry on port 5000.
5. Pull `lylcx2007/conflux-node:latest` from Docker Hub.
6. Tag that image as `conflux-node:base` and `localhost:5000/conflux-node:base`.
7. Push `conflux-node:base` into the local registry.

So the cloud image does not just contain Docker itself. It also contains a ready-to-run private registry layout and a preloaded copy of the Conflux node Docker image in that local registry.

### Cloud-Specific Image Creation

Each provider uses its own Python script to build the image, but the logic is largely identical: start from a public Ubuntu base, boot a temporary "builder" VM, run one of the preparation scripts (`auxiliary/tencent_instances/image_build.py`, `auxiliary/aws_instances/image_build.py` or `auxiliary/ali_instances/image_build.py`), and snapshot the result as a private image. The preparation script installs Docker, a local registry populated with the Conflux node image, and whatever tooling is needed for the simulation. Once the snapshot is taken the builder instance is torn down.

The three image‑build helpers simply implement the cloud‑specific API calls necessary to create and later copy the image (Aliyun also supports cross‑region copying). You can run them directly if you need to rebuild or refresh the server image on a particular platform.
### How Docker Images Are Distributed At Simulation Start

At the beginning of a simulation, `remote_simulation/__main__.py` calls `prepare_images_by_zone` from `remote_simulation/image_prepare.py` before launching any Conflux nodes.

This stage distributes the Docker image across the fleet in a zone-aware way:

1. Hosts are grouped by availability zone.
2. Within each zone, hosts are sorted by private IP.
3. The first host in that ordered list becomes the seed host for the zone.
4. The seed host runs `scripts/remote/cfx_pull_image_from_dockerhub_and_push_local.sh`.
5. That script pulls `lylcx2007/conflux-node:latest` from Docker Hub, tags it locally as `conflux-node:latest`, and pushes `conflux-node:base` into the host's own registry on `localhost:5000`.
6. Other hosts in the same zone run `scripts/remote/cfx_pull_image_from_registry_and_push_local.sh`, pointing at the nearest already prepared ancestor host.
7. Each receiving host configures Docker to trust the upstream registry as an insecure private registry, pulls `conflux-node:base` from that peer, retags it as `conflux-node:latest`, and republishes it into its own local registry on `localhost:5000`.

The result is a tree-shaped fan-out per zone:

- One host per zone downloads from Docker Hub.
- Other hosts preferentially fetch from a nearby peer inside the same zone.
- Each host becomes another local distribution point after it succeeds.
- If pulling from a peer fails, the code falls back to Docker Hub for that host.

This is why `launch_remote_nodes` is called with `pull_docker_image=False` in the main simulation path. The image distribution step has already staged `conflux-node:latest` on each machine, so the node-launch step does not need every host to hit Docker Hub independently.

## Massive Test Workflow

### 1. Create instances and generate `hosts.json`

Run the provisioner first with `--allow-create` when the required VPC, subnet, security group, or other network infrastructure may not exist yet:

```bash
python -m cloud_provisioner.create_instances \
	--allow-create \
	--request-config request_config.toml \
	--output-json hosts.json
```

What this step does:

1. Read the multi-cloud topology request from `request_config.toml`.
2. Check that the configured `user_tag` values match `USER_TAG_PREFIX`.
3. Ensure cloud network infrastructure exists.
4. Launch instances in the requested regions.
5. Attempt to backfill node shortfalls into healthy regions unless disabled.
6. Write the final instance inventory to `hosts.json`.

If the network infrastructure already exists, you can omit `--allow-create` and rerun the same command to launch instances using the existing network setup.

This command prepares infrastructure and writes the inventory. It does not run the blockchain experiment itself.

### 2. Run the remote simulation and collect logs

After `hosts.json` exists, run the simulation:

```bash
python -m remote_simulation \
	--host-spec hosts.json \
	--log-path logs/$(date +%Y%m%d%H%M%S) \
	--num-blocks 2000
```

What this step does:

1. Load all hosts from `hosts.json`.
2. Copy `hosts.json` into the run directory for reproducibility.
3. Generate a Conflux node config for the total number of nodes.
4. Prepare Docker images by zone.
5. Launch Conflux-Rust nodes on the remote hosts.
6. Build a random peer topology and connect the nodes.
7. Start transaction generation and block production.
8. Wait for synchronization as far as possible.
9. Stop nodes and collect per-node logs into the run directory.

The repository also includes `one_click.sh`, which wraps provisioning, `remote_simulation`, cleanup, and several analyzer steps into a single script. The manual flow above is the clearest way to understand and control a run.

### 3. Clean up cloud instances

When the run is finished, delete the instances by tag prefix:

```bash
python -m cloud_provisioner.cleanup_instances -u <user_tag_prefix>
```

Example:

```bash
python -m cloud_provisioner.cleanup_instances -u lichenxing-alpha-5
```

This cleanup command only deletes instances that match both:

- The repository's common project tag.
- A `user_tag` beginning with the supplied prefix.

Be careful with broad prefixes. An empty prefix matches every instance carrying the common project tag.

## Major Command-Line Flags

### `python -m cloud_provisioner.create_instances`

`-c`, `--request-config`: path to the TOML request config (default `./request_config.toml`).

`-o`, `--output-json`: output path for the generated host inventory (default `./hosts.json`).

`--allow-create`: permit creation of missing network infrastructure.

`--network-only`: provision networking but skip instance creation and `hosts.json`.

`--no-backfill`: disable cross-region backfill when some regions cannot satisfy the requested host count.

Typical usage:

```bash
python -m cloud_provisioner.create_instances --allow-create -c request_config.toml -o hosts.json
```

### `python -m remote_simulation`

- `-s`, `--host-spec`: path to the host inventory JSON. Default: `./hosts.json`.
- `-l`, `--log-path`: output directory for the run. Default: `logs/YYYYMMDDHHMMSS`.
- `-b`, `--num-blocks`: number of blocks to produce during the experiment. Default: `2000`.

Typical usage:

```bash
python -m remote_simulation -s hosts.json -l logs/20260310120000 -b 2000
```

### `python -m cloud_provisioner.cleanup_instances`

- `-u`, `--user-prefix`: required tag prefix used to select instances for deletion.
- `-c`, `--config`: request config used for a safety check against the supplied prefix. Default: `request_config.toml`.
- `--no-check`: skip the config-prefix consistency check.
- `-y`, `--yes`: skip interactive confirmations.

Typical usage:

```bash
python -m cloud_provisioner.cleanup_instances -u lichenxing-alpha-5 -y
```

## `hosts.json`

`hosts.json` is the bridge between provisioning and simulation. It is generated by `cloud_provisioner.create_instances` and consumed by `remote_simulation`.

It is a JSON array. Each element describes one cloud host with fields such as:

- `ip`: public IP address used for SSH and RPC access.
- `nodes_per_host`: how many Conflux nodes should run on that machine.
- `ssh_user`: SSH login user, such as `root` or `ubuntu`.
- `ssh_key_path`: private key path used by the automation.
- `provider`: cloud provider name, for example `aliyun`, `aws`, or `tencent`.
- `region`: cloud region.
- `zone`: cloud availability zone.
- `instance_id`: provider-specific instance ID.
- `private_ip`: host private IP address.

This file represents the actual allocated fleet, not just the requested fleet. If a region partially fails and another region backfills the missing capacity, `hosts.json` reflects the final placement that was really launched.

## Run Directory And Log Files

Each simulation run writes to one directory under `logs/`, usually named with a timestamp such as `logs/20260310120000`.

The top-level run directory typically contains:

- `remote_simulate.log`: controller log for the orchestration process.
- `hosts.json`: copy of the host inventory used for that run.
- `config.toml`: generated Conflux configuration used by the launched nodes.
- `nodes/`: per-node collected logs.

### `remote_simulate.log`

This is the control-plane log for the experiment. It records events such as:

- The list of loaded hosts.
- Which node was chosen as the observed sample node.
- Topology construction and peer connection progress.
- Synchronization warnings and timeouts.
- Block generation progress and failures.
- Goodput and log collection progress.

When a run behaves unexpectedly, this is the first file to inspect.

### `nodes/`

Inside `nodes/`, each remote node gets its own directory, named with the host IP and node index, for example `nodes/1.2.3.4-0/`.

The collector script currently preserves these main per-node artifacts:

- `blocks.log.7z`: compressed structured block-event log generated from the node's raw runtime logs.
- `conflux.log.new_blocks.7z`: compressed subset of the Conflux log containing lines with `new block inserted into graph`.
- `metrics.log.7z`: compressed node metrics log.
- In some workflows, derived files such as `metrics.pq` may be created later by the analyzers.

Briefly, the contents mean:

- `blocks.log`: reduced event log produced by `stat_latency_map_reduce.py`. It records parsed block and transaction timing events used by the latency analysis pipeline, including propagation and stage-to-stage delay measurements.
- `conflux.log.new_blocks`: block insertion events from the node's main Conflux log. These are used to reconstruct propagation and confirmation timing across nodes.
- `metrics.log`: time-series metrics emitted by the node runtime, including counters and gauges used for throughput, queue pressure, sync behavior, and transaction-pool analysis.
- `blocks.log.7z`, `conflux.log.new_blocks.7z`, and `metrics.log.7z`: the archived forms that are actually downloaded from remote hosts to save bandwidth and disk space.
- `exp_latency.log`: run-level summary produced by the latency analyzer, usually containing percentile and aggregate latency numbers rather than raw per-event logs.
- `confirmation.log`: analysis output derived from block-event logs, used to study propagation and confirmation structure.

## Minimal End-To-End Example

```bash
cp request_config.example.toml request_config.toml
python -m cloud_provisioner.create_instances --allow-create -c request_config.toml -o hosts.json
python -m remote_simulation -s hosts.json -l logs/$(date +%Y%m%d%H%M%S) -b 2000
python -m cloud_provisioner.cleanup_instances -u lichenxing-alpha-5 -y
```

## Project Structure

```text
cloud_provisioner/   Multi-cloud provisioning, inventory generation, cleanup
remote_simulation/   Remote node launch, topology setup, experiment control, log collection
analyzer/            Post-run log and latency analysis
logs/                Timestamped run directories and collected artifacts
scripts/             Helper scripts used during image preparation and log collection
```

