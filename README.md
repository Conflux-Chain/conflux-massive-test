```bash
conflux-deployer force-instances --mode state -c config.json --state-file ./deployment_state.json
conflux-deployer force-instances --mode pattern -c config.json --deployment-id deploy-20250101-010101-deadbeef
conflux-deployer force-instances --mode pattern -c config.json --pattern-prefix "conflux-deployer-mytest-deploy-abc-us-west-2"
```

# Conflux Deployer Framework

A Python framework for deploying Conflux private chain networks across multiple countries/regions on AWS and Alibaba Cloud platforms, and executing stress tests and other related tests.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Modules](#modules)
- [Examples](#examples)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## Features

- **Multi-cloud Support**: Deploy Conflux nodes on both AWS and Alibaba Cloud
- **Cross-region Deployment**: Support for deploying nodes across multiple countries/regions
- **Automatic Resource Management**: Automatic creation and cleanup of cloud resources
- **Intelligent Node Allocation**: Automatic adjustment of Conflux node deployment count based on hardware configuration
- **Comprehensive Testing**: Support for TPS, latency, stability, and stress tests
- **Real-time Monitoring**: Real-time collection and storage of node information
- **Reliable Cleanup**: Automatic cleanup of resources even in case of errors or interruptions

## Architecture

The framework follows a modular architecture with clear separation of concerns:

1. **Config Manager**: Handles configuration loading and management
2. **Cloud Account Manager**: Manages AWS and Alibaba Cloud account authentication
3. **Image Manager**: Creates and manages server images with Docker and Conflux pre-installed
4. **Server Deployer**: Deploys and manages cloud server instances
5. **Node Manager**: Collects and manages Conflux node information
6. **Test Controller**: Executes and controls various tests
7. **Resource Cleanup Manager**: Cleans up all resources after tests

## Installation

### Prerequisites

- Python 3.8+
- AWS SDK for Python (boto3)
- Alibaba Cloud SDK for Python (alibabacloud_ecs20140526)
- loguru (for logging)

### Installation Steps

1. Clone the repository:

```bash
git clone <repository-url>
cd conflux-massive-test
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Configure AWS and Alibaba Cloud credentials:

- For AWS: Set up your AWS credentials using the AWS CLI or environment variables
- For Alibaba Cloud: Set up your Alibaba Cloud credentials

## Configuration

### Configuration File

Create a `config.json` file based on the `config.example.json` template:

```json
{
  "cloud_accounts": {
    "aws": {
      "default": {
        "access_key": "YOUR_AWS_ACCESS_KEY",
        "secret_key": "YOUR_AWS_SECRET_KEY"
      }
    },
    "alibaba": {
      "default": {
        "access_key": "YOUR_ALIBABA_ACCESS_KEY",
        "secret_key": "YOUR_ALIBABA_SECRET_KEY"
      }
    }
  },
  "instance_configs": {
    "m6i.2xlarge": {
      "base_image_id": "ami-0c55b159cbfafe1f0",
      "key_name": "your-key-pair",
      "security_group_ids": ["sg-12345678"],
      "subnet_id": "subnet-12345678",
      "volume_size": 100,
      "alternative_instance_type": "m7i.2xlarge"
    },
    "m7i.2xlarge": {
      "base_image_id": "ami-0c55b159cbfafe1f0",
      "key_name": "your-key-pair",
      "security_group_ids": ["sg-12345678"],
      "subnet_id": "subnet-12345678",
      "volume_size": 100,
      "alternative_instance_type": "m7i.4xlarge"
    }
  },
  "conflux": {
    "base_port": 12537,
    "base_rpc_port": 12539,
    "base_p2p_port": 12538
  },
  "tests": {
    "tps": {
      "duration": 300,
      "tx_rate": 1000
    },
    "latency": {
      "duration": 300,
      "tx_count": 1000
    },
    "stability": {
      "duration": 3600
    },
    "stress": {
      "duration": 600,
      "max_tx_rate": 5000
    }
  }
}
```

### Node Allocation Configuration

The framework automatically adjusts the number of Conflux nodes based on instance type:

- **AWS m6i.2xlarge**: 1 Conflux node
- **AWS m7i.2xlarge**: 2 Conflux nodes
- **AWS m6i.4xlarge**: 4 Conflux nodes
- **AWS m7i.4xlarge**: 6 Conflux nodes
- **AWS m6i.8xlarge**: 8 Conflux nodes
- **AWS m7i.8xlarge**: 12 Conflux nodes
- **Alibaba Cloud ecs.c6.2xlarge**: 2 Conflux nodes

## Usage

### Basic Usage

```python
from conflux_deployer import ConfluxDeployer

# Initialize deployer
deployer = ConfluxDeployer("config.json")

# Deploy network across regions
region_configs = [
    {
        "cloud_provider": "aws",
        "region": "us-west-2",
        "instance_type": "m6i.2xlarge",
        "count": 2
    },
    {
        "cloud_provider": "alibaba",
        "region": "us-west-1",
        "instance_type": "ecs.c6.2xlarge",
        "count": 2
    }
]

node_info_list = deployer.deploy_network(region_configs)

# Run tests
tps_result = deployer.run_test("tps", {"duration": 300, "tx_rate": 1000})
latency_result = deployer.run_test("latency", {"duration": 300, "tx_count": 1000})

# Clean up resources
cleanup_result = deployer.cleanup(force=True)
```

### Running the Example

```bash
python examples/deploy_and_test.py
```

## Modules

### Config Manager

Handles loading and management of configuration settings from JSON files.

### Cloud Account Manager

Manages authentication for AWS and Alibaba Cloud accounts, including support for multiple accounts and regions.

### Image Manager

Automatically creates server images with Docker and Conflux pre-installed, including image existence checks and reuse mechanisms.

### Server Deployer

Deploys cloud server instances across multiple regions, with automatic adjustment of node count based on hardware configuration.

### Node Manager

Collects and stores node information, including IP addresses, ports, and status, with state persistence for recovery from failures.

### Test Controller

Executes various tests including TPS, latency, stability, and stress tests, with detailed metrics collection.

### Resource Cleanup Manager

Ensures reliable cleanup of all resources, even in case of errors or interruptions, with signal handlers for graceful shutdown.

## Examples

### Deployment Example

See `examples/deploy_and_test.py` for a complete example of deploying a Conflux network across multiple regions and running tests.

### Test Example

```python
# Run a TPS test
tps_result = deployer.run_test("tps", {
    "duration": 300,  # 5 minutes
    "tx_rate": 1000   # 1000 transactions per second
})

# Run a stress test
stress_result = deployer.run_test("stress", {
    "duration": 600,    # 10 minutes
    "max_tx_rate": 5000  # Up to 5000 transactions per second
})
```

### Running remote_simulation tests with the new framework ✅

You can run the existing tests in `remote_simulation` using the new
`conflux_deployer` codebase without changing the test logic. Use the
included adapter and helper script to create instances, run simulations,
and clean up resources.

Option 1 — Quick CLI (recommended): use `remote_simulate_v2.py` which uses
`DeployerAdapter` under the hood and keeps compatibility with the old
`instances.pkl` format.

Examples:

- Run full workflow (create instances → test → cleanup):

```bash
python remote_simulate_v2.py run --instance-count 10 --instance-type m6i.2xlarge
```

- Create instances only (save state for later):

```bash
python remote_simulate_v2.py create -n 10 -t m6i.2xlarge -o instances.pkl
```

- Run test on existing instances (from pickle):

```bash
python remote_simulate_v2.py test -f instances.pkl --nodes-per-host 1
```

- Cleanup instances (from pickle or state file):

```bash
python remote_simulate_v2.py cleanup -f instances.pkl
# or
python remote_simulate_v2.py cleanup -s state/conflux-test-<timestamp>.json
```

- Stop nodes on all instances (keep instances):

```bash
python remote_simulate_v2.py stop-nodes -f instances.pkl  # stop
python remote_simulate_v2.py stop-nodes -f instances.pkl --destroy  # destroy nodes
```

Option 2 — Use the adapter directly in Python (programmatic):

```python
from conflux_deployer import DeployerAdapter

adapter = DeployerAdapter()
# Launch instances
instances = adapter.launch(instance_count=10, instance_type="m6i.2xlarge")
# Use ip addresses with existing remote_simulation tests
ip_addresses = instances.ip_addresses

# Run remote_simulation functions (example)
from remote_simulate_v2 import run_simulation
run_simulation(ip_addresses, nodes_per_host=1)

# Cleanup when done
adapter.terminate()
```

Notes and tips:

- Authentication: set AWS/Alibaba credentials via environment variables
  or a `.env` file (e.g. `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`).
- Compatibility: `remote_simulate_v2.py` understands legacy `instances.pkl`
  and the new deployer state files — it will prefer state-based recovery
  when available.
- Auto-cleanup: the new framework registers signal handlers and supports
  automatic cleanup on errors; you can disable automatic cleanup by
  saving state and calling `cleanup` later.

## Troubleshooting

### Common Issues

1. **Authentication Errors**: Ensure your AWS and Alibaba Cloud credentials are correctly configured.
2. **Resource Limitations**: If you encounter resource limits, increase your cloud provider's service quotas.
3. **Network Issues**: Ensure your security groups allow the necessary ports for Conflux nodes.
4. **Cleanup Failures**: If cleanup fails, manually terminate instances through the cloud provider's console.

### Logs

Check the `deploy_and_test.log` file for detailed logs of framework operations and errors.

## License

[MIT License](LICENSE)
