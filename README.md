# Conflux Massive Test

## Single-node Aliyun smoke test

The entry point is ali_single_node.py at the repository root.

### Usage

1. Ensure the virtual environment is available at .venv and dependencies are installed.
2. Run the script with your Aliyun image ID and key.

Example:

PYTHONPATH=. .venv/bin/python ali_single_node.py \
  --image-id m-8psfy1u8k1jdcjo3vkkq \
  --ssh-private-key ./keys/chenxinghao-conflux-image-builder.pem \
  --region-id ap-southeast-3 \
  --conflux-bin /usr/local/bin/conflux

### Notes

- EVM transactions are sent via the EVM JSON-RPC port and confirmed via receipt polling.
- Conflux RPC is used for block production checks and test block generation.
- The script provisions and cleans up a single ECS instance automatically.
