#!/bin/bash
set -e
if [[ "$1" == "--install" ]]
then
    rustup toolchain add nightly-2025-03-15
    rustup component add rustfmt --toolchain nightly-2025-03-15
    rustup component add clippy
    shift
fi
cargo +nightly-2025-03-15 fmt --all $@