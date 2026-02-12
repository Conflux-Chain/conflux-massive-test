#!/bin/bash

set -e  # 遇到错误立即退出

# 颜色输出
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}=== Rust 构建脚本 ===${NC}"

# 检查 Rust 是否已安装
if command -v rustc &> /dev/null && command -v cargo &> /dev/null; then
    echo -e "${GREEN} 检测到 Rust: ${NC}$(cargo --version)"
else
    echo -e "${YELLOW} 未检测到 Rust，开始安装...${NC}"
    
    # 安装 Rust
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    
    # 加载环境变量
    source "$HOME/.cargo/env"
    
    echo -e "${GREEN} Rust 安装完成${NC}"
fi

# 切换到构建目录
SCRIPT_DIR="$(dirname "${BASH_SOURCE[0]}")"
cd "$SCRIPT_DIR/../analyzer/tree_graph_parse/tree-graph-parse-rust"

# 检查是否是 Rust 项目
if [ ! -f "Cargo.toml" ]; then
    echo -e "${RED}✗ 错误: 当前目录不是 Rust 项目（找不到 Cargo.toml）${NC}"
    exit 1
fi

cargo build --release

cd ..
rm -f tg_parse_rpy.so
ln -s tree-graph-parse-rust/target/release/libtg_parse_rpy.so tg_parse_rpy.so
echo -e "${GREEN} 完成构建"