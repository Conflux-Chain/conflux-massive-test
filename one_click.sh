#!/bin/bash

ulimit -n 65536

# 加载 .env 文件
if [ -f .env ]; then
    set -a  # 自动导出所有变量
    source .env
    set +a  # 关闭自动导出
fi

# 检查 USER_TAG_PREFIX 是否为空
if [ -z "$USER_TAG_PREFIX" ]; then
    echo "错误: USER_TAG_PREFIX 未设置或为空"
    exit 1
fi

# 遇到错误立即退出 (确保第一步失败时脚本停止)
set -e

# 定义 Python 解释器路径
PYTHON="/home/ubuntu/miniconda/bin/python"

# 定义日志分割函数
print_separator() {
    echo ""
    echo "============================================================"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
    echo "============================================================"
    echo ""
}

# 定义清理函数 (确保无论第二步成功还是失败，都会执行销毁)
cleanup() {
    # 临时关闭 'set -e'，防止销毁过程中的小错误阻断日志打印
    set +e
    
    print_separator "正在执行清理操作: 销毁云服务实例..."
    $PYTHON -m cloud_provisioner.cleanup_instances -u $USER_TAG_PREFIX
    
    # print_separator "跳过销毁实例"
    print_separator "清理结束"
}

# 注册 trap：脚本退出时（无论是正常结束还是出错中断）都会执行 cleanup 函数
trap cleanup EXIT

LOG_PATH="logs/$(date +"%Y%m%d%H%M%S")"
mkdir -p $LOG_PATH

# --- 步骤 1: 创建实例 ---
print_separator "步骤 1/3: 创建云服务实例..."
$PYTHON -m cloud_provisioner.create_instances

# --- 步骤 2: 远程模拟 ---
print_separator "步骤 2/3: 开始远程模拟..."
$PYTHON -m remote_simulation -l $LOG_PATH

# --- 如果远程模拟成功退出，手动触发清理，并且关闭 trap ---
cleanup
trap - EXIT

# --- 步骤 3: 日志分析 ---
print_separator "步骤 3/3：开始分析日志"

python -m analyzer.stat_latency -l $LOG_PATH/nodes | tee $LOG_PATH/exp_latency.log

print_separator "测试完毕，查看 $LOG_PATH 获得更多细节"