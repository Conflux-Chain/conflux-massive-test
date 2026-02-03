import argparse
from .stat_latency_log_analyzer import LogAnalyzer


def main():
    parser = argparse.ArgumentParser(description="分析日志延迟")
    
    parser.add_argument(
        "-l", "--log-path",
        type=str,
        required=True,
        help="日志存储路径 (必需)"
    )
    
    args = parser.parse_args()
    
    # 调用分析器
    LogAnalyzer("name_tmp", args.log_path, csv_output=None).analyze()

if __name__ == "__main__":
    main()