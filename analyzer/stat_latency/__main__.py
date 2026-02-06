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

    parser.add_argument(
        "-d", "--storage-db",
        type=str,
        required=False,
        help="Path to sqlite db to use as disk-backed storage (optional)"
    )

    parser.add_argument(
        "--db-batch-size",
        type=int,
        required=False,
        help="Number of ops per batch for DB writer (default: 100)",
    )

    parser.add_argument(
        "--db-commit-threshold",
        type=int,
        required=False,
        help="Commit threshold (ops) before forcing a commit (default: 1000)",
    )

    parser.add_argument(
        "--db-wal",
        action="store_true",
        required=False,
        help="Enable WAL mode (default: enabled)",
    )

    parser.add_argument(
        "--db-synchronous",
        type=str,
        choices=["OFF", "NORMAL", "FULL"],
        required=False,
        help="PRAGMA synchronous (default: NORMAL)",
    )

    parser.add_argument(
        "--db-cache-size",
        type=int,
        required=False,
        help="PRAGMA cache_size (default: 10000)",
    )

    parser.add_argument(
        "--read-db",
        action="store_true",
        required=False,
        help="Read and analyze an existing storage DB instead of parsing logs",
    )

    parser.add_argument(
        "--preserve-db",
        action="store_true",
        required=False,
        help="Preserve raw rows in storage DB during aggregation (useful for debugging)",
    )
    
    args = parser.parse_args()

    storage_kwargs = {}
    if args.db_batch_size is not None:
        storage_kwargs["batch_size"] = args.db_batch_size
    if args.db_commit_threshold is not None:
        storage_kwargs["commit_threshold"] = args.db_commit_threshold
    if args.db_wal:
        storage_kwargs["wal"] = True
    if args.db_synchronous is not None:
        storage_kwargs["synchronous"] = args.db_synchronous
    if args.db_cache_size is not None:
        storage_kwargs["cache_size"] = args.db_cache_size

    # 调用分析器
    analyzer = LogAnalyzer("name_tmp", args.log_path, csv_output=None, storage_db=args.storage_db, storage_kwargs=(storage_kwargs or None), preserve_db=args.preserve_db)
    if args.read_db:
        analyzer.analyze_db_only()
    else:
        analyzer.analyze()

if __name__ == "__main__":
    main()