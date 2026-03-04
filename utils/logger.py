from loguru import logger
import sys
from pathlib import Path

def enrich_record(record):
    # 计算相对路径
    file_path = Path(record["file"].path)
    try:
        relative_path = file_path.relative_to(Path.cwd())
    except ValueError:
        relative_path = file_path
    record["extra"]["rel_path"] = str(relative_path)

    # Context 配置，暂时 disable    
    record["extra"]["formatted_prefix"] = ""
    # # 排除内部字段，其他都当作 prefix
    # internal_keys = {"rel_path", "formatted_prefix"}
    # prefix_keys = [k for k in record["extra"].keys() if k not in internal_keys]
    
    # if prefix_keys:
    #     prefix_parts = [f"[{record['extra'][k]: <4}]" for k in prefix_keys]
    #     record["extra"]["formatted_prefix"] = " ".join(prefix_parts) + " "
    # else:
    #     record["extra"]["formatted_prefix"] = ""
    
    return True

def configure_logger():
    # 配置 logger
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSSSSS}</green> | <level>{level: <8}</level> | <cyan>{extra[rel_path]}</cyan>:<cyan>{line}</cyan> - <level>{extra[formatted_prefix]}{message}</level>",
        colorize=True,
        filter=enrich_record
    )

# 示例使用
if __name__ == "__main__":
    configure_logger()
    
    # 无 context
    logger.info("普通日志消息")
    logger.warning("警告消息")
    
    # 单层 context
    with logger.contextualize(worker="Worker-1"):
        logger.info("工作线程消息")
        logger.error("工作线程错误")
    
    # 双层 context
    with logger.contextualize(region="US-West"):
        logger.info("区域消息")
        with logger.contextualize(task="TaskA"):
            logger.info("任务消息")
            logger.debug("调试信息")
    
    # context 保序            
    with logger.contextualize(task="TaskA"):
        logger.info("任务消息")
        with logger.contextualize(region="US-West"):
            logger.info("区域消息")
            logger.debug("调试信息")
    
    # 三层 context
    with logger.contextualize(worker="Worker-2"):
        with logger.contextualize(stage="Init"):
            with logger.contextualize(step="LoadData"):
                logger.info("多层嵌套消息")
                
    token = logger.contextualize(prefix="Worker-1")
    token.__enter__()
    logger.info("手动管理 context")
    token.__exit__(None, None, None)
    
    # 回到无 context
    logger.success("完成")