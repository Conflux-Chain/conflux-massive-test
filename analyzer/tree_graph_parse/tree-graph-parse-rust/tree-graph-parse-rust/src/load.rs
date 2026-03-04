//! Conflux 日志加载模块
//!
//! 主要功能：
//! 1. 根据输入路径查找并加载区块就绪日志 `*.conflux.log.new_block_read`
//! 2. 当基础日志文件 `*.conflux.log` 存在时，自动生成区块就绪日志（通过grep过滤原始日志）
//! 3. 处理路径为目录或文件的不同情况

use anyhow::{anyhow, bail, Context, Result};
use glob::glob;
use std::{
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
    process::Command,
};

/// 打开并返回Conflux日志的缓冲读取器
pub fn open_conflux_log(path_string: &str) -> Result<BufReader<File>> {
    let filename = find_conflux_log(path_string)?;
    let file = File::open(filename)?;
    Ok(BufReader::new(file))
}

/// 判断路径类型并分派处理
fn find_conflux_log(path_string: &str) -> Result<String> {
    let path = Path::new(path_string);
    if path.is_dir() {
        find_or_create_in_directory(path_string)
    } else if path.is_file() {
        handle_file_path(path_string)
    } else {
        bail!("Path '{}' is neither a file nor a directory", path_string)
    }
}

/// 处理目录路径：查找或创建日志文件
fn find_or_create_in_directory(dir_path: &str) -> Result<String> {
    // 优先查找区块就绪日志
    let new_blocks_files = find_files_with_pattern(dir_path, "*.log.new_blocks")?;
    if !new_blocks_files.is_empty() {
        return handle_multiple_files(new_blocks_files, "*.log.new_blocks", dir_path);
    }

    // 查找基础日志文件
    let base_log_files = find_files_with_pattern(dir_path, "*.conflux.log")?;
    if base_log_files.is_empty() {
        bail!("目录 '{}' 中没有找到.conflux.log文件", dir_path);
    }

    // 处理找到的基础日志
    let single_log_file = handle_multiple_files(base_log_files, "*.conflux.log", dir_path)?;
    create_new_blocks_file(&single_log_file)
}

/// 处理文件路径：验证文件类型或创建关联文件
fn handle_file_path(file_path: &str) -> Result<String> {
    let path = Path::new(file_path);
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow!("无效文件名: {}", file_path))?;

    // 已经是区块就绪日志文件
    if file_name.ends_with(".log.new_blocks") {
        return Ok(file_path.to_string());
    }

    // 处理基础日志文件
    if file_name.ends_with(".conflux.log") {
        let new_blocks_path = format!("{}.new_blocks", file_path);
        // 检查关联文件是否存在
        if Path::new(&new_blocks_path).exists() {
            return Ok(new_blocks_path);
        }
        // 创建新的关联文件
        return create_new_blocks_file(file_path);
    }

    bail!(
        "File '{}' must be either *.conflux.log or *.log.new_blocks",
        file_path
    )
}

/// 使用glob模式查找目录中的文件
fn find_files_with_pattern(dir_path: &str, pattern: &str) -> Result<Vec<PathBuf>> {
    let full_pattern = format!("{}/{}", dir_path, pattern);
    let mut files = Vec::new();

    for entry in glob(&full_pattern)? {
        match entry {
            Ok(path) => files.push(path),
            Err(e) => bail!("Error scanning files: {}", e),
        }
    }

    Ok(files)
}

/// 处理多个匹配文件的情况（单文件正常返回，多文件报错）
fn handle_multiple_files(files: Vec<PathBuf>, pattern: &str, dir_path: &str) -> Result<String> {
    match files.len() {
        0 => bail!("No {} files found in directory '{}'", pattern, dir_path),
        1 => Ok(files[0].to_string_lossy().to_string()),
        _ => bail!(
            "Multiple {} files found in directory '{}': {:?}",
            pattern,
            dir_path,
            files
        ),
    }
}

/// 通过shell命令生成区块就绪日志文件
fn create_new_blocks_file(base_file: &str) -> Result<String> {
    let new_path = format!("{}.new_blocks", base_file);

    let output = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "cat {} | grep \"new block inserted into graph\" > {}",
            base_file, new_path
        ))
        .output()
        .context("Failed to execute command to create .new_blocks file")?;

    if !output.status.success() {
        bail!(
            "Failed to create .new_blocks file: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    Ok(new_path)
}
