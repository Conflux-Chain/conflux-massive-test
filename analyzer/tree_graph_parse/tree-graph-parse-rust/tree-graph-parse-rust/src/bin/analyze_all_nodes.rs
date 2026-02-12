extern crate tree_graph_parse_rust;

use rayon::prelude::*;
use std::error::Error;
use walkdir::WalkDir;

use tree_graph_parse_rust::graph::Graph;

// 查找所有匹配pattern的文件
fn find_files(root_path: &str, pattern: &str) -> Vec<String> {
    let mut matching_files = Vec::new();

    for entry in WalkDir::new(root_path)
        .follow_links(true)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let path = entry.path();
        if path.is_file()
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name == pattern)
                .unwrap_or(false)
        {
            matching_files.push(path.to_path_buf().to_str().unwrap().to_string());
        }
    }

    matching_files
}

// 多线程加载所有图
fn load_all_graphs(file_paths: Vec<String>) -> Vec<Graph> {
    // 使用rayon并行处理所有文件
    file_paths
        .par_iter()
        .map(|path| Graph::load(&path).unwrap())
        .collect()
}

fn main() -> Result<(), Box<dyn Error>> {
    // 要搜索的根路径
    let root_path = "/data/liuyuan/perftest/0422/2000_rand";

    // 固定的文件名模式
    let file_pattern = "conflux.log.new_blocks";

    // 查找所有匹配的文件
    let matching_files = find_files(root_path, file_pattern);
    println!("Found {} matching files", matching_files.len());

    // 多线程加载所有文件
    let graphs = load_all_graphs(matching_files);
    println!("Successfully loaded {} graphs", graphs.len());

    graphs.par_iter().for_each(|x| {
        x.avg_confirm_time(10, 1e-6);
    });

    Ok(())
}
