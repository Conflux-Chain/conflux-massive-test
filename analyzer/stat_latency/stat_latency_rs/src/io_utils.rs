use anyhow::{anyhow, Context, Result};
use std::ffi::OsStr;
use std::fs;
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use crate::model::HostBlocksLog;

pub fn scan_logs(log_dir: &Path) -> Result<(Vec<PathBuf>, Vec<PathBuf>)> {
    let mut blocks_logs = Vec::new();
    let mut dirs_with_blocks_log = std::collections::HashSet::new();

    for entry in WalkDir::new(log_dir).follow_links(false) {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        if entry.file_name() == OsStr::new("blocks.log") {
            let path = entry.path().to_path_buf();
            blocks_logs.push(path.clone());
            if let Some(parent) = path.parent() {
                dirs_with_blocks_log.insert(parent.to_path_buf());
            }
        }
    }

    let mut archives = Vec::new();
    for entry in WalkDir::new(log_dir).follow_links(false) {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        if entry.file_name() == OsStr::new("blocks.log.7z") {
            let path = entry.path().to_path_buf();
            archives.push(path.clone());
            if let Some(parent) = path.parent() {
                dirs_with_blocks_log.insert(parent.to_path_buf());
            }
        }
    }

    blocks_logs.sort();
    archives.sort();
    Ok((blocks_logs, archives))
}

pub fn load_host_log_from_path(path: &Path) -> Result<HostBlocksLog> {
    let data = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let host: HostBlocksLog = serde_json::from_slice(&data)
        .with_context(|| format!("parse JSON from {}", path.display()))?;
    Ok(host)
}

pub fn load_host_log_from_archive(path: &Path) -> Result<HostBlocksLog> {
    let data = extract_blocks_log_from_7z(path)?;
    let host: HostBlocksLog = serde_json::from_slice(&data)
        .with_context(|| format!("parse JSON from {} (blocks.log in archive)", path.display()))?;
    Ok(host)
}

fn archive_reader(path: &Path) -> Result<sevenz_rust::SevenZReader<fs::File>> {
    let mut file = fs::File::open(path)
        .with_context(|| format!("failed to open archive {}", path.display()))?;

    let pos = file
        .stream_position()
        .with_context(|| format!("failed to get stream position for {}", path.display()))?;
    let len = file
        .seek(SeekFrom::End(0))
        .with_context(|| format!("failed to seek to end for {}", path.display()))?;
    file.seek(SeekFrom::Start(pos))
        .with_context(|| format!("failed to seek to start for {}", path.display()))?;

    let password = sevenz_rust::Password::empty();
    sevenz_rust::SevenZReader::new(file, len, password)
        .with_context(|| format!("failed to create 7z reader for {}", path.display()))
}

fn extract_blocks_log_from_7z(archive_path: &Path) -> Result<Vec<u8>> {
    if let Ok(bytes) = extract_member_from_7z(archive_path, "blocks.log") {
        return Ok(bytes);
    }

    let mut seven = archive_reader(archive_path)?;
    let mut candidates: Vec<String> = Vec::new();
    seven
        .for_each_entries(|entry, _| {
            if entry.name().ends_with("blocks.log") {
                candidates.push(entry.name().to_string());
            }
            Ok(true)
        })
        .with_context(|| format!("failed to iterate entries in {}", archive_path.display()))?;

    if candidates.is_empty() {
        return Err(anyhow!(
            "no blocks.log found in archive {}",
            archive_path.display()
        ));
    }

    candidates.sort_by(|a, b| {
        let la = a.len();
        let lb = b.len();
        la.cmp(&lb).then_with(|| a.cmp(b))
    });
    extract_member_from_7z(archive_path, &candidates[0])
}

fn extract_member_from_7z(archive_path: &Path, member: &str) -> Result<Vec<u8>> {
    let mut seven = archive_reader(archive_path)?;
    let mut result: Option<Vec<u8>> = None;

    seven
        .for_each_entries(|entry, reader| {
            if entry.name() == member {
                let mut out = Vec::new();
                reader.read_to_end(&mut out)?;
                result = Some(out);
            }
            Ok(true)
        })
        .with_context(|| {
            format!(
                "failed to read content of {} from {}",
                member,
                archive_path.display()
            )
        })?;

    result.ok_or_else(|| {
        anyhow!(
            "member {} not found in archive {}",
            member,
            archive_path.display()
        )
    })
}
