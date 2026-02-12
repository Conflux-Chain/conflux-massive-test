use std::{
    collections::HashMap,
    sync::{LazyLock, RwLock},
};

pub const BATCH_SIZE: usize = 64;
static CACHE: LazyLock<RwLock<HashMap<CacheID, RwLock<Vec<f64>>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

#[derive(Hash, PartialEq, Eq, Clone, Copy, Debug)]
pub enum CacheID {
    HiddenMalicious(usize, usize),
    RandomWalk(usize),
}

pub fn compute_range(
    length: usize, cache_id: CacheID, compute: impl FnMut(usize) -> f64,
) -> Vec<f64> {
    let read_guard = CACHE.read().unwrap();
    if let Some(cache_item) = read_guard.get(&cache_id) {
        compute_range_inner(length, cache_id, compute, cache_item)
    } else {
        std::mem::drop(read_guard);
        CACHE.write().unwrap().entry(cache_id).or_default();

        let cache_guard = &*CACHE.read().unwrap();
        let cache_item = cache_guard.get(&cache_id).unwrap();
        compute_range_inner(length, cache_id, compute, cache_item)
    }
}

fn compute_range_inner(
    length: usize, _cache_id: CacheID, compute: impl FnMut(usize) -> f64,
    cache_item: &RwLock<Vec<f64>>,
) -> Vec<f64> {
    {
        let cached_vec = &*cache_item.read().unwrap();
        if cached_vec.len() >= length {
            return cached_vec[..length].to_vec();
        }
    }

    {
        let cached_vec = &mut *cache_item.write().unwrap();
        cached_vec.extend((cached_vec.len()..length).map(compute));
        cached_vec[..length].to_vec()
    }
}
