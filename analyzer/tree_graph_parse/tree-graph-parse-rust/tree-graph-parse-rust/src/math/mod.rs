pub mod hidden_malicious_blocks;
pub mod random_walk;
mod utils;

use statrs::distribution::{DiscreteCDF, NegativeBinomial};

use self::{
    hidden_malicious_blocks::compute_hidden_malicious_blocks,
    random_walk::compute_random_walk_prob, utils::compute_range,
};

use utils::CacheID;

pub fn normal_confirmation_risk(adv_percent: usize, m: usize, adv: usize) -> f32 {
    let prob = 1. - adv_percent as f64 / 100.0;
    let nb_dist = NegativeBinomial::new(m as f64 + 1., prob).unwrap();

    let random_walk_prob = compute_range(adv + 1, CacheID::RandomWalk(adv_percent), |k| {
        compute_random_walk_prob(k, adv_percent)
    });
    let pmf_list = compute_range(adv, CacheID::HiddenMalicious(m, adv_percent), |k| {
        compute_hidden_malicious_blocks(k, m, adv_percent)
    });

    let mut sum = 0.0;
    for k in 0..adv {
        sum += pmf_list[k] * random_walk_prob[adv - k];
    }

    sum += nb_dist.sf(adv as u64);
    sum as f32
}
