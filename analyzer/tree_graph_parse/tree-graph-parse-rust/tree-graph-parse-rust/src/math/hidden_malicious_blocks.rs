use std::sync::atomic::AtomicUsize;

use statrs::{
    distribution::{Discrete, NegativeBinomial},
    function::beta::beta_reg,
};

use super::utils::BATCH_SIZE;

pub fn compute_hidden_malicious_blocks_batch(
    start_k: usize, m: usize, adv_percent: usize,
) -> [f64; BATCH_SIZE] {
    static CNT: AtomicUsize = AtomicUsize::new(0);
    let cnt = CNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    dbg!(cnt);

    let prob = 1. - adv_percent as f64 / 100.0;
    let nb_dist = NegativeBinomial::new(m as f64 + 1., prob).unwrap();

    let mut answer = [0.; BATCH_SIZE];

    for i in 0..BATCH_SIZE {
        answer[i] = nb_dist.pmf((start_k + i) as u64);
    }

    return answer;
}

pub fn compute_hidden_malicious_blocks(k: usize, m: usize, adv_percent: usize) -> f64 {
    let prob = 1. - adv_percent as f64 / 100.0;
    let nb_dist = NegativeBinomial::new(m as f64 + 1., prob).unwrap();
    nb_dist.pmf(k as u64)
}

/// Given 恶意节点算力占比 b, m 个诚实区块，不诚实区块 >= k 的概率。
#[allow(unused)]
pub fn compute_hidden_malicious_blocks_prob(b: f64, m: usize, k: usize) -> f64 {
    assert!((0.0..0.5).contains(&b));

    if k == 0 {
        return 1.;
    }

    let success_prob = 1. - b;

    let r = (m + 1) as f64;
    let x = k as f64 - 1.;

    beta_reg(x + 1.0, r, 1. - success_prob)
}

#[cfg(test)]
mod tests {
    use super::*;
    use statrs::distribution::DiscreteCDF;

    #[test]
    fn test_hidden_malicious_blocks() {
        use statrs::prec::almost_eq;

        const B: f64 = 0.3;
        let prob = compute_hidden_malicious_blocks_prob;
        assert!(prob(B, 10, 10) > prob(B, 10, 11));
        assert!(prob(B * 1.01, 10, 10) > prob(B, 10, 10));
        assert_eq!(prob(B, 10, 0), 1.);

        assert!(prob(B, 7000, 2500) > 0.99);
        assert!(prob(B, 7000, 3500) < 0.01);
        assert!(prob(B, 0, 3) > f64::powf(0.3, 3.) * 0.9);

        assert!(almost_eq(
            prob(B, 7000, 2900),
            NegativeBinomial::new(7001., 1. - B).unwrap().sf(2899),
            1e-12
        ));
    }
}
