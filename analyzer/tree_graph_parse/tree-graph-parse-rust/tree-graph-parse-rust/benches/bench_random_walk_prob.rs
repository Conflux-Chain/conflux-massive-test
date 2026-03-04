use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tree_graph_parse_rust::math::*;

fn bench_random_walk_prob(c: &mut Criterion) {
    let mut group = c.benchmark_group("compute_sum_upper_bound");

    // Test with different input sizes
    for prob in [0.1, 0.3, 0.4].iter() {
        for k in [10, 100, 1000].iter() {
            group.bench_with_input(format!("k={}, b={}", k, prob), k, |b, &k| {
                b.iter(|| {
                    random_walk::compute_random_walk_prob(
                        black_box(k),
                        black_box((*prob * 100.) as usize),
                    )
                });
            });
        }
    }

    group.finish();
}

fn bench_confirmation_risk(c: &mut Criterion) {
    let mut group = c.benchmark_group("compute_confirmation_risk");

    // Test with different input sizes
    for prob in [0.1, 0.3, 0.4].iter() {
        for k in [10, 100, 1000].iter() {
            let m = (*k as f64 * (1. - prob) / prob) as usize;
            group.bench_with_input(format!("k={}, b={}", k, prob), k, |b, &k| {
                b.iter(|| {
                    normal_confirmation_risk(
                        (*prob * 100.) as usize,
                        black_box(m),
                        black_box(m - k),
                    )
                });
            });
        }
    }
}

criterion_group!(benches, bench_random_walk_prob, bench_confirmation_risk);
criterion_main!(benches);
