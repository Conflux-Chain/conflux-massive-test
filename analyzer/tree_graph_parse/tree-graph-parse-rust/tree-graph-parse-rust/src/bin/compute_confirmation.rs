extern crate tree_graph_parse_rust;

use std::time::Instant;

use tree_graph_parse_rust::graph::Graph;

fn avg_confirmation_time(graph: &Graph, adv_percent: usize, risk_threshold: f64) {
    let mut total_confirm_time = 0.;
    let mut block_cnt = 0;
    for block in graph.pivot_chain() {
        if block.height == 0 {
            continue;
        }

        let Some((time_elapsed, ..)) = graph.confirmation_risk(block, adv_percent, risk_threshold)
        else {
            continue;
        };

        total_confirm_time +=
            (time_elapsed as f64 + graph.avg_epoch_time(block)) * block.epoch_size() as f64;
        block_cnt += block.epoch_size();
    }
    println!(
        "Average confirmation time for {adv_percent}: {:.2} from {} blocks",
        total_confirm_time / block_cnt as f64,
        block_cnt
    );
}

fn main() {
    let instant = Instant::now();

    let graph = Graph::load("/data/liuyuan/perftest/0324/10000_15000/").unwrap();

    // dbg!(&graph.genesis_block().subtree_size_series);
    for block in graph.pivot_chain() {
        if block.height == 0 {
            continue;
        }

        println!(
            "height {}, subtree_size {}, past_set {}, epoch_span {}, avg_span {:.1}",
            block.height,
            block.subtree_size,
            block.past_set_size,
            graph.epoch_span(block),
            graph.avg_epoch_time(block),
        );
        for percentage in (10..=30).step_by(5) {
            print!("Adversary power {percentage}%: ");
            for &risk in [1e-4, 1e-5, 1e-6, 1e-7, 1e-8].iter() {
                let Some((time_offset, m, k, _)) = graph.confirmation_risk(block, percentage, risk)
                else {
                    continue;
                };
                print!(" {:e} | ({}, {}, {}) \t|", risk, time_offset, m, k);
            }
            print!("\n");

            // println!(
            //     "{i}% confirm {:?}",
            //     graph.confirmation_risk_series(block, i)
            // );
        }

        println!("\n");
    }

    for &risk in [1e-4, 1e-5, 1e-6, 1e-7, 1e-8].iter() {
        println!("\n confirmation risk {risk}");
        avg_confirmation_time(&graph, 10, risk);
        avg_confirmation_time(&graph, 15, risk);
        avg_confirmation_time(&graph, 20, risk);
        avg_confirmation_time(&graph, 30, risk);
    }

    println!("\nTotal time elapsed: {:?}", instant.elapsed());
}
