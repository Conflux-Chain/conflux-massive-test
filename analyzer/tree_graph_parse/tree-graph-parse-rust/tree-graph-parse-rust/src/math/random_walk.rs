// #[cached]
// pub fn compute_random_walk_prob_batch(start_k: usize, adv_percent: usize) -> [f64; BATCH_SIZE] {
//     let mut result = [0.0; BATCH_SIZE];

//     for i in 0..BATCH_SIZE {
//         result[i] = compute_random_walk_prob(start_k + i, adv_percent);
//     }

//     result
// }

/// 主计算函数：通过混合精确计算和渐近估计求上界
pub fn compute_random_walk_prob(k: usize, adv_percent: usize) -> f64 {
    let b = adv_percent as f64 / 100.;

    // 验证输入符合 MMA 代码的假设条件
    assert!((0.0..0.5).contains(&b), "b 必须在 (0, 0.5) 之间");
    if k == 0 {
        return 0.;
    }

    const ABSOLUTE_ERROR_LIMIT: f64 = 1e-40;
    const NELI_ERROR_LIMIT: f64 = 1e-80;
    const RELATIVE_ERROR_LIMIT: f64 = 1e-8;

    let k = k as i64;

    // 初始化关键参数
    let s_inf = min_s_inf(b);
    let r = geometric_ratio(b);
    let mut sum = 0.0;
    let mut current_n = k + 1;

    // 动态计算策略：精确项 + 渐近估计
    loop {
        // 精确计算当前项并累加
        sum += term_exact(current_n, k, b);
        if sum >= 1.0 {
            return 1.0;
        }

        // 预判下一项的渐近估计值
        current_n += 1;

        // 每 10 个 loop 估算一次
        if current_n % 10 != 0 {
            continue;
        }

        let approx_next_term = term_inf_approx(current_n, k, b, s_inf);
        let accurate_next_term = term_exact(current_n, k, b);

        let relative_error = (approx_next_term - accurate_next_term) / approx_next_term;

        let sum_remaining = approx_next_term / (1.0 - r);
        let sum_error = sum_remaining * relative_error;

        if sum_error > ABSOLUTE_ERROR_LIMIT {
            continue;
        }

        if sum + sum_remaining < NELI_ERROR_LIMIT {
            return 0.0;
        }

        if sum_error > (sum + sum_remaining) * RELATIVE_ERROR_LIMIT {
            continue;
        }

        return (sum + sum_remaining).min(1.0);
    }
}

// 对应 MMA 代码中的 g[s_, b_] := Log[b*Exp[s] + (1 - b)*Exp[-s]]
/// 计算辅助函数 g(s,b) = ln(b*e^s + (1-b)e^{-s})
fn g(s: f64, b: f64) -> f64 { (b * s.exp() + (1.0 - b) * (-s).exp()).ln() }

// 对应 MMA 中的 logProb[n_,k_,b_,s_] := g[s,b]*n - s*k
/// 计算对数概率函数：n*g(s,b) - s*k
fn log_prob(n: i64, k: i64, b: f64, s: f64) -> f64 {
    let g_value = g(s, b);
    (n as f64) * g_value - (k as f64) * s
}

// 对应 MMA 的公式：1/2 Log[(-k + b k - n + b n)/(b (k - n))]
/// 计算 s 的最优解：0.5 * ln[ ((1-b)(k+n)) / (b(n-k)) ]
fn min_s(n: i64, k: i64, b: f64) -> f64 {
    let numerator = (1.0 - b) * (k + n) as f64;
    let denominator = b * (n - k) as f64;
    0.5 * (numerator / denominator).ln()
}

// 对应 MMA 的极限公式：1/2 Log[-1 + 1/b]
/// 计算当 n→∞ 时的极限解：0.5 * ln( (1-b)/b )
fn min_s_inf(b: f64) -> f64 { 0.5 * ((1.0 - b) / b).ln() }

/// 计算精确项：exp(logProb) 的最小上界（基于精确解 min_s）
fn term_exact(n: i64, k: i64, b: f64) -> f64 {
    let s_opt = min_s(n, k, b);
    log_prob(n, k, b, s_opt).exp().min(1.0)
}

/// 计算近似项：exp(logProb) 的渐近估计（基于极限解 s_inf）
fn term_inf_approx(n: i64, k: i64, b: f64, s_inf: f64) -> f64 {
    let lp = log_prob(n, k, b, s_inf);
    lp.exp().min(1.0)
}

// 对应 MMA 中发现的等比数列性质：r = Exp[g(min_s_inf(b), b)] = 2*sqrt(b(1-b))
/// 计算无穷级数的公比：r = 2√[b(1-b)]
fn geometric_ratio(b: f64) -> f64 { 2.0 * (b * (1.0 - b)).sqrt() }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_walk_prob() {
        // 测试用例（对应 MMA 的输入示例）
        let test_cases = vec![(1000, 40), (100, 40), (100, 30), (10, 20), (0, 49)];

        for (k, b) in test_cases {
            let result = compute_random_walk_prob(k, b);
            println!("k={}, b={:.2} => 上界={:?}", k, b, result);
        }
    }
}
