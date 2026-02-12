use itertools::Itertools;
use std::{borrow::Borrow, fmt};

#[derive(Clone, PartialEq)]
pub struct TimeSeries<T: Clone> {
    start_timestamp: u32,
    series: Vec<(u16, T)>,
}

impl<T: Clone> TimeSeries<T> {
    /// Create a new TimeSeries with a single data point
    pub fn new(timestamp: u64, payload: T) -> Self {
        TimeSeries {
            start_timestamp: timestamp as u32,
            series: vec![(0, payload)],
        }
    }

    /// 创建一个新的 TimeSeries，输入是一个时间戳-值对的列表，
    /// 解决重复时间戳的冲突
    pub fn new_list(mut input: Vec<(u64, T)>, resolve_conflict: impl Fn(&[&T]) -> T) -> Self {
        assert!(!input.is_empty());
        // 按时间戳排序
        input.sort_by_key(|&(timestamp, _)| timestamp);

        // 确定 start_timestamp，取最小时间戳
        let start_timestamp = input[0].0 as u32;
        let mut series = vec![];

        // 使用 group_by 把相同时间戳的值分组，然后处理每组
        for (ts, group) in &input.iter().chunk_by(|&(ts, _)| *ts) {
            // 收集所有具有相同时间戳的值
            let values: Vec<&T> = group.map(|(_, v)| v).collect();

            // 如果有多个值，使用 resolve_conflict，否则直接克隆
            let resolved_value = if values.len() > 1 {
                resolve_conflict(&values)
            } else {
                values[0].clone()
            };

            // 计算偏移量并返回
            let offset = (ts - start_timestamp as u64) as u16;
            series.push((offset, resolved_value))
        }

        Self {
            start_timestamp,
            series,
        }
    }

    /// Get the start timestamp
    pub fn start_timestamp(&self) -> u32 { self.start_timestamp }

    /// Get the series data
    pub fn iter(&self) -> impl Iterator<Item = (u64, &T)> {
        self.series
            .iter()
            .map(|(ts_offset, val)| ((self.start_timestamp + *ts_offset as u32) as u64, val))
    }

    pub fn at(&self, timestamp: u64) -> Option<&T> {
        let timestamp = timestamp as u32;
        if timestamp < self.start_timestamp {
            return None;
        }

        let target_offset = timestamp - self.start_timestamp;

        let idx = match self
            .series
            .binary_search_by(|(offset, _)| (*offset as u32).cmp(&target_offset))
        {
            Ok(idx) => idx,
            Err(idx_next) => {
                let Some(idx) = idx_next.checked_sub(1) else {
                    return None;
                };
                idx
            }
        };
        Some(&self.series[idx].1)
    }

    pub fn union(a: &Self, b: &Self, resolve_conflict: impl Fn(&T, &T) -> T) -> Self {
        let mut result = Vec::new();
        let mut a_iter = a.series.iter().peekable();
        let mut b_iter = b.series.iter().peekable();

        // The new start timestamp is the minimum of the two
        let new_start = a.start_timestamp.min(b.start_timestamp);

        while let (Some(&&(a_offset, _)), Some(&&(b_offset, _))) = (a_iter.peek(), b_iter.peek()) {
            let a_abs = a.start_timestamp as u64 + a_offset as u64;
            let b_abs = b.start_timestamp as u64 + b_offset as u64;

            match a_abs.cmp(&b_abs) {
                std::cmp::Ordering::Less => {
                    let &(_, ref val) = a_iter.next().unwrap();
                    let new_offset = (a_abs - new_start as u64) as u16;
                    result.push((new_offset, val.clone()));
                }
                std::cmp::Ordering::Greater => {
                    let &(_, ref val) = b_iter.next().unwrap();
                    let new_offset = (b_abs - new_start as u64) as u16;
                    result.push((new_offset, val.clone()));
                }
                std::cmp::Ordering::Equal => {
                    let &(_, ref a_val) = a_iter.next().unwrap();
                    let &(_, ref b_val) = b_iter.next().unwrap();
                    let new_offset = (a_abs - new_start as u64) as u16;
                    let resolved = resolve_conflict(a_val, b_val);
                    result.push((new_offset, resolved));
                }
            }
        }

        // Push remaining items from either iterator
        for &(off, ref val) in a_iter {
            let new_offset = (a.start_timestamp as u64 + off as u64 - new_start as u64) as u16;
            result.push((new_offset, val.clone()));
        }

        for &(off, ref val) in b_iter {
            let new_offset = (b.start_timestamp as u64 + off as u64 - new_start as u64) as u16;
            result.push((new_offset, val.clone()));
        }

        TimeSeries {
            start_timestamp: new_start,
            series: result,
        }
    }

    pub fn tuple_cartesian_map<TA: Clone, TB: Clone>(
        a: &TimeSeries<TA>, b: &TimeSeries<TB>,
        combine: impl Fn(Option<&TA>, Option<&TB>) -> Option<T>,
    ) -> TimeSeries<T> {
        #[derive(Clone, Copy)]
        enum Event<'a, TA, TB> {
            A(&'a TA),
            B(&'a TB),
        }

        let mut events = vec![];

        events.extend(
            a.series
                .iter()
                .map(|(ts, val)| (0, a.start_timestamp + *ts as u32, Event::A(val))),
        );

        events.extend(
            b.series
                .iter()
                .map(|(ts, val)| (1, b.start_timestamp + *ts as u32, Event::B(val))),
        );

        Self::cartesian_map_inner(events, 2, |events| {
            let input_0 = events[0].as_ref().map(|e| match e {
                Event::A(val) => *val,
                _ => unreachable!(),
            });
            let input_1 = events[1].as_ref().map(|e| match e {
                Event::B(val) => *val,
                _ => unreachable!(),
            });
            combine(input_0, input_1)
        })
    }

    pub fn array_cartesian_map<U: Clone>(
        inputs: &[impl Borrow<Self>], combine: impl Fn(&[Option<&T>]) -> Option<U>,
    ) -> TimeSeries<U> {
        let events = inputs
            .iter()
            .enumerate()
            .flat_map(|(idx, time_series)| {
                let time_series: &Self = time_series.borrow();
                time_series
                    .series
                    .iter()
                    .map(move |(ts, val)| (idx, time_series.start_timestamp + *ts as u32, val))
            })
            .collect();

        TimeSeries::<U>::cartesian_map_inner(events, inputs.len(), combine)
    }

    fn cartesian_map_inner<E: Clone>(
        mut events: Vec<(usize, u32, E)>, input_len: usize,
        combine: impl Fn(&[Option<E>]) -> Option<T>,
    ) -> Self {
        events.sort_unstable_by_key(|(_idx, ts, _val)| *ts);

        let mut start_timestamp = None;
        let mut current_val: Vec<Option<E>> = vec![None; input_len];
        let mut series = vec![];

        for (ts, group) in &events.iter().chunk_by(|(_, ts, _)| ts) {
            for (idx, _, val) in group {
                current_val[*idx] = Some(val.clone());
            }

            let Some(v) = combine(&current_val) else {
                continue;
            };

            let start = *start_timestamp.get_or_insert(*ts);

            series.push(((ts - start) as u16, v));
        }

        Self {
            start_timestamp: start_timestamp.unwrap(),
            series,
        }
    }

    /// Map a function over the TimeSeries values
    pub fn map<U: Clone>(self, f: impl Fn(T) -> U) -> TimeSeries<U> {
        TimeSeries {
            start_timestamp: self.start_timestamp,
            series: self
                .series
                .into_iter()
                .map(|(offset, val)| (offset, f(val)))
                .collect(),
        }
    }
}

impl<T: Clone + PartialEq> TimeSeries<T> {
    pub fn reduce(&mut self) {
        if self.series.is_empty() {
            return;
        }
        let timestamp_offset = self.series[0].0;
        self.start_timestamp += timestamp_offset as u32;

        let mut series = vec![];

        // 使用 group_by 把相同时间戳的值分组，然后处理每组
        for (_val, mut group) in &self.series.iter().chunk_by(|(_, val)| val) {
            let (ts, val) = group.next().unwrap();
            series.push((ts - timestamp_offset, val.clone()))
        }

        self.series = series;
    }
}

impl<T: Clone, U: Clone> TimeSeries<(T, U)> {
    pub fn tuple_cartesian(a: &TimeSeries<T>, b: &TimeSeries<U>) -> TimeSeries<(T, U)> {
        Self::tuple_cartesian_map(a, b, |a, b| Some((a?.clone(), b?.clone())))
    }
}

impl<T: Clone + fmt::Debug> fmt::Debug for TimeSeries<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use chrono::{DateTime, Local, TimeZone};

        let mut debug_list = f.debug_list();

        for (offset, value) in &self.series {
            // Convert start timestamp + offset to DateTime
            let total_seconds = self.start_timestamp as i64 + *offset as i64;
            let naive = DateTime::from_timestamp(total_seconds, 0)
                .unwrap()
                .naive_utc();

            // Convert to local time and format
            let datetime: DateTime<Local> = Local.from_utc_datetime(&naive);

            let timestamp_str = datetime.format("%Y-%m-%d %H:%M:%S").to_string();

            debug_list.entry(&(timestamp_str, value));
        }

        debug_list.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test `new_list` with a list containing duplicate timestamps
    #[test]
    fn test_new_list() {
        let input = vec![
            (3, "a".to_string()),
            (1, "b".to_string()),
            (2, "c".to_string()),
            (1, "d".to_string()),
        ];
        let resolve_conflict = |values: &[&String]| (*values.last().unwrap()).clone();
        let ts = TimeSeries::new_list(input, resolve_conflict);
        assert_eq!(ts.start_timestamp, 1);
        assert_eq!(
            ts.series,
            vec![
                (0, "d".to_string()), /* timestamp 1, "d" is last in input
                                       * for ts=1 */
                (1, "c".to_string()), // timestamp 2
                (2, "a".to_string()), // timestamp 3
            ]
        );
    }

    /// Test `new` as a special case of a single data point
    #[test]
    fn test_new() {
        let ts = TimeSeries::new(5, "x".to_string());
        assert_eq!(ts.start_timestamp, 5);
        assert_eq!(ts.series, vec![(0, "x".to_string())]);
    }

    /// Test `union` with overlapping and non-overlapping timestamps
    #[test]
    fn test_union() {
        let a = TimeSeries {
            start_timestamp: 0,
            series: vec![
                (0, "a0".to_string()), // ts=0
                (2, "a2".to_string()), // ts=2
                (4, "a4".to_string()), // ts=4
            ],
        };
        let b = TimeSeries {
            start_timestamp: 1,
            series: vec![
                (0, "b1".to_string()), // ts=1
                (1, "b2".to_string()), // ts=2
                (2, "b3".to_string()), // ts=3
            ],
        };
        let resolve_conflict = |a: &String, b: &String| format!("{},{}", a, b);
        let union_ts = TimeSeries::union(&a, &b, resolve_conflict);
        assert_eq!(union_ts.start_timestamp, 0); // min(0, 1)
        assert_eq!(
            union_ts.series,
            vec![
                (0, "a0".to_string()),    // ts=0 from a
                (1, "b1".to_string()),    // ts=1 from b
                (2, "a2,b2".to_string()), // ts=2, conflict resolved
                (3, "b3".to_string()),    // ts=3 from b
                (4, "a4".to_string()),    // ts=4 from a
            ]
        );
    }

    /// Test `cartesian_map` with a multiplication function
    #[test]
    fn test_cartesian_map() {
        let a = TimeSeries {
            start_timestamp: 0,
            series: vec![
                (0, 10), // ts=0
                (2, 20), // ts=2
                (4, 40), // ts=4
                (5, 50), // ts=5
                (6, 60), // ts=6
                (7, 70), // ts=7
            ],
        };
        let b = TimeSeries {
            start_timestamp: 1,
            series: vec![
                (0, 100), // ts=1
                (2, 300), // ts=3
                (4, 500), // ts=5
            ],
        };
        let combine = |a: Option<&i32>, b: Option<&i32>| Some(a? + b?);
        let mut result = TimeSeries::tuple_cartesian_map(&a, &b, combine);
        result.reduce();
        assert_eq!(result.start_timestamp, 1); // max(0, 1)
        assert_eq!(
            result.series,
            vec![
                (0, 10 + 100), // ts=1: a@0 with b@1
                (1, 20 + 100), // ts=2: a@2 with b@1
                (2, 20 + 300), // ts=3: a@2 with b@3
                (3, 40 + 300), // ts=4: a@4 with b@3
                (4, 50 + 500),
                (5, 60 + 500),
                (6, 70 + 500),
            ]
        );
    }

    /// Test `map` by doubling the values
    #[test]
    fn test_map() {
        let ts = TimeSeries {
            start_timestamp: 0,
            series: vec![
                (0, 1), // ts=0
                (1, 2), // ts=1
                (2, 3), // ts=2
            ],
        };
        let mapped = ts.map(|x| x * 2);
        assert_eq!(mapped.start_timestamp, 0);
        assert_eq!(
            mapped.series,
            vec![
                (0, 2), // ts=0
                (1, 4), // ts=1
                (2, 6), // ts=2
            ]
        );
    }

    /// Test `cartesian` producing pairs of values
    #[test]
    fn test_cartesian() {
        let a = TimeSeries {
            start_timestamp: 0,
            series: vec![
                (0, 10), // ts=0
                (2, 20), // ts=2
                (4, 40), // ts=4
            ],
        };
        let b = TimeSeries {
            start_timestamp: 1,
            series: vec![
                (0, 100), // ts=1
                (2, 300), // ts=3
            ],
        };
        let mut result = TimeSeries::tuple_cartesian(&a, &b);
        result.reduce();
        assert_eq!(result.start_timestamp, 1); // max(0, 1)
        assert_eq!(
            result.series,
            vec![
                (0, (10, 100)), // ts=1: a@0 with b@1
                (1, (20, 100)), // ts=2: a@2 with b@1
                (2, (20, 300)), // ts=3: a@2 with b@3
                (3, (40, 300)), // ts=4: a@4 with b@3
            ]
        );
    }

    /// Test `new_list` with all identical timestamps
    #[test]
    fn test_new_list_all_same_timestamp() {
        let input = vec![
            (1, "a".to_string()),
            (1, "b".to_string()),
            (1, "c".to_string()),
        ];
        let resolve_conflict = |values: &[&String]| (*values.last().unwrap()).clone();
        let ts = TimeSeries::new_list(input, resolve_conflict);
        assert_eq!(ts.start_timestamp, 1);
        assert_eq!(ts.series, vec![(0, "c".to_string())]); // Last value "c"
    }

    /// Test `union` with identical start timestamps
    #[test]
    fn test_union_same_start() {
        let a = TimeSeries {
            start_timestamp: 0,
            series: vec![
                (0, "a0".to_string()), // ts=0
                (1, "a1".to_string()), // ts=1
            ],
        };
        let b = TimeSeries {
            start_timestamp: 0,
            series: vec![
                (0, "b0".to_string()), // ts=0
                (2, "b2".to_string()), // ts=2
            ],
        };
        let resolve_conflict = |a: &String, b: &String| format!("{},{}", a, b);
        let union_ts = TimeSeries::union(&a, &b, resolve_conflict);
        assert_eq!(union_ts.start_timestamp, 0);
        assert_eq!(
            union_ts.series,
            vec![
                (0, "a0,b0".to_string()), // ts=0, conflict
                (1, "a1".to_string()),    // ts=1 from a
                (2, "b2".to_string()),    // ts=2 from b
            ]
        );
    }

    #[test]
    fn test_reduce_with_duplicates() {
        // 测试含有重复值的序列
        let mut time_series: TimeSeries<String> = TimeSeries {
            start_timestamp: 1000,
            series: vec![
                (10, "value1".to_string()),
                (20, "value2".to_string()),
                (30, "value2".to_string()), // 重复值
                (40, "value3".to_string()),
                (50, "value1".to_string()), // 重复值
            ],
        };

        time_series.reduce();

        // 检查起始时间戳应该增加第一个元素的时间戳偏移
        assert_eq!(time_series.start_timestamp, 1010); // 1000 + 10

        // 检查结果只包含唯一值，且时间戳已调整
        let expected = vec![
            (0, "value1".to_string()),  // 10 - 10 = 0
            (10, "value2".to_string()), // 20 - 10 = 10
            (30, "value3".to_string()), // 40 - 10 = 30
            (40, "value1".to_string()),
        ];

        assert_eq!(time_series.series, expected);
    }

    #[test]
    fn test_reduce_without_duplicates() {
        // 测试没有重复值的序列
        let mut time_series: TimeSeries<i32> = TimeSeries {
            start_timestamp: 500,
            series: vec![(5, 10), (15, 20), (25, 30)],
        };

        time_series.reduce();

        assert_eq!(time_series.start_timestamp, 505); // 500 + 5

        let expected = vec![
            (0, 10),  // 5 - 5 = 0
            (10, 20), // 15 - 5 = 10
            (20, 30), // 25 - 5 = 20
        ];

        assert_eq!(time_series.series, expected);
    }
}
