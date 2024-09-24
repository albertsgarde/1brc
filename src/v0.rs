use std::{cmp::Ordering, collections::HashMap, hash::Hasher, path::Path};

use anyhow::Result;
use itertools::Itertools;
use memmap::MmapOptions;
use rayon::iter::{ParallelBridge, ParallelIterator};
use rustc_hash::{FxBuildHasher, FxHasher};

type HashBuilder = FxBuildHasher;

#[derive(Debug)]
pub struct SummaryError {}

struct Summary<'a> {
    data: Vec<(&'a str, f32, f32, f32, u32)>,
}

impl<'a> Summary<'a> {
    fn new() -> Self {
        Self { data: vec![] }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn merge(self, other: Self) -> Self {
        let mut result = vec![];
        let mut a_iter = self.into_iter().peekable();
        let mut b_iter = other.into_iter().peekable();

        let mut cur_a = a_iter.next();
        let mut cur_b = b_iter.next();
        loop {
            if let Some((a_name, a_min, a_max, a_total, a_count)) = cur_a {
                if let Some((b_name, b_min, b_max, b_total, b_count)) = cur_b {
                    match a_name.cmp(b_name) {
                        Ordering::Less => {
                            result.push((a_name, a_min, a_max, a_total, a_count));
                            cur_a = a_iter.next();
                        }
                        Ordering::Equal => {
                            result.push((
                                a_name,
                                a_min.min(b_min),
                                a_max.max(b_max),
                                a_total + b_total,
                                a_count + b_count,
                            ));
                            cur_a = a_iter.next();
                            cur_b = b_iter.next();
                        }
                        Ordering::Greater => {
                            result.push((b_name, b_min, b_max, b_total, b_count));
                            cur_b = b_iter.next();
                        }
                    }
                } else {
                    result.extend(cur_a.into_iter().chain(a_iter));
                    break;
                }
            } else {
                result.extend(cur_b.into_iter().chain(b_iter));
                break;
            }
        }
        Self { data: result }
    }

    fn sort(&mut self) {
        self.data.sort_by_key(|&(key, _, _, _, _)| key);
    }
}

impl<'a> IntoIterator for Summary<'a> {
    type Item = (&'a str, f32, f32, f32, u32);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}

fn to_string(mut data: Summary) -> String {
    data.sort();
    let mut entries = data.into_iter();
    let mut result = "{".to_string();
    if let Some((name, min, max, total, count)) = entries.next() {
        result.push_str(&format!(
            "{name}={min:.1}/{:.1}/{max:.1}",
            ((total / (count as f32)) * 10.).round() / 10.
        ));
    }
    for (name, min, max, total, count) in entries {
        result.push_str(", ");
        result.push_str(&format!(
            "{name}={min:.1}/{:.1}/{max:.1}",
            ((total / (count as f32)) * 10.).round() / 10.
        ));
    }
    result.push_str("}\n");
    result
}

fn hash_str(s: &[u8]) -> u64 {
    let mut hash = FxHasher::default();

    hash.write(s);
    hash.finish()
}

fn find_split_index(slice: &[u8], index: usize) -> usize {
    assert!(index <= slice.len());
    if index == 0 {
        return index;
    }
    let mut split_index = index;
    while index != slice.len() && slice[split_index] != b'\n' {
        split_index += 1;
    }
    split_index + 1
}

fn summarize_slice(slice: &[u8]) -> Summary {
    assert_ne!(slice.last(), Some(&b';'));
    let mut cur_data: Summary = Summary::new();

    let mut indices: HashMap<u64, usize, HashBuilder> =
        HashMap::with_hasher(HashBuilder::default());

    for line in slice.split(|&c| c == b'\n').filter(|line| !line.is_empty()) {
        let mut split = line.split(|&c| c == b';');
        let key = split.next().unwrap();
        let value = fast_float::parse(split.next().unwrap()).unwrap();

        let hash = hash_str(key);

        let index = indices.entry(hash).or_insert_with(|| {
            cur_data.data.push((
                std::str::from_utf8(key).unwrap(),
                f32::MAX,
                f32::MIN,
                0.0,
                0,
            ));
            cur_data.len() - 1
        });

        let (_name, min, max, total, count) = &mut cur_data.data[*index];
        *min = min.min(value);
        *max = max.max(value);
        *total += value;
        *count += 1;
    }

    cur_data.sort();
    cur_data
}

pub fn summarize(path: &Path, max_bytes: Option<usize>, num_threads: usize) -> Result<String> {
    // Create buffer for reading file line by line
    let file = std::fs::File::open(path).unwrap();
    let file = unsafe { MmapOptions::new().map(&file).unwrap() };

    let len = find_split_index(&file, file.len().min(max_bytes.unwrap_or(usize::MAX)));
    let total_slice = &file[..len - 1];

    let summary = (0..=num_threads)
        .map(|i| find_split_index(total_slice, (total_slice.len() * i) / num_threads))
        .tuple_windows()
        .map(|(start, end)| {
            if start == end {
                &total_slice[start..start]
            } else {
                &total_slice[start..(end - 1)]
            }
        })
        .par_bridge()
        .map(|slice| summarize_slice(slice))
        .reduce(Summary::new, |a, b| a.merge(b));
    //.reduce(|a, b| merge_summaries(a, b))
    //.unwrap();

    Ok(to_string(summary))
}
