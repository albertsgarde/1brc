use std::{cmp::Ordering, collections::HashMap, hash::Hasher, path::Path};

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
    if slice.is_empty() {
        return Summary::new();
    }

    assert_ne!(slice.last(), Some(&b';'));
    let mut cur_data: Summary = Summary::new();

    let mut indices: HashMap<u64, usize, HashBuilder> =
        HashMap::with_hasher(HashBuilder::default());

    let mut index = 0;
    assert_ne!(slice.last(), Some(&b'.'));

    loop {
        if slice.get(index) == Some(&b'\n') {
            index += 1;
            continue;
        }
        if index != 0 {
            assert_eq!(slice[index - 1], b'\n');
            assert_ne!(slice.get(index), Some(&b'\n'));
        }
        if let Some(&name_start) = slice.get(index) {
            assert_ne!(
                name_start, b';',
                "A line should never start with a semicolon."
            );
        } else {
            assert_eq!(slice.get(index - 1), Some(&b'\n'));
            break;
        };
        let name_start_index = index;
        index += 1;
        loop {
            if let Some(&c) = slice.get(index) {
                if c == b';' {
                    break;
                }
                index += 1;
            } else {
                unreachable!("Input should never end in the middle of a name.");
            }
        }
        let name_end_index = index;
        let name = &slice[name_start_index..name_end_index];
        index += 1;
        let negative = if let Some(&first_value_byte) = slice.get(index) {
            if first_value_byte == b'-' {
                index += 1;
                true
            } else {
                false
            }
        } else {
            unreachable!("Input should never end right after a semicolon.");
        };
        let mut value = if let Some(&first_digit) = slice.get(index) {
            assert!(
                first_digit.is_ascii_digit(),
                "Value should start with a digit."
            );
            (first_digit - b'0') as i32
        } else {
            unreachable!("Input should never end right after a semicolon or negative sign.");
        };
        index += 1;
        assert!(slice.len() >= index + 2);
        loop {
            if let Some(&b) = slice.get(index) {
                if b == b'.' {
                    index += 1;
                    break;
                }
                assert!(
                    b.is_ascii_digit(),
                    "Value should only contain digits and a single period."
                );
                value = value * 10 + (b - b'0') as i32;
                index += 1;
            } else {
                unreachable!("Input should never end in the middle of a value.");
            }
        }
        assert!(slice[index - 1] == b'.');
        let decimal = slice
            .get(index)
            .expect("Values should contain exactly one decimal.");
        assert!(decimal.is_ascii_digit());
        let value = (value * 10 + (decimal - b'0') as i32) * if negative { -1 } else { 1 };
        let value = value as f32;

        index += 1;
        if let Some(&new_line) = slice.get(index) {
            if new_line == b'\n' {
                index += 1;
            } else {
                unreachable!("Values should end with a newline.");
            }
        } else {
            break;
        }

        let hash = hash_str(name);

        let index = indices.entry(hash).or_insert_with(|| {
            cur_data.data.push((
                std::str::from_utf8(name).unwrap(),
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

pub fn summarize(path: impl AsRef<Path>, max_bytes: Option<usize>) -> Result<String, SummaryError> {
    // Get number of cpus available.
    let num_threads = num_cpus::get();

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
