use std::{cmp::Ordering, collections::HashMap, hash::Hasher, path::Path};

use anyhow::Result;
use itertools::Itertools;
use memmap::MmapOptions;
use nohash_hasher::BuildNoHashHasher;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rustc_hash::FxHasher;

type HashBuilder = BuildNoHashHasher<u64>;

#[derive(Debug, Clone, Copy)]
struct SummaryEntry<'a> {
    name: &'a str,
    min: i32,
    max: i32,
    total: i64,
    count: u32,
}

impl<'a> SummaryEntry<'a> {
    fn new(name: &'a str) -> Self {
        Self {
            name,
            min: i32::MAX,
            max: i32::MIN,
            total: 0,
            count: 0,
        }
    }

    fn into_string(self) -> String {
        let Self {
            name,
            min,
            max,
            total,
            count,
        } = self;
        let min_negative = min < 0;
        let min = min.abs();
        let max_negative = max < 0;
        let max = max.abs();
        let mean_negative = total < 0;
        let total = total.abs();

        let min_integer = min / 10;
        let min_decimal = min % 10;
        let max_integer = max / 10;
        let max_decimal = max % 10;

        let min_sign = if min_negative { "-" } else { "" };
        let max_sign = if max_negative { "-" } else { "" };
        let mean_sign = if mean_negative { "-" } else { "" };

        let mean_times_ten = (total / count as i64)
            + if (total.abs() % count as i64) * 2 >= count as i64 {
                1
            } else {
                0
            };
        let mean_integer = mean_times_ten / 10;
        let mean_decimal = mean_times_ten % 10;

        format!(
            "{name}={min_sign}{min_integer}.{min_decimal}/{mean_sign}{mean_integer}.{mean_decimal}/{max_sign}{max_integer}.{max_decimal}",
        )
    }

    #[inline(always)]
    fn update(&mut self, value: i32) {
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.total += value as i64;
        self.count += 1;
    }
}

struct Summary<'a> {
    data: Vec<SummaryEntry<'a>>,
}

impl<'a> Summary<'a> {
    fn new() -> Self {
        Self { data: vec![] }
    }

    fn from_hashmap(data: HashMap<u64, SummaryEntry<'a>, HashBuilder>) -> Self {
        Self {
            data: {
                let mut vec: Vec<_> = data.into_values().collect();
                vec.sort_by_key(|entry| entry.name);
                vec
            },
        }
    }

    #[cfg(test)]
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
            if let Some(a) = cur_a {
                if let Some(b) = cur_b {
                    match a.name.cmp(b.name) {
                        Ordering::Less => {
                            result.push(a);
                            cur_a = a_iter.next();
                        }
                        Ordering::Equal => {
                            result.push(SummaryEntry {
                                min: a.min.min(b.min),
                                max: a.max.max(b.max),
                                total: a.total + b.total,
                                count: a.count + b.count,
                                ..a
                            });
                            cur_a = a_iter.next();
                            cur_b = b_iter.next();
                        }
                        Ordering::Greater => {
                            result.push(b);
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
        self.data.sort_by_key(|entry| entry.name);
    }

    fn into_result(mut self) -> String {
        self.sort();
        let mut entries = self.into_iter();
        let mut result = "{".to_string();
        if let Some(entry) = entries.next() {
            result.push_str(&entry.into_string());
        }
        for entry in entries {
            result.push_str(", ");
            result.push_str(&entry.into_string());
        }
        result.push_str("}\n");
        result
    }
}

impl<'a> IntoIterator for Summary<'a> {
    type Item = SummaryEntry<'a>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}

fn find_delimiter_long<const DELIM: u8>(word: u128) -> u8 {
    const SPREADER: u128 = 0x0101_0101_0101_0101_0101_0101_0101_0101;
    let delim_pattern: u128 = DELIM as u128 * SPREADER;
    let input = word ^ delim_pattern;
    let processed_input = input.wrapping_sub(SPREADER) & !input & (0x80 * SPREADER);
    processed_input.trailing_zeros() as u8 >> 3 // The position of the first ; byte, or 16 if there is none.
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

    let mut cur_data: HashMap<u64, SummaryEntry, HashBuilder> =
        HashMap::with_hasher(HashBuilder::default());

    let mut index = 0;
    assert_ne!(slice.last(), Some(&b'.'));

    while index < slice.len() {
        if slice.get(index) == Some(&b'\n') {
            index += 1;
            continue;
        }

        assert_ne!(
            slice.get(index),
            Some(&b';'),
            "A line should never start with a semicolon."
        );

        if index != 0 {
            assert_eq!(slice[index - 1], b'\n');
            assert_ne!(slice.get(index), Some(&b'\n'));
        }

        let name_start_index = index;

        while let Some(word_slice) = slice.get(index..index + 16) {
            let word = u128::from_le_bytes(word_slice.try_into().unwrap());
            let delimiter_offset = find_delimiter_long::<b';'>(word) as usize;
            index += delimiter_offset;
            if delimiter_offset != 16 {
                break;
            }
        }
        while slice[index] != b';' {
            index += 1;
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

        let hash = hash_str(name);

        let city_data = cur_data
            .entry(hash)
            .or_insert_with(|| SummaryEntry::new(std::str::from_utf8(name).unwrap()));

        city_data.update(value);

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
    }

    Summary::from_hashmap(cur_data)
}

pub fn summarize(path: &Path, max_bytes: Option<usize>, num_slices: usize) -> Result<String> {
    // Create buffer for reading file line by line
    let file = std::fs::File::open(path).unwrap();
    let file = unsafe { MmapOptions::new().map(&file).unwrap() };

    let len = find_split_index(&file, file.len().min(max_bytes.unwrap_or(usize::MAX)));
    let total_slice = &file[..len - 1];

    let slices = (0..=num_slices)
        .map(|i| find_split_index(total_slice, (total_slice.len() * i) / num_slices))
        .tuple_windows()
        .map(|(start, end)| {
            if start == end {
                &total_slice[start..start]
            } else {
                &total_slice[start..(end - 1)]
            }
        })
        .collect::<Vec<_>>();
    let summaries: Vec<Summary> = slices
        .into_par_iter()
        .map(|slice| summarize_slice(slice))
        .collect();
    let summary = summaries.into_iter().reduce(|a, b| a.merge(b)).unwrap();

    Ok(summary.into_result())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn single() {
        let slice = &[75, 117, 110, 109, 105, 110, 103, 59, 49, 57, 46, 56];
        let summary = summarize_slice(slice);
        assert_eq!(summary.len(), 1);
    }
}
