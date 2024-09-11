use std::{collections::HashMap, hash::Hasher, io::Write, path::Path};

use memmap::MmapOptions;
use rustc_hash::{FxBuildHasher, FxHasher};

type HashBuilder = FxBuildHasher;

#[derive(Debug)]
pub struct SummaryError {}

fn to_string(mut data: Vec<(&str, f32, f32, f32, u32)>) -> String {
    data.sort_by_key(|&(key, _, _, _, _)| key);
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

pub fn summarize_slice(slice: &[u8]) -> Vec<(&str, f32, f32, f32, u32)> {
    let mut cur_data: Vec<(&str, f32, f32, f32, u32)> = Vec::new();

    let mut indices: HashMap<u64, usize, HashBuilder> =
        HashMap::with_hasher(HashBuilder::default());

    for (index, line) in slice
        .split(|&c| c == b'\n')
        .filter(|line| !line.is_empty())
        .enumerate()
        .take(10_000_000)
    {
        if index % 1_000_000 == 0 {
            print!("Processed {} million lines\r", index / 1_000_000);
            std::io::stdout().flush().unwrap();
        }

        let mut split = line.split(|&c| c == b';');
        let key = split.next().unwrap();
        let value = fast_float::parse(split.next().unwrap()).unwrap();

        let hash = hash_str(key);

        let index = indices.entry(hash).or_insert_with(|| {
            cur_data.push((
                std::str::from_utf8(key).unwrap(),
                f32::MAX,
                f32::MIN,
                0.0,
                0,
            ));
            cur_data.len() - 1
        });

        let (_name, min, max, total, count) = &mut cur_data[*index];
        *min = min.min(value);
        *max = max.max(value);
        *total += value;
        *count += 1;
    }

    cur_data
}

pub fn summarize(path: impl AsRef<Path>) -> Result<String, SummaryError> {
    let mut cur_data: Vec<(&str, f32, f32, f32, u32)> = Vec::new();

    let mut indices: HashMap<u64, usize, HashBuilder> =
        HashMap::with_hasher(HashBuilder::default());

    // Create buffer for reading file line by line
    let file = std::fs::File::open(path).unwrap();
    let file = unsafe { MmapOptions::new().map(&file).unwrap() };

    for (index, line) in file
        .split(|&c| c == b'\n')
        .filter(|line| !line.is_empty())
        .enumerate()
        .take(10_000_000)
    {
        if index % 1_000_000 == 0 {
            print!("Processed {} million lines\r", index / 1_000_000);
            std::io::stdout().flush().unwrap();
        }

        let mut split = line.split(|&c| c == b';');
        let key = split.next().unwrap();
        let value = fast_float::parse(split.next().unwrap()).unwrap();

        let hash = hash_str(key);

        let index = indices.entry(hash).or_insert_with(|| {
            cur_data.push((
                std::str::from_utf8(key).unwrap(),
                f32::MAX,
                f32::MIN,
                0.0,
                0,
            ));
            cur_data.len() - 1
        });

        let (_name, min, max, total, count) = &mut cur_data[*index];
        *min = min.min(value);
        *max = max.max(value);
        *total += value;
        *count += 1;
    }

    Ok(to_string(cur_data))
}
