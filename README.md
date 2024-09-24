# 1BRC
## Usage
Run a benchmark of versions 1 and 2 with the following command:
```bash
cargo run --release -- bench 1 2 -r 8
```
The `-r` flag specifies the number of repetitions.
Run ```cargo run --release -- -h``` for more information on the available flags.

The benchmark will interleave the runs of each version in order to avoid any bias due to the order of execution.
It will output the minimum, average, and maximum execution times of each version.

## Versions
### `v0`
A basic implementation using `rayon` for parallelism and making heavy use of `std` in the hot code.

This usually runs in about 7.5 seconds on my machine when given 8 cores.

### `v1`
Replaces the iteration based parsing with custom parser logic, including some bit manipulation in `find_delimiter_long` to efficiently find the semicolon.
The custom parsing logic uses many properties of the input that the `std` version cannot take advantage of.

This version has by far the greatest performance improvement over the previous with almost a 2x speedup on most runs.

### `v2`
The previous version had a `HashMap` to map station names to indices and a separate `Vec` to store the station information.
Combining this into a single `HashMap` gave a small (~5%) but consistent speedup.

### `v3`
Hashing is done separately from the `HashMap` lookup in `v2`.
This is not an issue in itself, except that the `HashMap` rehashes the hash.
To fix this, we use the `nohash-hasher` crate to get the `HashMap` to use the hash directly.
This gives a tiny (~3%) but consistent speedup.

### `vno_assert`
This is a version of `v3` with all `assert`s made into `debug_assert`s.
This is reliably faster, but the difference is small enough that I prefer to keep the `assert`s in the code for now.

### `vbatch`
This is an attempt at batching parsing with each thread to get some SIMD action or parallelize some memory fetches.
I don't understand either of those that well so I'm not sure if this is the right way to go about it.
It performs significantly worse than `v3` on my machine, but the flame graph shows some signs that less time is spent on cache misses, 
just not enough to make up for the overhead of the batching.

## Attempts
### Value parsing with bit manipulation
I tried to parse the values with bit manipulation, but this was slower than the previous implementation.