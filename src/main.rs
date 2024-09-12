use std::env;

pub fn main() {
    let data_path = std::path::Path::new("data/measurements.txt");
    let max_bytes = env::args().nth(1).map(|s| s.parse().unwrap());
    let start_time = std::time::Instant::now();
    let _ = brc::summarize(data_path, max_bytes).unwrap();
    let elapsed = start_time.elapsed();
    println!("{}", elapsed.as_secs_f32());
}
