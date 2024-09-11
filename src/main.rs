pub fn main() {
    let data_path = std::path::Path::new("data/measurements.txt");
    let start_time = std::time::Instant::now();
    let _ = brc::summarize(data_path).unwrap();
    let elapsed = start_time.elapsed();
    println!("{}", elapsed.as_secs_f32());
}
