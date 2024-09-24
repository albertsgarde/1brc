use std::io::Write;

use clap::Parser;

#[derive(Parser, Debug, Clone)]
struct Cli {
    #[arg(short='n', long, default_value=None)]
    max_bytes: Option<usize>,
    #[arg(short = 'r', long, default_value = "1")]
    repeats: u32,
    #[arg(short = 'p', long, default_value = "8")]
    num_threads: u32,
    #[arg(required = true)]
    versions: Vec<u32>,
}

fn result_to_out(result: &str) -> String {
    result.replace(", ", "\n").replace(['{', '}'], "")
}

pub fn main() {
    let args = Cli::parse();
    assert!(!args.versions.is_empty());
    assert!(args.repeats > 0);
    let data_path = std::path::Path::new("data/measurements.txt");
    let out_path = data_path
        .with_file_name(if let Some(max_bytes) = args.max_bytes {
            format!("measurements_{max_bytes}")
        } else {
            "measurements".to_string()
        })
        .with_extension("out");
    let expected = std::fs::read_to_string(out_path).unwrap();
    // Get number of cpus available.
    let num_slices = usize::try_from(args.num_threads).unwrap();

    let version_funcs = brc::versions();
    let versions = args
        .versions
        .iter()
        .map(|&version_index| version_funcs[version_index as usize])
        .collect::<Vec<_>>();
    let mut runtimes = vec![vec![]; versions.len()];
    for i in 0..args.repeats {
        for (runtime_index, (version, &version_index)) in
            versions.iter().zip(args.versions.iter()).enumerate()
        {
            print!(
                "\rRepeat {i:>2}/{:<2}  Version {version_index}                                    ",
                args.repeats,
            );
            std::io::stdout().flush().unwrap();
            let start_time = std::time::Instant::now();
            let result =
                std::hint::black_box(version(data_path, args.max_bytes, num_slices)).unwrap();
            let runtime = start_time.elapsed();
            runtimes[runtime_index].push(runtime);
            let result = result_to_out(result.as_str());
            result.lines().zip(expected.lines()).enumerate().for_each(
                |(line_index, (out_line, expected))| {
                    if out_line != expected {
                        let output_path = data_path.with_extension("out.err");
                        std::fs::write(output_path, &result).unwrap();
                        panic!("Output for version {version_index} does not match expected on line {}.", line_index);
                    }
                },
            );
        }
    }
    println!("\rResults from {} repetitions:", args.repeats);

    for (runtimes, &version_index) in runtimes.iter().zip(args.versions.iter()) {
        assert_eq!(runtimes.len(), args.repeats as usize);
        let min_time = runtimes.iter().min().unwrap().as_secs_f32();
        let max_time = runtimes.iter().max().unwrap().as_secs_f32();
        let total_time = runtimes.iter().sum::<std::time::Duration>().as_secs_f32();
        let average_time = total_time / args.repeats as f32;
        println!("V{version_index}: {min_time:.2} / {average_time:.2} / {max_time:.2}",);
    }
}
