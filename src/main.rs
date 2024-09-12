use clap::Parser;

#[derive(Parser, Debug, Clone)]
struct Cli {
    #[arg(default_value=None)]
    max_bytes: Option<usize>,
    #[arg(short = 'o', long)]
    write_output: bool,
}

fn result_to_out(result: &str) -> String {
    result.replace(", ", "\n").replace(['{', '}'], "")
}

pub fn main() {
    let args = Cli::parse();
    let data_path = std::path::Path::new("data/measurements.txt");
    let start_time = std::time::Instant::now();
    let result = brc::summarize(data_path, args.max_bytes).unwrap();
    let elapsed = start_time.elapsed();
    println!("{}", elapsed.as_secs_f32());
    let out_path = data_path
        .with_file_name(if let Some(max_bytes) = args.max_bytes {
            format!("measurements_{max_bytes}")
        } else {
            "measurements".to_string()
        })
        .with_extension("out");

    if args.write_output {
        std::fs::write(out_path, result_to_out(result.as_str())).unwrap();
    } else {
        let expected = std::fs::read_to_string(out_path).unwrap();
        result.lines().zip(expected.lines()).enumerate().for_each(
            |(line_index, (result, expected))| {
                if result != expected {
                    let output_path = data_path.with_extension("out.err");
                    std::fs::write(output_path, result_to_out(result)).unwrap();
                    panic!("Output does not match expected on line {}.", line_index);
                }
            },
        );
    }
}
