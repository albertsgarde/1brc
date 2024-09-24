use clap::{Args, Parser, Subcommand};
use std::{io::Write, path::PathBuf, process::Command};

#[derive(Parser, Debug, Clone)]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,
}

impl Cli {
    pub fn run(&self) {
        match &self.command {
            Commands::Bench(bench) => bench.run(),
            Commands::Base(base) => base.run(),
            Commands::Flame(flame) => flame.run(),
        }
    }
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    Bench(Bench),
    Base(Base),
    Flame(Flame),
}

#[derive(Args, Debug, Clone)]
struct Bench {
    #[arg(short='n', long, default_value=None)]
    max_bytes: Option<usize>,
    #[arg(short = 'r', long, default_value = "1")]
    repeats: u32,
    #[arg(short = 'p', long, default_value = "8")]
    num_threads: u32,
    #[arg(short = 'f', long, default_value = "measurements")]
    data_name: String,
    #[arg(required = true)]
    versions: Vec<u32>,
}

fn paths(data_name: &str, max_bytes: Option<usize>) -> (PathBuf, PathBuf) {
    let data_path = std::path::Path::new("data")
        .join(data_name)
        .with_extension("txt");
    let out_path = data_path
        .with_file_name(if let Some(max_bytes) = max_bytes {
            format!("{data_name}_{max_bytes}")
        } else {
            data_name.to_string()
        })
        .with_extension("out");
    (data_path, out_path)
}

fn result_to_out(result: &str) -> String {
    result.replace(", ", "\n").replace(['{', '}'], "")
}

impl Bench {
    pub fn run(&self) {
        assert!(!self.versions.is_empty());
        assert!(self.repeats > 0);
        let (data_path, out_path) = paths(self.data_name.as_str(), self.max_bytes);
        let expected = std::fs::read_to_string(out_path).unwrap();
        // Get number of cpus available.
        let num_slices = usize::try_from(self.num_threads).unwrap();

        let version_funcs = crate::versions();
        let versions = self
            .versions
            .iter()
            .map(|&version_index| version_funcs[version_index as usize])
            .collect::<Vec<_>>();
        let mut runtimes = vec![vec![]; versions.len()];
        for i in 0..self.repeats {
            for (runtime_index, (version, &version_index)) in
                versions.iter().zip(self.versions.iter()).enumerate()
            {
                print!(
                "Repeat {i:>2}/{:<2}  Version {version_index}                                    \r",
                self.repeats,
            );
                std::io::stdout().flush().unwrap();
                let start_time = std::time::Instant::now();
                let result =
                    std::hint::black_box(version(data_path.as_path(), self.max_bytes, num_slices))
                        .unwrap();
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
        println!("Results from {} repetitions:", self.repeats);

        for (runtimes, &version_index) in runtimes.iter().zip(self.versions.iter()) {
            assert_eq!(runtimes.len(), self.repeats as usize);
            let min_time = runtimes.iter().min().unwrap().as_secs_f32();
            let max_time = runtimes.iter().max().unwrap().as_secs_f32();
            let total_time = runtimes.iter().sum::<std::time::Duration>().as_secs_f32();
            let average_time = total_time / self.repeats as f32;
            println!("V{version_index}: {min_time:.2} / {average_time:.2} / {max_time:.2}",);
        }
    }
}

#[derive(Args, Debug, Clone)]
struct Base {
    #[arg(short='n', long, default_value=None)]
    max_bytes: Option<usize>,
    #[arg(short = 's', long, default_value = "8")]
    num_threads: u32,
    #[arg(short = 'f', long, default_value = "measurements")]
    data_name: String,
    #[arg(required = true)]
    version: u32,
}

impl Base {
    pub fn run(&self) {
        let (data_path, out_path) = paths(self.data_name.as_str(), self.max_bytes);
        if out_path.exists() {
            eprintln!("Output file already exists: {out_path:?}",);
            return;
        }
        // Get number of cpus available.
        let num_slices = usize::try_from(self.num_threads).unwrap();

        let version = crate::versions()[self.version as usize];
        let result = version(data_path.as_path(), self.max_bytes, num_slices).unwrap();

        let result = result_to_out(result.as_str());

        std::fs::write(out_path.as_path(), result).unwrap();
        println!("Base line output written to {out_path:?}",);
    }
}

#[derive(Args, Debug, Clone)]
struct Flame {
    #[arg(short='n', long, default_value=None)]
    max_bytes: Option<usize>,
    #[arg(short = 'r', long, default_value = "1")]
    repeats: u32,
    #[arg(short = 'p', long, default_value = "8")]
    num_threads: u32,
    #[arg(short = 'f', long, default_value = "measurements")]
    data_name: String,
    #[arg(required = true)]
    version: u32,
}

impl Flame {
    pub fn run(&self) {
        let mut build_command = Command::new("cargo");
        build_command.args(["build", "--release"]);
        build_command.spawn().unwrap().wait().unwrap();

        let (_data_path, output_path) = paths(self.data_name.as_str(), self.max_bytes);
        if !output_path.exists() {
            eprintln!("Output does not exist. Please run `brc base` with the same arguments to generate a base output first.",);
            return;
        }

        let mut command = Command::new("samply");
        command.args([
            "record",
            "target/release/brc",
            "bench",
            &format!("{}", self.version),
            "-r",
            &format!("{}", self.repeats),
            "-p",
            &format!("{}", self.num_threads),
            "-f",
            &self.data_name,
        ]);
        if let Some(max_bytes) = self.max_bytes {
            command.args(["-n", &format!("{max_bytes}")]);
        }
        command.spawn().unwrap().wait().unwrap();
    }
}
