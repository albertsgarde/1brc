use brc::cli::Cli;
use clap::Parser;

pub fn main() {
    let args = Cli::parse();
    args.run();
}
