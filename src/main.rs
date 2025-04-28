use crate::command::Command;
use clap::Parser;
use std::io;

mod cli;
mod command;
mod file_manager;
mod filesystem;

fn main() -> io::Result<()> {
    let args = cli::Cli::parse();
    #[cfg(feature = "logging")]
    simple_logger::init_with_level(args.verbose.log_level().unwrap_or(log::Level::Trace))
        .map_err(io::Error::other)?;
    args.execute()
}
