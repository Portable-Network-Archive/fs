use clap::Parser;
use std::io;

mod cli;
mod command;
mod file_manager;
mod filesystem;

fn main() -> io::Result<()> {
    #[cfg(feature = "logging")]
    simple_logger::init_with_level(log::Level::Trace).map_err(io::Error::other)?;
    command::entry(cli::Cli::parse())
}
