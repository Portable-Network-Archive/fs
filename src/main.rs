use crate::cli::SubCommand;
use crate::command::Command;
use clap::Parser;
use std::io;

mod cli;
mod command;
mod file_manager;
mod filesystem;

fn main() -> io::Result<()> {
    #[cfg(feature = "logging")]
    simple_logger::init_with_level(log::Level::Trace).unwrap();
    entry()
}

fn entry() -> io::Result<()> {
    let cli = cli::Cli::parse();
    match cli.subcommand {
        SubCommand::Mount(args) => args.execute(),
    }
}
