#![doc = include_str!("../README.md")]

use crate::command::Command;
use clap::Parser;
use std::io;

mod archive_io;
mod cli;
mod command;
mod file_tree;
mod filesystem;

#[cfg(test)]
mod roundtrip_proptest;

fn main() -> io::Result<()> {
    let args = cli::Cli::parse();
    #[cfg(feature = "logging")]
    simple_logger::init_with_level(args.verbose.log_level().unwrap_or(log::Level::Trace))
        .map_err(io::Error::other)?;
    args.execute()
}
