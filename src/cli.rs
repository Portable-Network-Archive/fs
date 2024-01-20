use crate::command::mount::MountArgs;
use clap::{Parser, Subcommand};

#[derive(Parser)]
pub(crate) struct CLI {
    #[clap(subcommand)]
    pub(crate) subcommand: SubCommand,
}

#[derive(Subcommand)]
pub(crate) enum SubCommand {
    Mount(MountArgs),
}
