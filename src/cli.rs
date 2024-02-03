use crate::command::mount::MountArgs;
use clap::{Parser, Subcommand};

#[derive(Parser)]
pub(crate) struct Cli {
    #[clap(subcommand)]
    pub(crate) subcommand: SubCommand,
}

#[derive(Subcommand)]
pub(crate) enum SubCommand {
    Mount(MountArgs),
}

#[derive(Parser, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub(crate) struct PasswordArgs {
    #[arg(
        long,
        help = "Password of archive. If password is not given it's asked from the tty"
    )]
    pub(crate) password: Option<Option<String>>,
}
