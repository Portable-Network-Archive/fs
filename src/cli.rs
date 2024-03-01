use crate::command::mount::MountArgs;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = env!("CARGO_PKG_NAME"),
    version,
    about,
    author,
    arg_required_else_help = true,
)]
pub(crate) struct Cli {
    #[clap(subcommand)]
    pub(crate) subcommand: SubCommand,
}

#[derive(Subcommand)]
pub(crate) enum SubCommand {
    #[command(about = "Mount archive")]
    Mount(MountArgs),
    #[cfg(feature = "unstable-generate")]
    #[command(about = "Generate shell auto complete")]
    Complete(crate::command::complete::CompleteArgs),
}

#[derive(Parser, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub(crate) struct PasswordArgs {
    #[arg(
        long,
        help = "Password of archive. If password is not given it's asked from the tty"
    )]
    pub(crate) password: Option<Option<String>>,
}
