use crate::command::{complete::CompleteArgs, mount::MountArgs};
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
    #[command(flatten)]
    pub(crate) verbose: clap_verbosity_flag::Verbosity,
}

#[derive(Subcommand)]
pub(crate) enum SubCommand {
    #[command(about = "Mount archive")]
    Mount(MountArgs),
    #[command(about = "Generate shell auto complete")]
    Complete(CompleteArgs),
}

#[derive(Parser, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub(crate) struct PasswordArgs {
    #[arg(
        long,
        help = "Password of archive. If password is not given it's asked from the tty"
    )]
    pub(crate) password: Option<Option<String>>,
}
