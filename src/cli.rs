use crate::command::mount::MountArgs;
use clap::{Parser, Subcommand};
#[cfg(feature = "unstable-generate")]
use clap_complete::Shell;

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
    pub(crate) subcommand: Option<SubCommand>,
    #[cfg(feature = "unstable-generate")]
    #[arg(long, help = "Generate shell auto complete")]
    pub(crate) generate: Option<Shell>,
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
