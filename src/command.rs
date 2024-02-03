pub(crate) mod mount;

use crate::cli::{Cli, PasswordArgs, SubCommand};
#[cfg(feature = "unstable-generate")]
use clap::CommandFactory;
#[cfg(feature = "unstable-generate")]
use clap_complete::{generate, Generator};
use std::io;
#[cfg(feature = "unstable-generate")]
use std::{env, path::PathBuf};

pub(crate) fn entry(args: Cli) -> io::Result<()> {
    #[cfg(feature = "unstable-generate")]
    if let Some(shell) = args.generate {
        print_completions(shell, &mut Cli::command());
        return Ok(());
    }
    match args.subcommand {
        Some(SubCommand::Mount(args)) => args.execute(),
        None => Ok(()),
    }
}

pub(crate) trait Command {
    fn execute(self) -> io::Result<()>;
}

fn ask_password(args: PasswordArgs) -> io::Result<Option<String>> {
    Ok(match args.password {
        Some(password @ Some(_)) => {
            eprintln!("warning: Using a password on the command line interface can be insecure.");
            password
        }
        Some(None) => Some(rpassword::prompt_password("Enter password: ")?),
        None => None,
    })
}

#[cfg(feature = "unstable-generate")]
fn print_completions<G: Generator>(gen: G, cmd: &mut clap::Command) {
    let name = env::args().next().map(PathBuf::from).unwrap();
    generate(
        gen,
        cmd,
        name.file_name().unwrap().to_string_lossy(),
        &mut io::stdout(),
    );
}
