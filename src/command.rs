pub(crate) mod complete;
pub(crate) mod mount;

use crate::cli::{Cli, PasswordArgs, SubCommand};
use std::io;

pub(crate) fn entry(args: Cli) -> io::Result<()> {
    match args.subcommand {
        SubCommand::Mount(args) => args.execute(),
        SubCommand::Complete(args) => args.execute(),
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
