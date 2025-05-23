pub(crate) mod bugreport;
pub(crate) mod complete;
pub(crate) mod mount;

use crate::cli::PasswordArgs;
use std::io;

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
