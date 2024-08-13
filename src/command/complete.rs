use crate::{cli::Cli, command::Command};
use clap::{Args, CommandFactory};
use clap_complete::{generate, Generator, Shell};
use std::{env, io, path::PathBuf};

#[derive(Args)]
pub(crate) struct CompleteArgs {
    #[arg(help = "shell")]
    pub(crate) shell: Shell,
}

impl Command for CompleteArgs {
    fn execute(self) -> io::Result<()> {
        print_completions(self.shell, &mut Cli::command());
        Ok(())
    }
}

fn print_completions<G: Generator>(generator: G, cmd: &mut clap::Command) {
    let name = env::args().next().map(PathBuf::from).unwrap();
    generate(
        generator,
        cmd,
        name.file_name().unwrap().to_string_lossy(),
        &mut io::stdout(),
    );
}
