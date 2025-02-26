use crate::{
    cli::PasswordArgs,
    command::{ask_password, Command},
    filesystem::PnaFS,
};
use clap::{Args, ValueHint};
use fuser::{mount2, MountOption};
use std::fs::create_dir_all;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Args)]
pub(crate) struct MountArgs {
    #[command(flatten)]
    password: PasswordArgs,
    #[command(flatten)]
    mount_options: MountOptions,
    #[arg(value_hint = ValueHint::FilePath)]
    archive: PathBuf,
    #[arg(value_hint = ValueHint::DirPath)]
    mount_point: PathBuf,
}

#[derive(Args)]
struct MountOptions {
    #[arg(
        long,
        help = "Allow the root user to access this filesystem, in addition to the user who mounted it"
    )]
    allow_root: bool,
    #[arg(
        long,
        help = "Allow all users to access files on this filesystem. By default access is restricted to the user who mounted it"
    )]
    allow_other: bool,
}

impl Command for MountArgs {
    #[inline]
    fn execute(self) -> io::Result<()> {
        let password = ask_password(self.password)?;
        mount_archive(self.mount_point, self.archive, password, self.mount_options)
    }
}

fn mount_archive(
    mount_point: impl AsRef<Path>,
    archive: impl Into<PathBuf>,
    password: Option<String>,
    mount_options: MountOptions,
) -> io::Result<()> {
    let fs = PnaFS::new(archive.into(), password);
    create_dir_all(&mount_point)?;
    mount2(
        fs,
        mount_point,
        &[
            Some(MountOption::FSName("pnafs".to_owned())),
            mount_options.allow_root.then_some(MountOption::AllowRoot),
            mount_options.allow_other.then_some(MountOption::AllowOther),
            Some(MountOption::RO),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>(),
    )?;
    Ok(())
}
