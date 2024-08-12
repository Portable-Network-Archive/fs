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
    #[arg(value_hint = ValueHint::FilePath)]
    archive: PathBuf,
    #[arg(value_hint = ValueHint::DirPath)]
    mount_point: PathBuf,
}

impl Command for MountArgs {
    #[inline]
    fn execute(self) -> io::Result<()> {
        let password = ask_password(self.password)?;
        mount_archive(&self.mount_point, &self.archive, password)
    }
}

fn mount_archive<MountPoint: AsRef<Path>, Archive: AsRef<Path>>(
    mount_point: MountPoint,
    archive: Archive,
    password: Option<String>,
) -> io::Result<()> {
    let fs = PnaFS::new(archive.as_ref().into(), password);
    create_dir_all(&mount_point)?;
    mount2(
        fs,
        mount_point,
        &[
            MountOption::FSName("pnafs".to_owned()),
            // MountOption::AllowRoot,
            MountOption::RO,
        ],
    )?;
    Ok(())
}
