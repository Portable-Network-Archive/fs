use crate::{
    cli::PasswordArgs,
    command::{Command, ask_password},
    filesystem::PnaFS,
};
use clap::{ArgGroup, Args, ValueHint};
use fuser::{MountOption, mount2};
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
#[command(
    group(ArgGroup::new("mount_mode").args(["read_only", "read_write"])),
)]
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
    #[arg(long, help = "Mount the filesystem in read-only mode (default)")]
    read_only: bool,
    #[arg(long, help = "Mount the filesystem in read-write mode")]
    read_write: bool,
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
            Some(if mount_options.read_write {
                MountOption::RW
            } else {
                MountOption::RO
            }),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>(),
    )?;
    Ok(())
}
