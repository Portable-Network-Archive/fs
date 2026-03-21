use crate::{
    cli::PasswordArgs,
    command::{Command, ask_password},
    filesystem::{PnaFS, WriteStrategy},
};
use clap::{Args, ValueHint};
use fuser::{Config, MountOption, SessionACL, mount2};
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
    #[arg(long, help = "Enable write mode (default: read-only)")]
    write: bool,
    #[arg(
        long,
        default_value = "lazy",
        requires = "write",
        help = "When to flush: lazy (on unmount) or immediate (on file close)"
    )]
    write_strategy: WriteStrategy,
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
    let write_strategy = if mount_options.write {
        Some(mount_options.write_strategy)
    } else {
        None
    };

    let fs = PnaFS::new(archive.into(), password, write_strategy)?;
    create_dir_all(&mount_point)?;

    let acl = if mount_options.allow_other {
        SessionACL::All
    } else if mount_options.allow_root {
        SessionACL::RootAndOwner
    } else {
        SessionACL::Owner
    };

    let mut config = Config::default();
    config.mount_options = vec![MountOption::FSName("pnafs".to_owned())];
    if write_strategy.is_none() {
        config.mount_options.push(MountOption::RO);
    }
    config.acl = acl;

    mount2(fs, mount_point, &config)?;
    Ok(())
}
