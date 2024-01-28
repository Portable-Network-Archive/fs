use crate::command::Command;
use crate::filesystem::PnaFS;
use clap::Args;
use fuser::{mount2, MountOption};
use std::fs::create_dir_all;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Args)]
pub(crate) struct MountArgs {
    #[arg()]
    archive: PathBuf,
    #[arg()]
    mount_point: PathBuf,
}

impl Command for MountArgs {
    fn execute(&self) -> io::Result<()> {
        mount_archive(&self.mount_point, &self.archive)
    }
}

fn mount_archive<MountPoint: AsRef<Path>, Archive: AsRef<Path>>(
    mount_point: MountPoint,
    archive: Archive,
) -> io::Result<()> {
    let fs = PnaFS::new(archive.as_ref().into());
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
