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
    // DefaultPermissions makes the kernel enforce standard UNIX permission
    // checks against attr.uid/gid/mode before forwarding ops to FUSE. Without
    // it, pnafs would silently honour writes from any uid, which breaks every
    // test that expects EACCES on an unprivileged operation.
    config.mount_options = vec![
        MountOption::FSName("pnafs".to_owned()),
        MountOption::DefaultPermissions,
    ];
    if write_strategy.is_none() {
        config.mount_options.push(MountOption::RO);
    }
    config.acl = acl;

    mount2(fs, mount_point, &config)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::WriteStrategy;
    use crate::cli::{Cli, SubCommand};
    use clap::Parser;

    fn parse_mount(args: &[&str]) -> Result<super::MountOptions, clap::Error> {
        let argv = ["pnafs", "mount"]
            .iter()
            .copied()
            .chain(args.iter().copied())
            .chain(["archive.pna", "mnt"]);
        let cli = Cli::try_parse_from(argv)?;
        match cli.subcommand {
            SubCommand::Mount(m) => Ok(m.mount_options),
            _ => unreachable!(
                "argv is hard-coded with the \"mount\" subcommand, so clap cannot \
                 parse any other variant here"
            ),
        }
    }

    #[test]
    fn default_is_read_only() {
        let opts = parse_mount(&[]).unwrap();
        assert!(!opts.write);
        assert!(opts.write_strategy == WriteStrategy::Lazy);
    }

    #[test]
    fn write_enables_default_lazy_strategy() {
        let opts = parse_mount(&["--write"]).unwrap();
        assert!(opts.write);
        assert!(opts.write_strategy == WriteStrategy::Lazy);
    }

    #[test]
    fn write_strategy_immediate_with_write() {
        let opts = parse_mount(&["--write", "--write-strategy", "immediate"]).unwrap();
        assert!(opts.write);
        assert!(opts.write_strategy == WriteStrategy::Immediate);
    }

    #[test]
    fn write_strategy_immediate_requires_write() {
        let err = match parse_mount(&["--write-strategy", "immediate"]) {
            Err(e) => e,
            Ok(_) => panic!("--write-strategy without --write should be a parse error"),
        };
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn allow_root_parses() {
        let opts = parse_mount(&["--allow-root"]).unwrap();
        assert!(opts.allow_root);
        assert!(!opts.allow_other);
    }

    #[test]
    fn allow_other_parses() {
        let opts = parse_mount(&["--allow-other"]).unwrap();
        assert!(opts.allow_other);
        assert!(!opts.allow_root);
    }
}
