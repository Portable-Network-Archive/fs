# PNA-FS

Portable-Network-Archive Filesystem

PNA-FS is a virtual filesystem that allows users to mount their PNA file and interact with it as a regular disk partition.

### Requirements

PNA-FS requires the stable branch of the Rust programming language, which can be installed following the instructions on [rustup.rs](https://rustup.rs). If you already have Rust installed, make sure that it is updated to the latest version (≥1.88):

```bash
$ rustup update stable
```

#### MacOS

On MacOS, PNA-FS requires [macfuse](https://osxfuse.github.io/) and [pkg-config](http://macappstore.org/pkg-config/):

```bash
$ brew install pkg-config; brew install --cask macfuse
```

#### Ubuntu

On Ubuntu, PNA-FS requires [libfuse-dev](https://packages.ubuntu.com/disco/libfuse-dev) and [pkg-config](https://packages.ubuntu.com/disco/pkg-config):

```bash
sudo apt-get install -y libfuse-dev pkg-config
```

#### SUSE

```bash
sudo zypper install -y fuse-devel fuse rust pkgconf-pkg-config
```

#### Other linux distros

Make sure you have `pkg-config` and the `fuse` library installed. These are usually found in the package repositories of major distributions.

#### FreeBSD

Rust can be installed via the `lang/rust` port. You will need to install `sysutils/fusefs-libs` for the `cargo install` command to succeed.

### Installation

After all requirements are met, PNA-FS can be installed using `cargo`:


```bash
$ cargo install --git https://github.com/Portable-Network-Archive/fs.git
```

This will generate the `pnafs` binary in `$HOME/.cargo/bin`. Make sure that this directory is in your `PATH` variable: `export PATH=$PATH:$HOME/.cargo/bin`

### Usage

Mount archive:

```bash
$ pnafs mount archive.pna /mnt/pnafs/
```

### Testing

```bash
cargo test --locked --release
```

Mount-level shell harnesses (POSIX conformance, randomised I/O,
multi-process stress) live under `scripts/tests/` — see
[`scripts/tests/README.md`](scripts/tests/README.md) for what each one
covers, host requirements, and how to reproduce failures.

### Troubleshooting

#### Could not mount to `$mountpoint`: Operation not permitted (os error 1)

This error occurs when `user_allow_other` is not set in `/etc/fuse.conf` or the file has improper permissions. Fix by running (as root):

```bash
# echo 'user_allow_other' >> /etc/fuse.conf
# chmod 644 /etc/fuse.conf
# sudo chown root:root /etc/fuse.conf
```

#### Special files are not persisted

Special files — named pipes (fifo), sockets, and device nodes — are
supported only in memory while a writable archive is mounted. The PNA
format has no on-disk representation for them, so they are dropped
(with a warning) when the archive is saved. Any such node you
create while mounted will disappear from the archive once it is written
back; this is a data-loss risk, so avoid relying on special files inside
a PNA-FS mount.

```bash
$ mkfifo /mnt/pnafs/pipe   # exists during the mount
$ # ...after unmount and reload, /mnt/pnafs/pipe is gone
```

#### archive ... is already mounted by another pnafs instance

pnafs takes a kernel `flock` on a sidecar file (`.{archive-name}.lock`,
created next to the archive) for the lifetime of every mount: read-only
mounts share the lock, a `--write` mount holds it exclusively. This
error means another pnafs process has the archive mounted in a
conflicting mode — unmount it first.

The `.{archive-name}.lock` file is intentionally left in place after
unmount (removing it would race against concurrent mounts); it is empty
and safe to ignore. If the pnafs process dies the kernel releases the
lock automatically, so there is no stale state to clean up.

Note: on network filesystems (NFS in particular) `flock` semantics
depend on the server and mount options, so the multi-mount guard is
only as reliable as the underlying filesystem's `flock` support.

A read-only mount that cannot take the lock for an environmental
reason — the archive directory is not writable (read-only media, a
shared/other-owned directory) so the sidecar cannot be created, or the
filesystem has no `flock` support at all — still succeeds but proceeds
without the cross-process guard (a warning is logged), rather than
failing. An actual lock conflict is never bypassed this way. A
`--write` mount always needs a writable directory — it rewrites the
archive on save — so it keeps the strict lock requirement.
