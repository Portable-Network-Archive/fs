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
