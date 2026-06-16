# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog], and this project follows [Semantic Versioning].

<!-- next-header -->
## [Unreleased] - ReleaseDate

### Added

- Added `CHANGELOG.md` using a Keep a Changelog-style release history.
- Added cargo-release changelog replacement rules for future release-prep PRs.
- Implemented `statfs` so `df` reflects archive-tree capacity and usage.
- Added sidecar file locking to reject conflicting concurrent mounts of the same archive.

### Changed

- Updated release-prep automation to run cargo-release changelog replacements.
- Implemented read/write FUSE support for PNA archives.
- Adapted archive loading and saving to libpna 0.34.
- Expanded filesystem conformance and stress coverage in CI.
- Pinned the generated release workflow actions to commit SHAs.

### Fixed

- Preserved archive uid and gid when local user or group names cannot be resolved.
- Made xattr archive serialization deterministic.
- Preserved symlink target roots and reloaded symlink sizes.
- Returned `EIO` instead of panicking when the shared tree lock is poisoned.
- Anchored mountpoint detection in shell tests to avoid substring false positives.

### Tests

- Added mutation-sequence property tests for rename, hardlink, fallocate, symlink creation, setattr, copy-file-range, and encrypted archives.
- Added regression coverage for read-only mount `EROFS`, double-mount rejection, symlink edge targets, orphan lifecycle behavior, hardlink equivalence, and copy-file-range destination bytes.

### Documentation

- Documented special-file persistence limits and moved the note into troubleshooting.
- Included `README.md` as crate-level documentation.

### Dependencies

- Updated `pna` to 0.34.0, `log` to 0.4.33, `rpassword` to 7.5.4, and `clap_complete` to 4.6.6.

## [0.0.10] - 2026-05-14

### Changed

- Migrated to fuser 0.17.0 and added macOS `macos-no-mount` CI coverage.
- Raised the Rust version requirement from 1.85 to 1.88.
- Enriched crate metadata for crates.io.
- Synced the README MSRV with `Cargo.toml`.

### Dependencies

- Updated `pna` through 0.33.0, `fuser` to 0.17.0, `bugreport` to 0.6.0, `simple_logger` to 5.2.0, `libc` to 0.2.186, `memmap2` to 0.9.10, `nix` to 0.31.3, `clap` to 4.6.1, `clap_complete` to 4.6.5, and release/CI actions.

## [0.0.9] - 2026-02-15

### Changed

- Added the `release-prep` workflow using `cargo-release`.
- Migrated source compatibility for pna 0.29.x.

### Dependencies

- Updated `pna` through 0.29.3, `nix` to 0.31.1, `memmap2` to 0.9.9, `clap` to 4.5.58, `clap_complete` to 4.5.66, `log` to 0.4.29, and GitHub Actions dependencies.

## [0.0.8] - 2025-05-15

### Breaking

- Raised the MSRV to 1.85 and migrated the crate to Rust 2024 edition.

### Changed

- Pinned GitHub Actions to commit hashes to reduce supply-chain risk.
- Introduced a single test entry point and renamed mount test scripts.
- Set up a development container.
- Removed the FreeBSD runner from CI.

### Fixed

- Fixed the `setup-rust` action's rustc version detection.
- Fixed clippy warnings and test-script cleanup behavior.

### Dependencies

- Updated `pna` to 0.25.0, `rpassword` to 7.4.0, `nix` to 0.30.1, `clap` to 4.5.38, `clap_complete` to 4.5.50, and CI dependencies.

## [0.0.7] - 2025-04-03

### Added

- Added the `bug-report` subcommand backed by the `bugreport` crate.
- Added MSRV CI coverage.

### Changed

- Raised the MSRV from 1.77 to 1.80.
- Updated CI scripts to call `cargo +{{ channel }}` explicitly.

### Dependencies

- Added `bugreport`.
- Updated `pna` to 0.24.0, `clap` to 4.5.35, `clap_complete` to 4.5.47, `log` to 0.4.27, and `libc` to 0.2.171.

## [0.0.6] - 2025-02-28

### Added

- Added mount options for `--allow_root` and `allow_other`.

### Changed

- Avoided a CI panic on the FreeBSD nightly channel.
- Simplified internals with `impl Trait` syntax.

### Dependencies

- Updated `pna` through 0.23.0, `clap` to 4.5.31, `clap_complete` to 4.5.46, `log` to 0.4.26, and `libc` to 0.2.170.

## [0.0.5] - 2025-01-20

### Added

- Added Linux arm CI and release coverage.
- Added `aarch64-unknown-linux-gnu` as a release binary target.

### Changed

- Declared `package.rust-version` as 1.77.
- Enabled merge queue (`merge_group`) CI triggers.

### Dependencies

- Updated `pna` through 0.21.1, `fuser` to 0.15.1, `clap-verbosity-flag` to 3.0.2, `cargo-dist` to 0.28.0, and other CLI/CI dependencies.

## [0.0.4] - 2024-10-31

### Changed

- Migrated to pna 0.19.0.
- Migrated to fuser 0.15.0.
- Split pna and pnafs options in CI scripts.
- Added coverage for archives created without a directory pattern.

### Dependencies

- Updated `pna` to 0.19.0, `fuser` to 0.15.0, `cargo-dist` to 0.24.1, `memmap2` to 0.9.5, and related CLI/CI dependencies.

## [0.0.3] - 2024-09-06

### Changed

- Added `memmap2` and moved archive loading toward slice-friendly APIs.
- Inlined the internal `State` type.

### Dependencies

- Updated `pna` to 0.18.0, `cargo-dist` to 0.22.1, and related CLI/CI dependencies.

## [0.0.2] - 2024-08-13

### Added

- Added verbosity support to the command line.
- Added website generation through oranda and configured project homepage metadata.

### Changed

- Prepared code for Rust 2024 edition compatibility (renamed reserved `gen` identifiers, added `dep:` feature prefixes).
- Removed stored password state from `FileManager`.
- Simplified install documentation and logging startup error handling.

### Dependencies

- Added `clap-verbosity-flag`.
- Updated `pna` through 0.16.0, `cargo-dist` to 0.20.0, and related CLI/CI dependencies.

## [0.0.1] - 2024-07-02

### Added

- Added xattr support for archive entries and xattr-preserving integration checks.
- Added publish workflow and crate metadata such as description.
- Added beta and nightly Rust channel CI coverage.

### Changed

- Migrated to pna 0.13.0.
- Re-enabled the temporarily disabled workflow and cleaned up duplicated CI steps.

### Dependencies

- Updated `pna` through 0.13.0, `nix` to 0.29.0, `simple_logger` to 5.0.0, `cargo-dist` to 0.17.0, and related CLI/CI dependencies.

## [0.0.0] - 2024-03-23

### Added

- Initial `pnafs` FUSE archive filesystem implementation.
- Added lookup, read, permission, uid/gid, atime, raw-size, and encrypted archive mount support.
- Added shell completion generation through the `complete` subcommand.
- Added mount integration tests and GitHub release CI.

### Changed

- Renamed the crate from `pna-fs` to `pnafs`.
- Stabilized the completion subcommand naming.

### Fixed

- Removed `AllowRoot` by default.
- Fixed directory creation when child file entries appeared before parent directory entries.
- Avoided panics in early CLI and file-manager paths.

<!-- next-url -->
[Unreleased]: https://github.com/Portable-Network-Archive/fs/compare/0.0.10...HEAD
[0.0.10]: https://github.com/Portable-Network-Archive/fs/compare/0.0.9...0.0.10
[0.0.9]: https://github.com/Portable-Network-Archive/fs/compare/0.0.8...0.0.9
[0.0.8]: https://github.com/Portable-Network-Archive/fs/compare/0.0.7...0.0.8
[0.0.7]: https://github.com/Portable-Network-Archive/fs/compare/0.0.6...0.0.7
[0.0.6]: https://github.com/Portable-Network-Archive/fs/compare/0.0.5...0.0.6
[0.0.5]: https://github.com/Portable-Network-Archive/fs/compare/0.0.4...0.0.5
[0.0.4]: https://github.com/Portable-Network-Archive/fs/compare/0.0.3...0.0.4
[0.0.3]: https://github.com/Portable-Network-Archive/fs/compare/0.0.2...0.0.3
[0.0.2]: https://github.com/Portable-Network-Archive/fs/compare/0.0.1...0.0.2
[0.0.1]: https://github.com/Portable-Network-Archive/fs/compare/0.0.0...0.0.1
[0.0.0]: https://github.com/Portable-Network-Archive/fs/releases/tag/0.0.0
[Keep a Changelog]: https://keepachangelog.com/en/1.0.0/
[Semantic Versioning]: https://semver.org/spec/v2.0.0.html
