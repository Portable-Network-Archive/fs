//! Entry locators and on-demand entry decoding.
//!
//! libpna's high-level archive iteration is sequential: it never exposes
//! where an entry's chunk run lives inside the archive image. The chunk
//! layer underneath it, however, is public, and since libpna 0.40 an
//! entry can be rebuilt from its raw chunk run alone
//! ([`NormalEntry::parse_chunks`] / [`SolidEntry::parse_chunks`]). That
//! combination is enough to implement the random-access entry reads that
//! <https://github.com/Portable-Network-Archive/fs/issues/451> asked for
//! without any new upstream API:
//!
//! - [`ChunkRuns`] walks an archive image at chunk level and yields each
//!   entry's chunk run together with the byte offset it starts at.
//! - [`decode_locator`] later seeks to a recorded offset, re-reads just
//!   that run, and decodes it — one entry for a normal run, every member
//!   for a solid block (locators are solid-block granular, matching the
//!   format: solid members share one compressed stream and cannot be
//!   decoded independently).
//!
//! `archive_io::load_lazy` builds trees whose file bodies stay in the
//! archive image; `FileTree::materialize` decodes them on first use via
//! this module.

use pna::{Chunk, ChunkType, EntryName, NormalEntry, RawChunk, ReadOptions, SolidEntry};
use std::io::{self, Cursor, Read, Seek, SeekFrom};

/// Reads one chunk from an in-memory archive image.
///
/// `pna::io::read_chunk` allocates the chunk's *declared* length before
/// discovering whether that many bytes actually remain, so a tiny
/// hostile image claiming a ~4 GiB chunk would force a huge allocation
/// (plus zero-fill) where the slice-based reader the eager loader uses
/// allocates nothing. Mirror the slice reader: the length field counts
/// the chunk data alone, so a complete frame needs `length + 12` bytes
/// (length field, type, data, CRC) — reject up front when that cannot
/// fit in the remaining image, a physical property of any chunk in a
/// single complete image.
fn read_chunk_bounded(cur: &mut Cursor<&[u8]>) -> io::Result<RawChunk> {
    let image = cur.get_ref();
    let pos = cur.position() as usize;
    if let Some(length_field) = image.get(pos..pos + 4) {
        let declared = u64::from(u32::from_be_bytes(
            length_field.try_into().expect("4-byte slice"),
        ));
        let remaining = (image.len() - pos) as u64;
        if declared + 12 > remaining {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "chunk at offset {pos} declares {declared} data bytes with only {remaining} remaining"
                ),
            ));
        }
    }
    pna::io::read_chunk(cur, u32::MAX)
}

/// Where one file's body lives inside its archive image.
///
/// The offset is the byte position of the first chunk of the entry's
/// chunk run (`FHED` for a normal entry, `SHED` for a solid block),
/// counted from the start of the archive image (i.e. including the
/// 8-byte PNA signature).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum EntryLocator {
    /// A standalone entry: the run at `offset` decodes to this file.
    Normal { offset: u64 },
    /// One member of a solid block: the run at `block_offset` decodes
    /// to the whole block; `name` selects this file among its members.
    SolidMember { block_offset: u64, name: EntryName },
}

impl EntryLocator {
    /// Byte offset of the chunk run this locator points at.
    pub(crate) fn run_offset(&self) -> u64 {
        match self {
            EntryLocator::Normal { offset } => *offset,
            EntryLocator::SolidMember { block_offset, .. } => *block_offset,
        }
    }
}

/// Iterates the top-level entry chunk runs of an archive image.
///
/// The walker is structurally identical to libpna's own sequential
/// iterator (`next_raw_item` / `next_raw_item_slice`), so lazy and
/// eager loads accept exactly the same archives:
///
/// - After the signature and the `AHED` header chunk (consumed by
///   [`ChunkRuns::new`]), chunks are accumulated verbatim until either
///   terminator — `FEND` **or** `SEND`, whichever comes first — ends
///   the run. libpna is not run-aware either; it accumulates the same
///   way and lets the entry parser sort out what it collected.
/// - `ANXT` chunks are dropped wherever they appear (split archives
///   carry one after their last entry to announce a following part).
/// - `AEND` ends iteration, discarding whatever was accumulated —
///   trailing junk after the last entry is ignored by both loaders,
///   and a run cut short by `AEND` simply never surfaces.
/// - A completed run must start with `FHED` or `SHED`; anything else
///   (a stray chunk glued to the front of an entry, a `PHSF` placed
///   before its `FHED`, a second `AHED`) is an error, mirroring
///   `ReadEntry`'s "invalid entry" rejection. A run whose terminator
///   does not match its header is passed on as-is: the entry parser
///   rejects it, again exactly as in the eager path.
///
/// No entry body is decoded — the chunks are the raw, still-compressed
/// on-disk representation.
pub(crate) struct ChunkRuns<'a> {
    cur: Cursor<&'a [u8]>,
    done: bool,
}

impl<'a> ChunkRuns<'a> {
    /// Starts iterating `data`, which must begin with the PNA signature
    /// followed by the `AHED` header chunk; both are consumed.
    pub(crate) fn new(data: &'a [u8]) -> io::Result<Self> {
        let mut cur = Cursor::new(data);
        pna::io::read_signature(&mut cur)?;
        let header = read_chunk_bounded(&mut cur)?;
        if header.ty() != ChunkType::AHED {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "expected AHED after the PNA signature, found {}",
                    header.ty()
                ),
            ));
        }
        Ok(Self { cur, done: false })
    }

    /// Reads the next entry chunk run, or `None` at `AEND`.
    ///
    /// The returned offset is the byte position of the run's first
    /// chunk (`FHED` / `SHED`); the chunks are the full accumulated
    /// run, terminator included.
    pub(crate) fn next_run(&mut self) -> io::Result<Option<(u64, Vec<RawChunk>)>> {
        if self.done {
            return Ok(None);
        }
        let mut chunks: Vec<RawChunk> = Vec::new();
        let mut offset = 0;
        loop {
            let pos = self.cur.position();
            let chunk = read_chunk_bounded(&mut self.cur)?;
            match chunk.ty() {
                ChunkType::ANXT => continue,
                ChunkType::AEND => {
                    self.done = true;
                    return Ok(None);
                }
                ChunkType::FEND | ChunkType::SEND => {
                    if chunks.is_empty() {
                        offset = pos;
                    }
                    chunks.push(chunk);
                    break;
                }
                _ => {
                    if chunks.is_empty() {
                        offset = pos;
                    }
                    chunks.push(chunk);
                }
            }
        }
        match chunks[0].ty() {
            ChunkType::FHED | ChunkType::SHED => Ok(Some((offset, chunks))),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid entry: chunk run at offset {offset} starts with {other}"),
            )),
        }
    }
}

/// Re-reads the chunk run starting at `offset` in `data`.
///
/// The first chunk must be `FHED` or `SHED`; accumulation then runs
/// until either terminator (`FEND` / `SEND`), mirroring [`ChunkRuns`].
/// A mismatched terminator is collected, not rejected here — the entry
/// parser rejects it, as in the eager path. `AEND` before a terminator
/// is an error: offsets handed out by a successful scan always point
/// at complete runs, so reaching one means the image is inconsistent.
pub(crate) fn read_run(data: &[u8], offset: u64) -> io::Result<Vec<RawChunk>> {
    let mut cur = Cursor::new(data);
    cur.seek(SeekFrom::Start(offset))?;
    let first = read_chunk_bounded(&mut cur)?;
    match first.ty() {
        ChunkType::FHED | ChunkType::SHED => {}
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("offset {offset} does not start an entry chunk run (found {other})"),
            ));
        }
    }
    let mut chunks = vec![first];
    loop {
        let chunk = read_chunk_bounded(&mut cur)?;
        match chunk.ty() {
            // Stepped over, mirroring ChunkRuns / libpna iteration.
            ChunkType::ANXT => continue,
            ChunkType::AEND => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("chunk run at offset {offset} is cut short by AEND"),
                ));
            }
            ChunkType::FEND | ChunkType::SEND => {
                chunks.push(chunk);
                return Ok(chunks);
            }
            _ => chunks.push(chunk),
        }
    }
}

/// Decodes the chunk run a locator points at.
///
/// Returns the decoded `(name, body)` pairs: exactly one for a normal
/// entry, one per member for a solid block. Decoding a solid member
/// therefore yields its siblings too — callers materializing one member
/// should cache the rest (see `FileTree::materialize`).
pub(crate) fn decode_locator(
    data: &[u8],
    locator: &EntryLocator,
    password: Option<&[u8]>,
) -> io::Result<Vec<(EntryName, Vec<u8>)>> {
    let chunks = read_run(data, locator.run_offset())?;
    let opts = ReadOptions::with_password(password);
    match locator {
        EntryLocator::Normal { .. } => {
            let entry = NormalEntry::parse_chunks(chunks)?;
            let mut body = Vec::new();
            entry.reader(&opts)?.read_to_end(&mut body)?;
            Ok(vec![(entry.header().path().clone(), body)])
        }
        EntryLocator::SolidMember { .. } => {
            let solid = SolidEntry::parse_chunks(chunks)?;
            let mut out = Vec::new();
            for entry in solid.entries(&opts)? {
                let entry = entry?;
                let mut body = Vec::new();
                entry.reader(&opts)?.read_to_end(&mut body)?;
                out.push((entry.header().path().clone(), body));
            }
            Ok(out)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pna::{
        Archive, Compression, Encryption, FileEntryBuilder, SolidEntryBuilder, WriteOptions,
    };
    use std::io::Write as _;

    fn payload(seed: u8, len: usize) -> Vec<u8> {
        (0..len).map(|i| seed ^ (i % 251) as u8).collect()
    }

    fn file(name: &str, data: &[u8], options: WriteOptions) -> io::Result<NormalEntry> {
        let mut b = FileEntryBuilder::new_with_options(name.into(), options)?;
        b.write_all(data)?;
        b.build()
    }

    fn zstd() -> WriteOptions {
        WriteOptions::builder()
            .compression(Compression::ZSTANDARD)
            .build()
    }

    /// Builds an archive holding a plain file, a solid block of two
    /// files, an encrypted file, and a trailing plain file.
    fn sample_archive() -> io::Result<Vec<u8>> {
        let mut ar = Archive::write_header(Vec::new())?;
        ar.add_entry(file("a.txt", b"alpha", WriteOptions::store())?)?;
        let mut solid = SolidEntryBuilder::new(zstd())?;
        solid.add_entry(file("s1.txt", b"solid one", WriteOptions::store())?)?;
        solid.add_entry(file(
            "s2.bin",
            &payload(0x77, 40_000),
            WriteOptions::store(),
        )?)?;
        ar.add_entry(solid.build()?)?;
        let enc = WriteOptions::builder()
            .compression(Compression::ZSTANDARD)
            .encryption(Encryption::AES)
            .password(Some("pw"))
            .try_build()?;
        ar.add_entry(file("secret.bin", &payload(0x42, 10_000), enc)?)?;
        ar.add_entry(file("z.txt", b"omega", WriteOptions::store())?)?;
        ar.finalize()
    }

    /// Sequential decode of the same archive via the high-level API —
    /// the ground truth every random-access result must match.
    fn sequential(data: &[u8]) -> io::Result<Vec<(String, Vec<u8>)>> {
        let opts = ReadOptions::with_password(Some("pw"));
        let mut ar = Archive::read_header(Cursor::new(data))?;
        let mut out = Vec::new();
        for entry in ar.entries_with_options(&opts) {
            let entry = entry?;
            let mut body = Vec::new();
            entry.reader(&opts)?.read_to_end(&mut body)?;
            out.push((entry.header().path().to_string(), body));
        }
        Ok(out)
    }

    #[test]
    fn runs_are_in_archive_order_with_entry_headers_first() -> io::Result<()> {
        let data = sample_archive()?;
        let mut runs = ChunkRuns::new(&data)?;
        let mut kinds = Vec::new();
        let mut prev_end = 0;
        while let Some((offset, chunks)) = runs.next_run()? {
            assert!(offset >= prev_end, "runs must not overlap or go backwards");
            prev_end = offset;
            kinds.push(chunks[0].ty());
        }
        assert_eq!(
            kinds,
            [
                ChunkType::FHED,
                ChunkType::SHED,
                ChunkType::FHED,
                ChunkType::FHED
            ]
        );
        Ok(())
    }

    #[test]
    fn decode_locator_reproduces_sequential_decode_in_reverse_order() -> io::Result<()> {
        let data = sample_archive()?;
        // Record a locator per file the way load_lazy does: normal runs
        // map to themselves, solid members share the block offset.
        let mut locators = Vec::new();
        let mut runs = ChunkRuns::new(&data)?;
        while let Some((offset, chunks)) = runs.next_run()? {
            if chunks[0].ty() == ChunkType::FHED {
                let entry = NormalEntry::parse_chunks(chunks)?;
                locators.push((
                    entry.header().path().to_string(),
                    EntryLocator::Normal { offset },
                ));
            } else {
                let solid = SolidEntry::parse_chunks(chunks)?;
                let opts = ReadOptions::with_password(Some("pw"));
                for entry in solid.entries(&opts)? {
                    let entry = entry?;
                    locators.push((
                        entry.header().path().to_string(),
                        EntryLocator::SolidMember {
                            block_offset: offset,
                            name: entry.header().path().clone(),
                        },
                    ));
                }
            }
        }

        let truth = sequential(&data)?;
        assert_eq!(locators.len(), truth.len());
        // Fetch in reverse order to prove no dependence on scan order.
        let mut got: Vec<(String, Vec<u8>)> = Vec::new();
        for (name, locator) in locators.iter().rev() {
            let members = decode_locator(&data, locator, Some(b"pw"))?;
            let body = members
                .iter()
                .find(|(n, _)| n.to_string() == *name)
                .map(|(_, b)| b.clone())
                .expect("locator decode must contain its own entry");
            got.push((name.clone(), body));
        }
        got.sort();
        let mut truth = truth;
        truth.sort();
        assert_eq!(got, truth);
        Ok(())
    }

    #[test]
    fn anxt_before_aend_is_skipped_like_the_sequential_iterator() -> io::Result<()> {
        let data = sample_archive()?;
        // Locate the AEND chunk by walking chunk framing from the
        // signature, then splice an ANXT chunk in front of it — the
        // layout a split archive's part file ends with.
        let mut cur = Cursor::new(&data);
        pna::io::read_signature(&mut cur)?;
        let aend_offset = loop {
            let pos = cur.position();
            let chunk = pna::io::read_chunk(&mut cur, u32::MAX)?;
            if chunk.ty() == ChunkType::AEND {
                break pos;
            }
        };
        let mut anxt = Vec::new();
        pna::io::write_chunk(&mut anxt, RawChunk::from_data(ChunkType::ANXT, Vec::new()))?;
        let mut spliced = data[..aend_offset as usize].to_vec();
        spliced.extend_from_slice(&anxt);
        spliced.extend_from_slice(&data[aend_offset as usize..]);

        let mut runs = ChunkRuns::new(&spliced)?;
        let mut count = 0;
        while runs.next_run()?.is_some() {
            count += 1;
        }
        assert_eq!(count, 4, "ANXT must not create or destroy entry runs");
        Ok(())
    }

    /// Splits an archive image into `(offset, serialized chunk)` pairs.
    fn chunk_layout(data: &[u8]) -> io::Result<Vec<(u64, ChunkType, Vec<u8>)>> {
        let mut cur = Cursor::new(data);
        pna::io::read_signature(&mut cur)?;
        let mut out = Vec::new();
        loop {
            let pos = cur.position();
            let chunk = pna::io::read_chunk(&mut cur, u32::MAX)?;
            let ty = chunk.ty();
            let mut bytes = Vec::new();
            pna::io::write_chunk(&mut bytes, chunk)?;
            out.push((pos, ty, bytes));
            if ty == ChunkType::AEND {
                return Ok(out);
            }
        }
    }

    fn reassemble(parts: &[Vec<u8>]) -> Vec<u8> {
        let mut out = b"\x89PNA\r\n\x1a\n".to_vec();
        for p in parts {
            out.extend_from_slice(p);
        }
        out
    }

    #[test]
    fn anxt_inside_a_run_is_stepped_over() -> io::Result<()> {
        // libpna's iterator excludes ANXT from entry chunks wherever it
        // appears; a run carrying one mid-stream must scan and decode
        // exactly like the run without it.
        let data = sample_archive()?;
        let layout = chunk_layout(&data)?;
        let first_fhed = layout
            .iter()
            .position(|(_, t, _)| *t == ChunkType::FHED)
            .unwrap();
        let mut anxt = Vec::new();
        pna::io::write_chunk(&mut anxt, RawChunk::from_data(ChunkType::ANXT, Vec::new()))?;
        let mut parts: Vec<Vec<u8>> = layout.iter().map(|(_, _, b)| b.clone()).collect();
        parts.insert(first_fhed + 1, anxt);
        let spliced = reassemble(&parts);

        let mut runs = ChunkRuns::new(&spliced)?;
        let mut kinds = Vec::new();
        while let Some((offset, chunks)) = runs.next_run()? {
            kinds.push(chunks[0].ty());
            assert!(
                chunks.iter().all(|c| c.ty() != ChunkType::ANXT),
                "ANXT must not join the run at offset {offset}"
            );
        }
        assert_eq!(kinds.len(), 4);
        assert_eq!(sequential(&spliced)?, sequential(&data)?);
        Ok(())
    }

    #[test]
    fn phsf_before_fhed_is_rejected_like_the_sequential_iterator() -> io::Result<()> {
        // Move the encrypted entry's PHSF chunk in front of its FHED.
        // libpna's entry parser rejects this layout ("invalid entry"),
        // so ChunkRuns must fail too rather than invent a locator for
        // a run the eager loader cannot read.
        let data = sample_archive()?;
        let layout = chunk_layout(&data)?;
        let phsf = layout
            .iter()
            .position(|(_, t, _)| *t == ChunkType::PHSF)
            .unwrap();
        let fhed = layout
            .iter()
            .position(|(_, t, _)| *t == ChunkType::FHED)
            .unwrap();
        assert!(fhed < phsf);
        let mut parts: Vec<Vec<u8>> = layout.iter().map(|(_, _, b)| b.clone()).collect();
        let phsf_bytes = parts.remove(phsf);
        parts.insert(fhed, phsf_bytes);
        let spliced = reassemble(&parts);

        assert!(
            sequential(&spliced).is_err(),
            "eager ground truth: the sequential iterator must reject this too"
        );
        let mut runs = ChunkRuns::new(&spliced)?;
        assert!(
            runs.next_run().is_err(),
            "a PHSF lead-in must not start a run"
        );
        Ok(())
    }

    #[test]
    fn stray_chunk_between_entries_is_rejected() -> io::Result<()> {
        // An ancillary chunk between two entries is glued into the
        // following entry by libpna's iterator and rejected there;
        // ChunkRuns rejects it at top level. Either way the load fails.
        let data = sample_archive()?;
        let layout = chunk_layout(&data)?;
        let first_fend = layout
            .iter()
            .position(|(_, t, _)| *t == ChunkType::FEND)
            .unwrap();
        let mut anc = Vec::new();
        pna::io::write_chunk(
            &mut anc,
            RawChunk::from_data(
                unsafe { ChunkType::from_unchecked(*b"cmNT") },
                b"hi".to_vec(),
            ),
        )?;
        let mut parts: Vec<Vec<u8>> = layout.iter().map(|(_, _, b)| b.clone()).collect();
        parts.insert(first_fend + 1, anc);
        let spliced = reassemble(&parts);

        assert!(
            sequential(&spliced).is_err(),
            "eager ground truth: the sequential iterator must reject this too"
        );
        let mut runs = ChunkRuns::new(&spliced)?;
        assert!(runs.next_run()?.is_some(), "first entry still scans");
        assert!(
            runs.next_run().is_err(),
            "stray chunk must fail the scan instead of being skipped"
        );
        Ok(())
    }

    #[test]
    fn aend_cutting_a_run_short_ends_iteration() -> io::Result<()> {
        // Keep the first entry and the start of the second, then append
        // AEND: the partial run is discarded, mirroring libpna.
        let data = sample_archive()?;
        let layout = chunk_layout(&data)?;
        let fends: Vec<usize> = layout
            .iter()
            .enumerate()
            .filter(|(_, (_, t, _))| *t == ChunkType::FEND)
            .map(|(i, _)| i)
            .collect();
        let mut parts: Vec<Vec<u8>> = layout[..=fends[0]]
            .iter()
            .map(|(_, _, b)| b.clone())
            .collect();
        parts.push(layout[fends[0] + 1].2.clone()); // header chunk of run 2, unterminated
        let aend = layout.last().unwrap().2.clone();
        parts.push(aend);
        let spliced = reassemble(&parts);

        let mut runs = ChunkRuns::new(&spliced)?;
        assert!(runs.next_run()?.is_some(), "complete entry survives");
        assert!(
            runs.next_run()?.is_none(),
            "partial run is discarded at AEND"
        );
        Ok(())
    }

    #[test]
    fn mismatched_terminator_is_collected_for_the_parser_to_reject() -> io::Result<()> {
        // The first entry is closed by SEND instead of FEND. libpna's
        // iterator ends the raw item at SEND all the same, and its
        // entry parser rejects the run — so the sequential load fails.
        // ChunkRuns must likewise surface the run (not swallow it at
        // the following AEND, which would silently lose the entry).
        let data = sample_archive()?;
        let layout = chunk_layout(&data)?;
        let fend = layout
            .iter()
            .position(|(_, t, _)| *t == ChunkType::FEND)
            .unwrap();
        let mut send = Vec::new();
        pna::io::write_chunk(&mut send, RawChunk::from_data(ChunkType::SEND, Vec::new()))?;
        let mut parts: Vec<Vec<u8>> = layout.iter().map(|(_, _, b)| b.clone()).collect();
        parts[fend] = send;
        let spliced = reassemble(&parts);

        assert!(
            sequential(&spliced).is_err(),
            "eager ground truth: this archive must fail to load"
        );
        let mut runs = ChunkRuns::new(&spliced)?;
        let (_, chunks) = runs
            .next_run()?
            .expect("the mismatched run must surface, not be discarded");
        assert_eq!(chunks.last().map(Chunk::ty), Some(ChunkType::SEND));
        assert!(
            NormalEntry::parse_chunks(chunks).is_err(),
            "the entry parser must reject the mismatched run"
        );
        Ok(())
    }

    #[test]
    fn trailing_junk_before_aend_is_discarded() -> io::Result<()> {
        // An FDAT chunk and a duplicate AHED after the last entry,
        // before AEND: libpna accumulates and discards them at AEND,
        // so the archive loads fine. ChunkRuns must do the same
        // instead of failing the whole scan.
        let data = sample_archive()?;
        let layout = chunk_layout(&data)?;
        let aend_pos = layout.len() - 1;
        let mut junk = Vec::new();
        pna::io::write_chunk(
            &mut junk,
            RawChunk::from_data(ChunkType::FDAT, b"junk".to_vec()),
        )?;
        let mut parts: Vec<Vec<u8>> = layout.iter().map(|(_, _, b)| b.clone()).collect();
        parts.insert(aend_pos, layout[0].2.clone()); // duplicate AHED
        parts.insert(aend_pos, junk); // FDAT junk
        let spliced = reassemble(&parts);

        let mut runs = ChunkRuns::new(&spliced)?;
        let mut count = 0;
        while runs.next_run()?.is_some() {
            count += 1;
        }
        assert_eq!(count, 4, "junk must not create or destroy entry runs");
        assert_eq!(sequential(&spliced)?.len(), sequential(&data)?.len());
        Ok(())
    }

    #[test]
    fn chunk_declaring_more_than_the_image_holds_is_rejected() -> io::Result<()> {
        // Inflate the first FDAT chunk's length field far beyond the
        // image size. The bounded reader must reject it from the frame
        // header alone, like the slice-based eager reader, instead of
        // attempting a multi-gigabyte allocation first.
        let data = sample_archive()?;
        let layout = chunk_layout(&data)?;
        let fdat = layout
            .iter()
            .find(|(_, t, _)| *t == ChunkType::FDAT)
            .unwrap();
        let mut evil = data.clone();
        evil[fdat.0 as usize..fdat.0 as usize + 4].copy_from_slice(&0xFFFF_FFF0u32.to_be_bytes());

        let mut runs = ChunkRuns::new(&evil)?;
        assert!(
            runs.next_run().is_err(),
            "an over-declared chunk length must fail the scan"
        );
        Ok(())
    }

    #[test]
    fn read_run_rejects_offset_that_is_not_a_run_start() -> io::Result<()> {
        let data = sample_archive()?;
        // Offset 0 is the PNA signature, not a chunk run.
        assert!(read_run(&data, 0).is_err());
        // The AHED chunk (right after the signature) is not an entry run.
        assert!(read_run(&data, 8).is_err());
        Ok(())
    }

    #[test]
    fn truncated_image_errors_instead_of_panicking() -> io::Result<()> {
        let data = sample_archive()?;
        let half = &data[..data.len() / 2];
        let mut runs = ChunkRuns::new(half)?;
        let mut result = Ok(());
        loop {
            match runs.next_run() {
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(e) => {
                    result = Err(e);
                    break;
                }
            }
        }
        assert!(result.is_err(), "truncated archive must not scan cleanly");
        Ok(())
    }
}
