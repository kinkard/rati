//! S3-backed tar archive: index parsing, tile lookups, and S3 I/O.
//!
//! Loads the tar index from an S3 object via range requests, then serves individual
//! tiles by reading their byte ranges on demand. Follows the same two-step protocol
//! as Valhalla's `GraphReader::load_remote_tar_offsets()`:
//! 1. Fetch bytes [0, 512) — the first tar header — and verify it's `index.bin`.
//! 2. Fetch bytes [512, 512 + size) — the raw index data — and parse it.

use bytes::Bytes;
use rustc_hash::FxHashMap;

/// Size of a POSIX tar header block.
const TAR_BLOCK_SIZE: usize = 512;
/// Size of a single index entry in bytes: u64 + u32 + u32.
const TILE_INDEX_ENTRY_SIZE: usize = 16;
/// Name of the index file that must be the first entry in the tar.
const TILE_INDEX_FILE_NAME: &str = "index.bin";

/// Byte offset of `dataset_id_` (`u64`) within Valhalla's `GraphTileHeader`.
///
/// Layout (272-byte POD struct, see `graphtileheader.h`):
///   - bytes  0..8:   bitfield (graphid_, density_, name_quality_, speed_quality_, exit_quality_, has_elevation_, has_ext_directededge_)
///   - bytes  8..16:  base_ll_ (std::array<float, 2>)
///   - bytes 16..32:  version_ (std::array<char, 16>)
///   - bytes 32..40:  dataset_id_ (uint64_t)
const GRAPH_TILE_HEADER_SIZE: usize = 272;
const DATASET_ID_OFFSET: usize = 32;

/// Valhalla tile id that encodes `level | (tile_index << 3)`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TileId(u32);

impl TileId {
    pub fn new(raw: u32) -> Self {
        Self(raw)
    }

    /// Parse a Valhalla tile path like `2/000/818/660.gph` into a packed ID.
    ///
    /// Mirrors `get_tile_id()` from `valhalla_build_extract`:
    /// strip extension, split off level, join remaining digits → `level | (index << 3)`.
    ///
    /// Accepts any single extension (`.gph`, `.csv`, `.spd`, etc.) or no extension at all.
    /// The extension is not validated — the caller decides what content type is expected.
    pub fn from_path(path: &str) -> Option<Self> {
        // Strip any single extension: find the last '.' that comes after the last '/'
        let last_slash = path.rfind('/').unwrap_or(0);
        let stem = match path[last_slash..].rfind('.') {
            Some(dot_pos) => &path[..last_slash + dot_pos],
            None => path,
        };

        let (level_str, idx_str) = stem.split_once('/')?;
        let level: u32 = level_str.parse().ok()?;
        let tile_index: u32 = idx_str.replace('/', "").parse().ok()?;
        Some(Self(level | (tile_index << 3)))
    }
}

impl std::fmt::Display for TileId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Matches Valhalla's `tile_index_entry` layout (16 bytes LE: offset u64, tile_id u32, size u32).
struct TileIndexEntry {
    tile_id: TileId,
    /// Byte offset from the start of the tar archive.
    offset: u64,
    size: u32,
}

impl TileIndexEntry {
    /// Parse a single entry from a 16-byte little-endian slice.
    /// Matches the https://github.com/valhalla/valhalla/blob/master/scripts/valhalla_build_extract
    fn from_bytes(data: &[u8; TILE_INDEX_ENTRY_SIZE]) -> Self {
        Self {
            offset: u64::from_le_bytes(data[0..8].try_into().unwrap()),
            tile_id: TileId(u32::from_le_bytes(data[8..12].try_into().unwrap())),
            size: u32::from_le_bytes(data[12..16].try_into().unwrap()),
        }
    }
}

/// (offset, size) — byte range of a tile within the tar archive.
type TileIndex = FxHashMap<TileId, (u64, u32)>;

/// Parse index from raw `index.bin` bytes.
fn parse_index(data: &[u8]) -> Result<TileIndex, TarError> {
    if !data.len().is_multiple_of(TILE_INDEX_ENTRY_SIZE) {
        return Err(TarError::InvalidIndexSize {
            size: data.len(),
            entry_size: TILE_INDEX_ENTRY_SIZE,
        });
    }

    let count = data.len() / TILE_INDEX_ENTRY_SIZE;
    let mut entries: TileIndex = FxHashMap::with_capacity_and_hasher(count, Default::default());

    for chunk in data.chunks_exact(TILE_INDEX_ENTRY_SIZE) {
        let e = TileIndexEntry::from_bytes(chunk.try_into().unwrap());
        entries.insert(e.tile_id, (e.offset, e.size));
    }

    if entries.is_empty() {
        return Err(TarError::EmptyIndex);
    }

    Ok(entries)
}

/// Build an index by sequentially scanning tar headers.
///
/// This is the fallback for archives that don't have `index.bin` as the first entry
/// (e.g., speed tile archives). It iterates every tar header in the archive, parses
/// filenames into `TileId`s, and records their (offset, size). Non-tile entries
/// (filenames that don't parse as tile paths) are silently skipped.
///
/// `data` must be the entire tar archive content (or at least all headers and entry
/// data up to the last tile).
fn parse_index_from_tar_scan(data: &[u8]) -> Result<TileIndex, TarError> {
    let mut entries = FxHashMap::default();

    let mut pos = 0usize;
    while pos + TAR_BLOCK_SIZE <= data.len() {
        // Check for end-of-archive marker. POSIX requires two consecutive zero blocks,
        // but we stop at the first one — this is safe because a valid tar entry always
        // has a non-zero header (at minimum the checksum field), so a single zero block
        // unambiguously signals the end of meaningful content.
        let header_block = &data[pos..pos + TAR_BLOCK_SIZE];
        if header_block.iter().all(|&b| b == 0) {
            break;
        }

        let header_array: &[u8; TAR_BLOCK_SIZE] = header_block
            .try_into()
            .map_err(|_| TarError::InvalidHeader("header block too short".into()))?;
        let header = TarHeader::parse(header_array)?;

        let data_offset = (pos + TAR_BLOCK_SIZE) as u64;
        let data_size = header.size;

        // Try to parse filename as a tile path; skip non-tile entries (e.g., index.bin, directories)
        if let Some(tile_id) = TileId::from_path(&header.name) {
            if data_size > u32::MAX as u64 {
                tracing::warn!(
                    "Skipping tile {} ({} bytes): exceeds u32::MAX size limit",
                    header.name,
                    data_size
                );
            } else {
                entries.insert(tile_id, (data_offset, data_size as u32));
            }
        }

        // Advance past header + data (padded to 512-byte boundary)
        let data_blocks = (data_size as usize).div_ceil(TAR_BLOCK_SIZE);
        pos += TAR_BLOCK_SIZE + data_blocks * TAR_BLOCK_SIZE;
    }

    if entries.is_empty() {
        return Err(TarError::EmptyIndex);
    }
    entries.shrink_to_fit();

    Ok(entries)
}

/// Parse the first tar header from raw bytes and extract the index.bin file content range.
///
/// Returns `(data_offset, data_size)` — the byte range within the archive where `index.bin`
/// content lives. The caller should read `archive[data_offset..data_offset + data_size]` to
/// get the raw index data, then pass it to [`parse_index`].
fn read_index_header(header_bytes: &[u8; TAR_BLOCK_SIZE]) -> Result<(u64, u64), TarError> {
    let header = TarHeader::parse(header_bytes)?;

    // The first entry must be index.bin — graphreader.cc:147 enforces this
    if header.name != TILE_INDEX_FILE_NAME {
        return Err(TarError::FirstEntryNotIndex {
            actual: header.name,
        });
    }

    let data_offset = TAR_BLOCK_SIZE as u64; // data starts right after the 512-byte header
    let data_size = header.size;

    Ok((data_offset, data_size))
}

/// Minimal POSIX tar header parser.
///
/// Only extracts fields we need: `name` and `size`. Verifies the header checksum
/// following the same algorithm as `tar::header_t::verify()` in `sequence.h:638-651`.
struct TarHeader {
    name: String,
    size: u64,
}

impl TarHeader {
    fn parse(raw: &[u8; TAR_BLOCK_SIZE]) -> Result<Self, TarError> {
        // Verify checksum (sequence.h:638-651)
        // The checksum is computed over the entire header with the chksum field treated as spaces
        let stored_checksum = octal_to_u64(&raw[148..156]);
        let mut unsigned_sum = 0u64;
        let mut signed_sum = 0i64;
        for (i, &byte) in raw.iter().enumerate() {
            let b = if (148..156).contains(&i) {
                b' ' // treat chksum field as spaces
            } else {
                byte
            };
            unsigned_sum += b as u64;
            signed_sum += (b as i8) as i64;
        }
        if stored_checksum != unsigned_sum && stored_checksum as i64 != signed_sum {
            return Err(TarError::InvalidHeader("checksum mismatch".into()));
        }

        // Extract null-terminated name (first 100 bytes)
        let name_end = raw[..100].iter().position(|&b| b == 0).unwrap_or(100);
        let name = std::str::from_utf8(&raw[..name_end])
            .map_err(|_| TarError::InvalidHeader("name is not valid UTF-8".into()))?
            .to_string();

        // Extract size (octal ASCII in bytes 124-136)
        let size = octal_to_u64(&raw[124..136]);

        Ok(Self { name, size })
    }
}

/// Parse an octal ASCII field from a tar header, handling trailing NULs and spaces.
///
/// Mirrors `tar::header_t::octal_to_int()` in `sequence.h:610-627`.
fn octal_to_u64(field: &[u8]) -> u64 {
    // Find the end of meaningful content (skip trailing NULs and spaces)
    let end = field
        .iter()
        .rposition(|&b| b != 0 && b != b' ')
        .map(|i| i + 1)
        .unwrap_or(0);

    // Parse octal digits
    let mut result = 0u64;
    for &byte in &field[..end] {
        if (b'0'..=b'7').contains(&byte) {
            result = result * 8 + (byte - b'0') as u64;
        }
        // Skip non-octal chars (spaces, NULs can appear before digits in some tars)
    }
    result
}

/// Metadata consumed once at startup: logging, tile headers, status endpoint.
pub struct ArchiveMeta {
    /// S3 object ETag (includes quotes, e.g. `"abc123"`).
    pub etag: Box<str>,
    /// S3 object Last-Modified as an HTTP-date string.
    pub last_modified: Box<str>,
    /// Dataset identifier: extracted from graph tile header, CLI override, or S3 ETag fallback.
    pub dataset_id: Box<str>,
    /// Number of tiles in the index.
    pub tile_count: usize,
}

pub struct S3Archive {
    client: aws_sdk_s3::Client,
    bucket: Box<str>,
    key: Box<str>,
    index: TileIndex,
}

impl S3Archive {
    /// Connect to S3 and load the tar index.
    ///
    /// `url` must be in the form `s3://bucket/path/to/key`.
    /// Uses the default AWS credential chain (SSO, IRSA, env vars, IMDS).
    ///
    /// If the first tar entry is not `index.bin` and `scan_index` is true, falls back
    /// to scanning the entire archive's tar headers to build the index. This requires
    /// downloading the full archive and can be slow for large files.
    ///
    /// `dataset_id_override` — if provided, used as the dataset ID instead of auto-detection.
    ///
    /// Returns the archive (for tile fetches) and metadata (consumed once at startup).
    pub async fn open(
        url: &str,
        scan_index: bool,
        dataset_id_override: Option<&str>,
    ) -> Result<(Self, ArchiveMeta), S3Error> {
        let (bucket, key) = parse_s3_url(url)
            .ok_or_else(|| S3Error::Protocol(format!("expected s3:// URL, got: {url}")))?;
        let client = aws_sdk_s3::Client::new(
            &aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await,
        );

        // Fetch S3 object metadata (ETag, Last-Modified) via HeadObject
        let head = client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| S3Error::Request(format!("HeadObject failed: {e}")))?;

        let etag: Box<str> = head
            .e_tag()
            .ok_or_else(|| S3Error::Protocol("S3 HeadObject returned no ETag".into()))?
            .into();

        let last_modified: Box<str> = head
            .last_modified()
            .and_then(|dt| {
                dt.fmt(aws_sdk_s3::primitives::DateTimeFormat::HttpDate)
                    .ok()
            })
            .ok_or_else(|| S3Error::Protocol("S3 HeadObject returned no Last-Modified".into()))?
            .into();

        // Step 1: Read the first 512-byte tar header
        let header_bytes = get_range(&client, bucket, key, 0, 512).await?;
        let header: &[u8; 512] = header_bytes
            .as_ref()
            .try_into()
            .map_err(|_| S3Error::Protocol("tar header shorter than 512 bytes".into()))?;

        // Step 2: Try to load index.bin; fall back to tar scan if missing and --scan-index is set
        let index = match read_index_header(header) {
            Ok((data_offset, data_size)) => {
                let index_bytes = get_range(&client, bucket, key, data_offset, data_size).await?;
                parse_index(&index_bytes).map_err(S3Error::Tar)?
            }
            Err(TarError::FirstEntryNotIndex { .. }) if scan_index => {
                tracing::warn!(
                    "index.bin not found in archive; scanning tar headers to build index. \
                     This requires reading the full archive and may be slow for large files."
                );
                let all_bytes = get_all(&client, bucket, key).await?;
                parse_index_from_tar_scan(&all_bytes).map_err(S3Error::Tar)?
            }
            Err(TarError::FirstEntryNotIndex { .. }) => {
                return Err(S3Error::Tar(TarError::MissingIndexNoScan));
            }
            Err(e) => return Err(S3Error::Tar(e)),
        };

        // Step 3: Determine the dataset ID
        let dataset_id: Box<str> = if let Some(override_id) = dataset_id_override {
            tracing::info!("Using CLI-provided dataset ID: {override_id}");
            override_id.into()
        } else {
            // Try to auto-detect from the first tile's GraphTileHeader
            match detect_dataset_id(&client, bucket, key, &index).await {
                Ok(id) => {
                    tracing::info!("Auto-detected dataset ID from graph tile header: {id}");
                    id.to_string().into()
                }
                Err(e) => {
                    // Fall back to S3 ETag
                    tracing::warn!(
                        "Could not detect dataset ID from tile header ({e}); \
                         falling back to S3 ETag: {etag}"
                    );
                    etag.clone()
                }
            }
        };

        let tile_count = index.len();

        let archive = Self {
            client,
            bucket: bucket.into(),
            key: key.into(),
            index,
        };

        let meta = ArchiveMeta {
            etag,
            last_modified,
            dataset_id,
            tile_count,
        };

        Ok((archive, meta))
    }

    /// Fetch a tile by its ID. Returns `None` if the tile is not in the index.
    pub async fn get_tile(&self, tile_id: TileId) -> Result<Option<Bytes>, S3Error> {
        let Some(&(offset, size)) = self.index.get(&tile_id) else {
            return Ok(None);
        };

        let data = get_range(&self.client, &self.bucket, &self.key, offset, size as u64).await?;
        Ok(Some(data))
    }
}

async fn get_range(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    offset: u64,
    length: u64,
) -> Result<Bytes, S3Error> {
    if length == 0 {
        return Ok(Bytes::new());
    }
    let range = format!("bytes={}-{}", offset, offset + length - 1);
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .range(&range)
        .send()
        .await
        .map_err(|e| S3Error::Request(format!("{e}")))?;

    let data = resp
        .body
        .collect()
        .await
        .map_err(|e| S3Error::Request(format!("reading response body: {e}")))?
        .into_bytes();

    Ok(data)
}

/// Used for the `--scan-index` fallback when we need to scan all tar headers.
async fn get_all(client: &aws_sdk_s3::Client, bucket: &str, key: &str) -> Result<Bytes, S3Error> {
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| S3Error::Request(format!("{e}")))?;

    let data = resp
        .body
        .collect()
        .await
        .map_err(|e| S3Error::Request(format!("reading response body: {e}")))?
        .into_bytes();

    Ok(data)
}

/// Try to detect the dataset ID by reading the first tile's `GraphTileHeader`.
///
/// Picks an arbitrary tile from the index, reads the first 272 bytes (the header),
/// and extracts the `dataset_id` field (`u64` at byte offset 32).
///
/// Returns an error if no tiles are in the index, the tile is too small, or
/// the `dataset_id` field is zero (likely not a graph tile).
async fn detect_dataset_id(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    index: &TileIndex,
) -> Result<u64, S3Error> {
    let &(offset, size) = index
        .values()
        .next()
        .ok_or_else(|| S3Error::Protocol("no tiles in index to read header from".into()))?;

    // We need at least GRAPH_TILE_HEADER_SIZE bytes from the tile
    let read_size = GRAPH_TILE_HEADER_SIZE as u64;
    if (size as u64) < read_size {
        return Err(S3Error::Protocol(format!(
            "tile is only {} bytes, too small for GraphTileHeader ({} bytes)",
            size, GRAPH_TILE_HEADER_SIZE
        )));
    }

    let data = get_range(client, bucket, key, offset, read_size).await?;
    let dataset_id = parse_dataset_id(&data)?;

    if dataset_id == 0 {
        return Err(S3Error::Protocol(
            "dataset_id is 0; tile may not be a graph tile".into(),
        ));
    }

    Ok(dataset_id)
}

/// Extract `dataset_id` from a raw `GraphTileHeader` byte slice.
///
/// The `dataset_id_` field is a little-endian `u64` at byte offset 32 within the
/// 272-byte header.
fn parse_dataset_id(header: &[u8]) -> Result<u64, S3Error> {
    if header.len() < DATASET_ID_OFFSET + 8 {
        return Err(S3Error::Protocol(format!(
            "header too short for dataset_id: {} bytes (need at least {})",
            header.len(),
            DATASET_ID_OFFSET + 8
        )));
    }
    let bytes: [u8; 8] = header[DATASET_ID_OFFSET..DATASET_ID_OFFSET + 8]
        .try_into()
        .unwrap();
    Ok(u64::from_le_bytes(bytes))
}

fn parse_s3_url(url: &str) -> Option<(&str, &str)> {
    let path = url.strip_prefix("s3://")?;
    path.split_once('/')
}

#[derive(Debug, thiserror::Error)]
pub enum TarError {
    #[error("index.bin size {size} is not a multiple of entry size {entry_size}")]
    InvalidIndexSize { size: usize, entry_size: usize },

    #[error("index.bin is empty")]
    EmptyIndex,

    #[error("first tar entry must be 'index.bin', got '{actual}'")]
    FirstEntryNotIndex { actual: String },

    #[error("archive has no index.bin; re-run with --scan-index to build index from tar headers")]
    MissingIndexNoScan,

    #[error("invalid tar header: {0}")]
    InvalidHeader(String),
}

#[derive(Debug, thiserror::Error)]
pub enum S3Error {
    #[error("S3 request failed: {0}")]
    Request(String),

    #[error("{0}")]
    Protocol(String),

    #[error(transparent)]
    Tar(TarError),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal tar header for a file with the given name and size.
    fn make_tar_header(name: &str, size: u64) -> [u8; TAR_BLOCK_SIZE] {
        let mut header = [0u8; TAR_BLOCK_SIZE];

        // name (bytes 0-99)
        header[..name.len()].copy_from_slice(name.as_bytes());

        // size as octal ASCII (bytes 124-135), null-terminated
        let size_str = format!("{:011o}\0", size);
        header[124..136].copy_from_slice(size_str.as_bytes());

        // typeflag = '0' (regular file) at byte 156
        header[156] = b'0';

        // magic = "ustar\0" at bytes 257-262
        header[257..263].copy_from_slice(b"ustar\0");

        // version = "00" at bytes 263-264
        header[263..265].copy_from_slice(b"00");

        // Compute checksum: treat chksum field (148-155) as spaces
        let mut sum = 0u64;
        for (i, &byte) in header.iter().enumerate() {
            if (148..156).contains(&i) {
                sum += b' ' as u64;
            } else {
                sum += byte as u64;
            }
        }
        let chksum_str = format!("{:06o}\0 ", sum);
        header[148..156].copy_from_slice(chksum_str.as_bytes());

        header
    }

    /// Build index.bin content from a slice of (offset, tile_id, size) tuples.
    fn make_index_bin(entries: &[(u64, u32, u32)]) -> Vec<u8> {
        let mut data = Vec::with_capacity(entries.len() * TILE_INDEX_ENTRY_SIZE);
        for &(offset, tile_id, size) in entries {
            data.extend_from_slice(&offset.to_le_bytes());
            data.extend_from_slice(&tile_id.to_le_bytes());
            data.extend_from_slice(&size.to_le_bytes());
        }
        data
    }

    /// Build a complete tar archive in memory from a list of (filename, data) entries.
    /// Appends a two-block end-of-archive marker.
    fn make_tar_archive(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut archive = Vec::new();
        for &(name, data) in entries {
            let header = make_tar_header(name, data.len() as u64);
            archive.extend_from_slice(&header);
            archive.extend_from_slice(data);
            // Pad to 512-byte boundary
            let padding = (TAR_BLOCK_SIZE - (data.len() % TAR_BLOCK_SIZE)) % TAR_BLOCK_SIZE;
            archive.extend(std::iter::repeat_n(0u8, padding));
        }
        // End-of-archive: two zero blocks
        archive.extend(std::iter::repeat_n(0u8, TAR_BLOCK_SIZE * 2));
        archive
    }

    #[test]
    fn from_path_gph_test() {
        let id = TileId::from_path("2/000/818/660.gph").unwrap();
        assert_eq!(id.0, 2 | (818660 << 3));

        let id = TileId::from_path("2/000/818/660.csv").unwrap();
        assert_eq!(id.0, 2 | (818660 << 3));

        let id = TileId::from_path("2/000/818/660.spd").unwrap();
        assert_eq!(id.0, 2 | (818660 << 3));

        let id = TileId::from_path("2/000/818/660").unwrap();
        assert_eq!(id.0, 2 | (818660 << 3));

        let id = TileId::from_path("0/000/529.gph").unwrap();
        assert_eq!(id.0, 529 << 3);

        let id = TileId::from_path("0/000/529").unwrap();
        assert_eq!(id.0, 529 << 3);

        // invalid
        assert!(TileId::from_path("").is_none());
        assert!(TileId::from_path("660.gph").is_none());
        assert!(TileId::from_path("660.gph").is_none());
        assert!(TileId::from_path("abc/000/818/660.gph").is_none());
        assert!(TileId::from_path("2/abc/818/660.gph").is_none());
    }

    #[test]
    fn parse_index_header() {
        let header = make_tar_header("index.bin", 48); // 3 entries x 16 bytes
        let (offset, size) = read_index_header(&header).unwrap();
        assert_eq!(offset, 512);
        assert_eq!(size, 48);
    }

    #[test]
    fn reject_non_index_first_entry() {
        let header = make_tar_header("0/000/529.gph", 1024);
        let err = read_index_header(&header).unwrap_err();
        assert!(matches!(err, TarError::FirstEntryNotIndex { .. }));
    }

    #[test]
    fn parse_index_entries() {
        // Tile IDs: level 0 tile 529 = 0 | (529 << 3) = 0x1088
        //           level 2 tile 744881 = 2 | (744881 << 3) = 0x005B1B0A
        let entries = [(3281408, 0x1088u32, 648u32), (5000000, 0x005B1B0A, 12345)];
        let data = make_index_bin(&entries);
        let index = parse_index(&data).unwrap();

        assert_eq!(index.len(), 2);

        assert_eq!(index[&TileId::new(0x1088)], (3281408, 648));
        assert_eq!(index[&TileId::new(0x005B1B0A)], (5000000, 12345));

        assert!(index.get(&TileId::new(0xDEAD)).is_none());
    }

    #[test]
    fn reject_invalid_index_size() {
        let data = vec![0u8; 17]; // not a multiple of 16
        let err = parse_index(&data).unwrap_err();
        assert!(matches!(err, TarError::InvalidIndexSize { .. }));
    }

    #[test]
    fn reject_empty_index() {
        let err = parse_index(&[]).unwrap_err();
        assert!(matches!(err, TarError::EmptyIndex));
    }

    #[test]
    fn octal_parsing() {
        // Standard octal: "00000031400\0" = 13056 in decimal
        assert_eq!(octal_to_u64(b"00000031400\0"), 13056);
        // With trailing spaces
        assert_eq!(octal_to_u64(b"0000144 \0\0\0\0"), 100);
        // Zero
        assert_eq!(octal_to_u64(b"00000000000\0"), 0);
    }

    #[test]
    fn scan_index_single_tile() {
        let tile_data = b"fake tile content";
        let archive = make_tar_archive(&[("2/000/818/660.gph", tile_data)]);

        let index = parse_index_from_tar_scan(&archive).unwrap();
        assert_eq!(index.len(), 1);

        let expected_id = TileId::from_path("2/000/818/660.gph").unwrap();
        // Data starts right after the 512-byte header
        assert_eq!(
            index[&expected_id],
            (TAR_BLOCK_SIZE as u64, tile_data.len() as u32)
        );
    }

    #[test]
    fn scan_index_multiple_tiles() {
        let data1 = b"tile one";
        let data2 = b"tile two data here";
        let data3 = b"third";
        let archive = make_tar_archive(&[
            ("2/000/818/660.gph", data1),
            ("0/000/529.csv", data2),
            ("2/000/100/200.spd", data3),
        ]);

        let index = parse_index_from_tar_scan(&archive).unwrap();
        assert_eq!(index.len(), 3);

        assert_eq!(
            index[&TileId::from_path("2/000/818/660.gph").unwrap()].1,
            data1.len() as u32
        );
        assert_eq!(
            index[&TileId::from_path("0/000/529.csv").unwrap()].1,
            data2.len() as u32
        );
        assert_eq!(
            index[&TileId::from_path("2/000/100/200.spd").unwrap()].1,
            data3.len() as u32
        );
    }

    #[test]
    fn scan_index_skips_non_tile_entries() {
        let tile_data = b"real tile";
        let archive = make_tar_archive(&[
            ("index.bin", b"not a real index but whatever" as &[u8]),
            ("2/000/818/660.gph", tile_data),
            ("metadata.json", b"{}"),
        ]);

        let index = parse_index_from_tar_scan(&archive).unwrap();
        // Only the tile entry should be indexed; index.bin and metadata.json are skipped
        assert_eq!(index.len(), 1);
        assert!(index.contains_key(&TileId::from_path("2/000/818/660.gph").unwrap()));
    }

    #[test]
    fn scan_index_empty_archive_fails() {
        // Archive with only non-tile entries
        let archive = make_tar_archive(&[("readme.txt", b"hello")]);
        let err = parse_index_from_tar_scan(&archive).unwrap_err();
        assert!(matches!(err, TarError::EmptyIndex));
    }

    #[test]
    fn scan_index_verifies_data_offsets() {
        // Two tiles: verify the second tile's offset accounts for header + data + padding of the first
        let data1 = vec![0xAA; 600]; // 600 bytes -> 2 data blocks (1024 bytes padded)
        let data2 = vec![0xBB; 100]; // 100 bytes -> 1 data block (512 bytes padded)
        let archive =
            make_tar_archive(&[("2/000/000/001.gph", &data1), ("2/000/000/002.gph", &data2)]);

        let index = parse_index_from_tar_scan(&archive).unwrap();
        assert_eq!(index.len(), 2);

        // First entry: header at 0, data at 512
        assert_eq!(
            index[&TileId::from_path("2/000/000/001").unwrap()],
            (512, 600)
        );
        // Second entry: after header(512) + 2 data blocks(1024) = 1536, then its own header(512) -> data at 2048
        assert_eq!(
            index[&TileId::from_path("2/000/000/002").unwrap()],
            (512 + 1024 + 512, 100)
        );
    }

    #[test]
    fn parse_dataset_id_valid() {
        // Build a fake 272-byte header with dataset_id = 123456789 at offset 32
        let mut header = vec![0u8; GRAPH_TILE_HEADER_SIZE];
        let id: u64 = 123_456_789;
        header[DATASET_ID_OFFSET..DATASET_ID_OFFSET + 8].copy_from_slice(&id.to_le_bytes());

        let result = parse_dataset_id(&header).unwrap();
        assert_eq!(result, 123_456_789);
    }

    #[test]
    fn parse_dataset_id_zero() {
        let header = vec![0u8; GRAPH_TILE_HEADER_SIZE];
        let result = parse_dataset_id(&header).unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn parse_dataset_id_large_value() {
        let mut header = vec![0u8; GRAPH_TILE_HEADER_SIZE];
        let id: u64 = 0xDEAD_BEEF_CAFE_BABE;
        header[DATASET_ID_OFFSET..DATASET_ID_OFFSET + 8].copy_from_slice(&id.to_le_bytes());

        let result = parse_dataset_id(&header).unwrap();
        assert_eq!(result, 0xDEAD_BEEF_CAFE_BABE);
    }

    #[test]
    fn parse_dataset_id_too_short() {
        let header = vec![0u8; 39]; // needs at least 40 bytes (32 + 8)
        let err = parse_dataset_id(&header).unwrap_err();
        assert!(err.to_string().contains("too short"));
    }

    #[test]
    fn parse_dataset_id_exact_minimum_size() {
        let mut header = vec![0u8; DATASET_ID_OFFSET + 8]; // exactly 40 bytes
        let id: u64 = 42;
        header[DATASET_ID_OFFSET..DATASET_ID_OFFSET + 8].copy_from_slice(&id.to_le_bytes());

        let result = parse_dataset_id(&header).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn parse_s3_url_test() {
        assert_eq!(
            parse_s3_url("s3://my-bucket/path/to/file.tar"),
            Some(("my-bucket", "path/to/file.tar"))
        );
        assert_eq!(
            parse_s3_url("s3://bucket/file.tar"),
            Some(("bucket", "file.tar"))
        );

        assert_eq!(parse_s3_url("bucket/key"), None);
        assert_eq!(parse_s3_url("https://wrong/scheme"), None);
        assert_eq!(parse_s3_url("s3:/bad-url/format"), None);
        assert_eq!(parse_s3_url("s3://bucket-only"), None);
        assert_eq!(parse_s3_url("s3://file-only.tar"), None);
    }
}
