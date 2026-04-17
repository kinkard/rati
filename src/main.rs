use std::io::Write;
use std::num::NonZero;
use std::sync::Arc;

use axum::{
    Router,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::get,
};
use bytes::Bytes;
use clap::Parser;
use flate2::Compression;
use serde::Serialize;
use tokio::signal;
use tracing::info;

mod archive;

#[derive(Parser)]
struct Config {
    /// S3 location of the archive: s3://bucket/key.tar
    archive: String,
    /// Build index by scanning tar headers if index.bin is missing
    #[arg(long)]
    scan_index: bool,
    /// Override the dataset ID (auto-detected from graph tile headers if omitted)
    #[arg(long)]
    dataset_id: Option<String>,
    /// Cache-Control max-age in seconds (default: 86400 = 1 day)
    #[arg(long, default_value_t = 86400)]
    cache_max_age: u32,
    /// Port to listen
    #[arg(long, default_value_t = 3000)]
    port: u16,
    /// Max threads to use
    #[arg(long, default_value_t = NonZero::new(4).unwrap())]
    concurrency: NonZero<u16>,
}

#[derive(Clone)]
struct AppState {
    archive: Arc<archive::S3Archive>,
    /// Pre-built cache related headers shared across all tile responses.
    tile_headers: HeaderMap,
    /// Pre-built status response (nothing changes at runtime).
    status: StatusResponse,
}

fn main() {
    tracing_subscriber::fmt::init();

    let config = Config::parse();

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(
            std::thread::available_parallelism()
                .map(NonZero::get)
                .unwrap_or(8) // fallback if we can't detect CPU count
                .min(config.concurrency.get() as usize),
        )
        .enable_all()
        .build()
        .unwrap()
        .block_on(run(config))
}

async fn run(config: Config) {
    let (archive, meta) = archive::S3Archive::open(
        &config.archive,
        config.scan_index,
        config.dataset_id.as_deref(),
    )
    .await
    .expect("failed to load tar index from S3");
    info!(
        "Loaded {} with {} tiles (dataset_id={})",
        config.archive, meta.tile_count, meta.dataset_id,
    );

    let tile_headers = build_tile_headers(&meta, config.cache_max_age);
    let state = AppState {
        archive: Arc::new(archive),
        tile_headers,
        status: StatusResponse {
            dataset_id: meta.dataset_id,
            tile_count: meta.tile_count,
            etag: meta.etag,
        },
    };

    let app = Router::new()
        .route("/", get(get_status))
        .route("/tiles/{*path}", get(get_tile))
        .route("/tiles_by_id/{tile_id}", get(get_tile_by_id))
        .route("/health", get(|| async { "OK" }))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", config.port))
        .await
        .unwrap();
    info!("Listening at http://0.0.0.0:{}", config.port);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::select! {
                _ = signal::ctrl_c() => {
                    info!("Ctrl+C received, shutting down");
                }
                _ = async {
                    signal::unix::signal(signal::unix::SignalKind::terminate())
                        .expect("failed to install SIGTERM signal handler")
                        .recv()
                        .await
                } => {
                    info!("SIGTERM received, shutting down");
                }
            }
        })
        .await
        .unwrap();
}

#[derive(Clone, Serialize)]
struct StatusResponse {
    dataset_id: Box<str>,
    tile_count: usize,
    etag: Box<str>,
}

async fn get_status(State(state): State<AppState>) -> axum::Json<StatusResponse> {
    axum::Json(state.status.clone())
}

async fn get_tile(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(path): Path<String>,
) -> impl IntoResponse {
    // Mode 2: `.gz` extension — client explicitly requests gzip file
    if let Some(base_path) = path.strip_suffix(".gz") {
        let tile_id = match archive::TileId::from_path(base_path) {
            Some(id) => id,
            None => return Err(StatusCode::BAD_REQUEST),
        };
        return fetch_tile_gz_file(&state, tile_id).await;
    }

    let tile_id = match archive::TileId::from_path(&path) {
        Some(id) => id,
        None => return Err(StatusCode::BAD_REQUEST),
    };

    // Mode 1: `Accept-Encoding: gzip` — compress on the fly with Content-Encoding
    if accepts_gzip(&headers) {
        return fetch_tile_gzip_encoded(&state, tile_id).await;
    }

    fetch_tile(&state, tile_id).await
}

/// Supports `Accept-Encoding: gzip` (mode 1) but not `.gz` extension (mode 2),
/// because numeric IDs have no file extension to append `.gz` to.
async fn get_tile_by_id(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(tile_id): Path<u32>,
) -> impl IntoResponse {
    let tile_id = archive::TileId::new(tile_id);

    if accepts_gzip(&headers) {
        return fetch_tile_gzip_encoded(&state, tile_id).await;
    }

    fetch_tile(&state, tile_id).await
}

async fn get_tile_data(state: &AppState, tile_id: archive::TileId) -> Result<Bytes, StatusCode> {
    match state.archive.get_tile(tile_id).await {
        Ok(Some(data)) => Ok(data),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => {
            tracing::error!(tile_id = %tile_id, "S3 error: {e}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn fetch_tile(
    state: &AppState,
    tile_id: archive::TileId,
) -> Result<(HeaderMap, Bytes), StatusCode> {
    let data = get_tile_data(state, tile_id).await?;
    Ok((state.tile_headers.clone(), data))
}

/// Mode 1: `Accept-Encoding: gzip` — compress on the fly, set `Content-Encoding: gzip`.
async fn fetch_tile_gzip_encoded(
    state: &AppState,
    tile_id: archive::TileId,
) -> Result<(HeaderMap, Bytes), StatusCode> {
    let data = get_tile_data(state, tile_id).await?;
    let compressed = gzip_compress(&data);
    let mut headers = state.tile_headers.clone();
    headers.insert(
        axum::http::header::CONTENT_ENCODING,
        HeaderValue::from_static("gzip"),
    );
    Ok((headers, Bytes::from(compressed)))
}

/// Mode 2: `.gz` extension — return raw gzip bytes, no `Content-Encoding` header.
async fn fetch_tile_gz_file(
    state: &AppState,
    tile_id: archive::TileId,
) -> Result<(HeaderMap, Bytes), StatusCode> {
    let data = get_tile_data(state, tile_id).await?;
    let compressed = gzip_compress(&data);
    Ok((state.tile_headers.clone(), Bytes::from(compressed)))
}

/// Per RFC 7231 section 5.3.4, `gzip;q=0` means gzip is explicitly unacceptable.
fn accepts_gzip(headers: &HeaderMap) -> bool {
    headers
        .get(axum::http::header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(',').any(|part| {
                let part = part.trim();
                if !part.starts_with("gzip") {
                    return false;
                }
                let after_gzip = &part["gzip".len()..];
                if after_gzip.is_empty() {
                    return true;
                }
                if let Some(rest) = after_gzip.strip_prefix(";q=") {
                    rest.trim().parse::<f32>().unwrap_or(1.0) > 0.0
                } else {
                    true
                }
            })
        })
}

fn gzip_compress(data: &[u8]) -> Vec<u8> {
    const GZIP_LEVEL: Compression = Compression::new(6); // good balance between size and performance
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), GZIP_LEVEL);
    encoder
        .write_all(data)
        .expect("gzip write to Vec cannot fail");
    encoder.finish().expect("gzip finish on Vec cannot fail")
}

/// Derived once at startup so that handlers can simply clone instead of re-computing per request.
fn build_tile_headers(meta: &archive::ArchiveMeta, cache_max_age: u32) -> HeaderMap {
    let mut headers = HeaderMap::new();

    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );

    if let Ok(val) = HeaderValue::from_str(&meta.etag) {
        headers.insert(axum::http::header::ETAG, val);
    }

    if let Ok(val) = HeaderValue::from_str(&meta.last_modified) {
        headers.insert(axum::http::header::LAST_MODIFIED, val);
    }

    let cache_control = format!("public, max-age={cache_max_age}, immutable");
    if let Ok(val) = HeaderValue::from_str(&cache_control) {
        headers.insert(axum::http::header::CACHE_CONTROL, val);
    }

    headers.insert(
        axum::http::header::VARY,
        HeaderValue::from_static("Accept-Encoding"),
    );

    if let Ok(val) = HeaderValue::from_str(&meta.dataset_id) {
        headers.insert("X-Dataset-Id", val);
    }

    headers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tile_headers_include_all_required_fields() {
        let meta = archive::ArchiveMeta {
            etag: "\"abc123\"".into(),
            last_modified: "Thu, 17 Apr 2025 12:00:00 GMT".into(),
            dataset_id: "42".into(),
            tile_count: 100,
        };
        let headers = build_tile_headers(&meta, 3600);

        assert_eq!(
            headers.get(axum::http::header::CONTENT_TYPE).unwrap(),
            "application/octet-stream"
        );
        assert_eq!(headers.get(axum::http::header::ETAG).unwrap(), "\"abc123\"");
        assert_eq!(
            headers.get(axum::http::header::LAST_MODIFIED).unwrap(),
            "Thu, 17 Apr 2025 12:00:00 GMT"
        );
        assert_eq!(
            headers.get(axum::http::header::CACHE_CONTROL).unwrap(),
            "public, max-age=3600, immutable"
        );
        assert_eq!(
            headers.get(axum::http::header::VARY).unwrap(),
            "Accept-Encoding"
        );
        assert_eq!(headers.get("X-Dataset-Id").unwrap(), "42");
    }

    #[test]
    fn tile_headers_default_cache_max_age() {
        let meta = archive::ArchiveMeta {
            etag: "\"x\"".into(),
            last_modified: "Thu, 01 Jan 2025 00:00:00 GMT".into(),
            dataset_id: "test-dataset".into(),
            tile_count: 1,
        };
        let headers = build_tile_headers(&meta, 86400);

        assert_eq!(
            headers.get(axum::http::header::CACHE_CONTROL).unwrap(),
            "public, max-age=86400, immutable"
        );
    }

    #[test]
    fn tile_headers_zero_max_age() {
        let meta = archive::ArchiveMeta {
            etag: "\"x\"".into(),
            last_modified: "Thu, 01 Jan 2025 00:00:00 GMT".into(),
            dataset_id: "ds".into(),
            tile_count: 1,
        };
        let headers = build_tile_headers(&meta, 0);

        assert_eq!(
            headers.get(axum::http::header::CACHE_CONTROL).unwrap(),
            "public, max-age=0, immutable"
        );
    }

    #[test]
    fn accepts_gzip_test() {
        let with = |val: &'static str| {
            let mut h = HeaderMap::new();
            h.insert(
                axum::http::header::ACCEPT_ENCODING,
                HeaderValue::from_static(val),
            );
            h
        };

        // Accepts
        assert!(accepts_gzip(&with("gzip, deflate, br")));
        assert!(accepts_gzip(&with("gzip")));
        assert!(accepts_gzip(&with("gzip;q=1.0, deflate;q=0.5")));
        assert!(accepts_gzip(&with("gzip;q=0.5")));

        // Rejects
        assert!(!accepts_gzip(&with("deflate, br")));
        assert!(!accepts_gzip(&HeaderMap::new()));
        assert!(!accepts_gzip(&with("gzip;q=0, deflate")));
        assert!(!accepts_gzip(&with("gzip;q=0.0")));
    }

    #[test]
    fn gzip_round_trip() {
        use flate2::read::GzDecoder;
        use std::io::Read;

        let original = b"Hello, Valhalla tile data!";
        let compressed = gzip_compress(original);
        let mut decoder = GzDecoder::new(compressed.as_slice());
        let mut result = Vec::new();
        decoder.read_to_end(&mut result).unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn gzip_starts_with_magic() {
        let compressed = gzip_compress(b"test");
        assert_eq!(compressed[0], 0x1f);
        assert_eq!(compressed[1], 0x8b);
    }
}
