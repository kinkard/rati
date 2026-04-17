# Rati

Rati (Range-Accessed Tar Index) is a lightweight HTTP server that serves individual tiles from a Valhalla-ecosystem tar archive stored on S3. It supports any tile type that uses Valhalla's tile ID scheme: graph tiles (`.gph`), speed tiles (`.csv`, `.spd`), and others.

Named after the auger Odin used to bore through a mountain to reach the mead of poetry locked within.

Created with two use cases in mind:

- **Predictive caching for offline navigation** — a CDN-friendly endpoint that lets mobile apps prefetch individual routing tiles along a planned route while still online, enabling fully offline navigation later.
- **Zero-download Valhalla setup** — Valhalla natively loads tiles from HTTP via `mjolnir.tile_url`. Point Valhalla at a Rati instance backed by S3 and get a working router with near-zero startup time — no need to download an 80 GB planet tarball first.

## Usage

```
rati <archive> [OPTIONS]
```

**Arguments:**
- `<archive>` — S3 location of the tar archive: `s3://bucket/path/to/tiles.tar`

**Options:**
| Flag | Default | Description |
|------|---------|-------------|
| `--port <PORT>` | `3000` | Port to listen on |
| `--concurrency <N>` | `4` | Max worker threads |
| `--scan-index` | off | Build index by scanning tar headers if `index.bin` is missing |
| `--dataset-id <ID>` | auto | Override the dataset ID (auto-detected from `GraphTileHeader` if omitted) |
| `--cache-max-age <SECONDS>` | `86400` | `Cache-Control` max-age in seconds |
| `--gzip-level <0-9>` | `6` | Compression level for on-the-fly gzip |

### Example with Valhalla

```sh
# Start Rati pointing at an S3 tile archive
rati s3://my-bucket/valhalla/tiles.tar --port 8080

# Configure Valhalla to fetch tiles from Rati
# In valhalla.json:
#   "mjolnir": { "tile_url": "http://localhost:8080/tiles/{tilePath}" }
```

## Routes

```
GET /                              Status: dataset_id, tile_count, s3_source, s3_etag
GET /tiles/{tilePath}              Tile by path (Valhalla-compatible)
GET /tiles/{tilePath}.gz           Tile by path, gzip-compressed file
GET /tiles_by_id/{tile_id}         Tile by numeric packed ID
GET /health                        Health check
```

The `/tiles/{tilePath}` route is directly compatible with Valhalla's `mjolnir.tile_url` setting, e.g. `/tiles/2/000/818/660.gph`.

## Gzip Support

Two compression modes are supported:

1. **`Accept-Encoding` negotiation** — When the client sends `Accept-Encoding: gzip`, the response is compressed on the fly with `Content-Encoding: gzip` set. Standard HTTP transparent compression; clients decompress automatically.

2. **`.gz` extension** — Requesting a path like `/tiles/2/000/818/660.gph.gz` returns raw gzip bytes without `Content-Encoding`. The response body *is* a gzip file. This is the mode Valhalla uses when `mjolnir.tile_url_gz` is configured.

## CDN Headers

Every tile response includes headers suitable for CDN caching:

| Header | Value | Source |
|--------|-------|--------|
| `ETag` | S3 object ETag | `HeadObject` at startup |
| `Last-Modified` | S3 object Last-Modified | `HeadObject` at startup |
| `Cache-Control` | `public, max-age=<n>, immutable` | `--cache-max-age` flag |
| `X-Dataset-Id` | Dataset identifier | Auto-detected or `--dataset-id` |
| `Vary` | `Accept-Encoding` | Always present |
| `Content-Type` | `application/octet-stream` | Always present |

## Dataset ID

For graph tile archives (`.gph`), the dataset ID is automatically extracted from the `GraphTileHeader` of the first tile in the archive. This is typically the OSM changeset ID (`dataset_id_` field, a `u64` at byte offset 32 in the 272-byte header).

For non-graph archives (speed tiles, etc.) where auto-detection fails, use `--dataset-id` to provide an explicit value. If neither works, the S3 ETag is used as a fallback.

## Index Modes

By default, Rati expects the tar archive to contain `index.bin` as its first entry — a binary index that maps tile IDs to byte offsets. This is how Valhalla tilesets are typically packaged (see `valhalla_build_extract`).

For archives without `index.bin` (e.g., speed tile tarballs), pass `--scan-index` to build the index by scanning all tar headers at startup. This requires reading the full archive, so it is slower for large files.

## Build

```sh
cargo build --release
```

## License

All code in this project is dual-licensed under either:

- [MIT license](https://opensource.org/licenses/MIT) ([`LICENSE-MIT`](LICENSE-MIT))
- [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) ([`LICENSE-APACHE`](LICENSE-APACHE))

at your option.
