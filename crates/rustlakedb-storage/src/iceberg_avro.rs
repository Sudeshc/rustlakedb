// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_catalog::{DataFileEntry, IcebergSnapshot, IcebergTableMetadata};
use rustlakedb_common::{Result, RustLakeError};
use std::collections::BTreeMap;

// Pure-Rust Apache Avro 1.11 Object Container File (`Obj\x01`) encoder/verifier
// producing spec-compliant Apache Iceberg v3 Manifest (`*-m0.avro`) and
// Manifest-List (`snap-*.avro`) binary files for direct Spark / Sail / Trino / DuckDB interop.

const AVRO_MAGIC: &[u8; 4] = b"Obj\x01";

const ICEBERG_V3_MANIFEST_ENTRY_SCHEMA: &str = r#"{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    {"name": "status", "type": "int", "field-id": 0},
    {"name": "snapshot_id", "type": ["null", "long"], "default": null, "field-id": 1},
    {"name": "sequence_number", "type": ["null", "long"], "default": null, "field-id": 3},
    {"name": "file_sequence_number", "type": ["null", "long"], "default": null, "field-id": 4},
    {"name": "data_file", "field-id": 2, "type": {
      "type": "record",
      "name": "r2",
      "fields": [
        {"name": "content", "type": "int", "field-id": 134},
        {"name": "file_path", "type": "string", "field-id": 100},
        {"name": "file_format", "type": "string", "field-id": 101},
        {"name": "record_count", "type": "long", "field-id": 103},
        {"name": "file_size_in_bytes", "type": "long", "field-id": 104}
      ]
    }}
  ]
}"#;

const ICEBERG_V3_MANIFEST_LIST_SCHEMA: &str = r#"{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {"name": "manifest_path", "type": "string", "field-id": 500},
    {"name": "manifest_length", "type": "long", "field-id": 501},
    {"name": "partition_spec_id", "type": "int", "field-id": 502},
    {"name": "content", "type": "int", "field-id": 517},
    {"name": "sequence_number", "type": "long", "field-id": 515},
    {"name": "min_sequence_number", "type": "long", "field-id": 516},
    {"name": "added_snapshot_id", "type": "long", "field-id": 503},
    {"name": "added_files_count", "type": "int", "field-id": 504},
    {"name": "existing_files_count", "type": "int", "field-id": 505},
    {"name": "deleted_files_count", "type": "int", "field-id": 506},
    {"name": "added_rows_count", "type": "long", "field-id": 512},
    {"name": "existing_rows_count", "type": "long", "field-id": 513},
    {"name": "deleted_rows_count", "type": "long", "field-id": 514}
  ]
}"#;

/// Encodes an Iceberg v3 Avro Manifest file (`manifest_entry` records) for a snapshot.
pub fn encode_iceberg_v3_manifest(
    meta: &IcebergTableMetadata,
    snap: &IcebergSnapshot,
) -> Vec<u8> {
    let mut header_meta = BTreeMap::new();
    header_meta.insert(
        "avro.schema".to_string(),
        ICEBERG_V3_MANIFEST_ENTRY_SCHEMA.as_bytes().to_vec(),
    );
    header_meta.insert("avro.codec".to_string(), b"null".to_vec());
    header_meta.insert("format-version".to_string(), b"3".to_vec());
    header_meta.insert("content".to_string(), b"data".to_vec());
    header_meta.insert("table-name".to_string(), meta.table_name.as_bytes().to_vec());
    header_meta.insert(
        "snapshot-id".to_string(),
        snap.snapshot_id.to_string().into_bytes(),
    );

    let mut records_buf = Vec::new();
    for df in &snap.data_files {
        encode_manifest_entry_record(&mut records_buf, snap.snapshot_id as i64, df);
    }

    build_avro_ocf(&header_meta, snap.data_files.len() as i64, &records_buf)
}

/// Encodes an Iceberg v3 Avro Manifest List file (`manifest_file` records) pointing to the manifest.
pub fn encode_iceberg_v3_manifest_list(
    snap: &IcebergSnapshot,
    manifest_path: &str,
    manifest_length: i64,
) -> Vec<u8> {
    let mut header_meta = BTreeMap::new();
    header_meta.insert(
        "avro.schema".to_string(),
        ICEBERG_V3_MANIFEST_LIST_SCHEMA.as_bytes().to_vec(),
    );
    header_meta.insert("avro.codec".to_string(), b"null".to_vec());
    header_meta.insert("format-version".to_string(), b"3".to_vec());
    header_meta.insert(
        "snapshot-id".to_string(),
        snap.snapshot_id.to_string().into_bytes(),
    );
    if let Some(pid) = snap.parent_snapshot_id {
        header_meta.insert("parent-snapshot-id".to_string(), pid.to_string().into_bytes());
    }
    header_meta.insert(
        "sequence-number".to_string(),
        snap.snapshot_id.to_string().into_bytes(),
    );

    let total_files = snap.data_files.len() as i32;
    let total_rows: i64 = snap.data_files.iter().map(|f| f.row_count as i64).sum();

    let mut record = Vec::new();
    write_avro_string(&mut record, manifest_path);
    write_avro_long(&mut record, manifest_length);
    write_avro_int(&mut record, 0); // partition_spec_id
    write_avro_int(&mut record, 0); // content = 0 (DATA)
    write_avro_long(&mut record, snap.snapshot_id as i64); // sequence_number
    write_avro_long(&mut record, 1); // min_sequence_number
    write_avro_long(&mut record, snap.snapshot_id as i64); // added_snapshot_id
    write_avro_int(&mut record, total_files); // added_files_count
    write_avro_int(&mut record, 0); // existing_files_count
    write_avro_int(&mut record, 0); // deleted_files_count
    write_avro_long(&mut record, total_rows); // added_rows_count
    write_avro_long(&mut record, 0); // existing_rows_count
    write_avro_long(&mut record, 0); // deleted_rows_count

    build_avro_ocf(&header_meta, 1, &record)
}

fn encode_manifest_entry_record(buf: &mut Vec<u8>, snap_id: i64, df: &DataFileEntry) {
    write_avro_int(buf, 1); // status: 1 = ADDED
    // union ["null", "long"] -> index 1 + long
    write_avro_long(buf, 1);
    write_avro_long(buf, snap_id);
    // sequence_number
    write_avro_long(buf, 1);
    write_avro_long(buf, snap_id);
    // file_sequence_number
    write_avro_long(buf, 1);
    write_avro_long(buf, snap_id);
    // data_file struct
    write_avro_int(buf, 0); // content = 0 (DATA)
    write_avro_string(buf, &df.file_path);
    write_avro_string(buf, "PARQUET");
    write_avro_long(buf, df.row_count as i64);
    write_avro_long(buf, df.byte_size as i64);
}

fn build_avro_ocf(
    metadata: &BTreeMap<String, Vec<u8>>,
    record_count: i64,
    records_bytes: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(512 + records_bytes.len());
    // 1. Magic bytes: "Obj\x01"
    out.extend_from_slice(AVRO_MAGIC);

    // 2. Metadata map: Avro map block (count, k/v pairs, terminating 0)
    write_avro_long(&mut out, metadata.len() as i64);
    for (k, v) in metadata {
        write_avro_string(&mut out, k);
        write_avro_bytes(&mut out, v);
    }
    write_avro_long(&mut out, 0);

    // 3. Deterministic 16-byte sync marker derived from metadata + payload
    let h1 = xxhash_rust::xxh3::xxh3_64(&out);
    let h2 = xxhash_rust::xxh3::xxh3_64(records_bytes);
    let mut sync = [0u8; 16];
    sync[..8].copy_from_slice(&h1.to_le_bytes());
    sync[8..].copy_from_slice(&h2.to_le_bytes());
    out.extend_from_slice(&sync);

    // 4. Data block (if record_count > 0)
    if record_count > 0 {
        write_avro_long(&mut out, record_count);
        write_avro_long(&mut out, records_bytes.len() as i64);
        out.extend_from_slice(records_bytes);
        out.extend_from_slice(&sync);
    }

    out
}

#[derive(Debug, Clone)]
pub struct AvroOcfSummary {
    pub metadata: BTreeMap<String, String>,
    pub sync_marker: [u8; 16],
    pub block_record_count: i64,
    pub block_byte_len: i64,
}

/// Parses and validates an Avro Object Container File (`Obj\x01`) header, metadata map,
/// sync marker, and first data block for Iceberg v3 specification verification.
pub fn parse_and_verify_avro_ocf(bytes: &[u8]) -> Result<AvroOcfSummary> {
    if bytes.len() < 4 || &bytes[..4] != AVRO_MAGIC {
        return Err(RustLakeError::Execution(
            "invalid Avro OCF: missing 'Obj\\x01' magic header".into(),
        ));
    }
    let mut pos = 4usize;
    let mut metadata = BTreeMap::new();
    loop {
        let count = read_avro_long(bytes, &mut pos)?;
        if count == 0 {
            break;
        }
        let entries = if count < 0 {
            let _block_bytes = read_avro_long(bytes, &mut pos)?;
            -count
        } else {
            count
        };
        for _ in 0..entries {
            let k = read_avro_string(bytes, &mut pos)?;
            let v = read_avro_bytes(bytes, &mut pos)?;
            metadata.insert(k, String::from_utf8_lossy(&v).to_string());
        }
    }
    if pos + 16 > bytes.len() {
        return Err(RustLakeError::Execution(
            "invalid Avro OCF: truncated sync marker".into(),
        ));
    }
    let mut sync_marker = [0u8; 16];
    sync_marker.copy_from_slice(&bytes[pos..pos + 16]);
    pos += 16;

    let (block_record_count, block_byte_len) = if pos < bytes.len() {
        let rc = read_avro_long(bytes, &mut pos)?;
        let bl = read_avro_long(bytes, &mut pos)?;
        let end = pos + (bl as usize);
        if end + 16 > bytes.len() || bytes[end..end + 16] != sync_marker {
            return Err(RustLakeError::Execution(
                "invalid Avro OCF: trailing sync marker mismatch".into(),
            ));
        }
        (rc, bl)
    } else {
        (0, 0)
    };

    Ok(AvroOcfSummary {
        metadata,
        sync_marker,
        block_record_count,
        block_byte_len,
    })
}

fn write_avro_int(buf: &mut Vec<u8>, v: i32) {
    write_avro_long(buf, v as i64);
}

fn write_avro_long(buf: &mut Vec<u8>, v: i64) {
    let mut n = ((v << 1) ^ (v >> 63)) as u64;
    while (n & !0x7F) != 0 {
        buf.push(((n & 0x7F) as u8) | 0x80);
        n >>= 7;
    }
    buf.push((n & 0x7F) as u8);
}

fn write_avro_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    write_avro_long(buf, data.len() as i64);
    buf.extend_from_slice(data);
}

fn write_avro_string(buf: &mut Vec<u8>, s: &str) {
    write_avro_bytes(buf, s.as_bytes());
}

fn read_avro_long(buf: &[u8], pos: &mut usize) -> Result<i64> {
    let mut shift = 0u32;
    let mut n = 0u64;
    loop {
        if *pos >= buf.len() {
            return Err(RustLakeError::Execution("unexpected EOF reading Avro varint".into()));
        }
        let b = buf[*pos];
        *pos += 1;
        n |= ((b & 0x7F) as u64) << shift;
        if (b & 0x80) == 0 {
            break;
        }
        shift += 7;
        if shift > 63 {
            return Err(RustLakeError::Execution("Avro varint overflow".into()));
        }
    }
    Ok(((n >> 1) as i64) ^ (-((n & 1) as i64)))
}

fn read_avro_bytes(buf: &[u8], pos: &mut usize) -> Result<Vec<u8>> {
    let len = read_avro_long(buf, pos)?;
    if len < 0 || *pos + (len as usize) > buf.len() {
        return Err(RustLakeError::Execution("invalid Avro bytes length".into()));
    }
    let slice = buf[*pos..*pos + (len as usize)].to_vec();
    *pos += len as usize;
    Ok(slice)
}

fn read_avro_string(buf: &[u8], pos: &mut usize) -> Result<String> {
    let b = read_avro_bytes(buf, pos)?;
    Ok(String::from_utf8_lossy(&b).to_string())
}
