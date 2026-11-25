// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use crate::Coordinator;
use bytes::{Buf, BufMut, BytesMut};
use rustlakedb_common::{Result, RustLakeError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct KafkaOffsetCheckpoint {
    next_offset: i64,
}

#[derive(Debug, Clone, Default)]
pub struct KafkaPollReport {
    pub messages_fetched: usize,
    pub applied: usize,
    pub skipped_duplicates: usize,
    pub next_offset: i64,
}

/// Pure-Rust Apache Kafka / Redpanda Binary Protocol Streaming CDC Consumer (Waves 15 & 17):
/// Connects directly to modern Apache Kafka 3.x/4.x or Redpanda TCP brokers (zero C `librdkafka`
/// dependency!), supports **Metadata v1**, **ListOffsets v1**, **Fetch v4+ RecordBatch v2 (`magic=2`,
/// zigzag varints, Zstd & uncompressed)** as well as legacy **Fetch v0 MessageSet**, ingests
/// Debezium CDC JSON envelopes (`op: c|u|d`) atomically into Apache Iceberg v3 via `ingest_cdc_events`,
/// and checkpoints committed offsets durably on disk for **exactly-once** delivery!
pub struct KafkaCdcConsumer {
    coord: Arc<Coordinator>,
    broker_addr: String,
    topic: String,
    partition: i32,
    table_name: String,
    branch: String,
    checkpoint_file: PathBuf,
}

impl KafkaCdcConsumer {
    pub fn new(
        coord: Arc<Coordinator>,
        broker_addr: impl Into<String>,
        topic: impl Into<String>,
        partition: i32,
        table_name: impl Into<String>,
        checkpoint_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        std::fs::create_dir_all(checkpoint_dir.as_ref())?;
        let topic_s = topic.into();
        let table_s = table_name.into();
        let safe_topic = topic_s.replace(['/', '\\', ':', '.'], "_");
        let checkpoint_file = checkpoint_dir
            .as_ref()
            .join(format!("kafka_{table_s}_{safe_topic}_p{partition}.json"));
        Ok(Self {
            coord,
            broker_addr: broker_addr.into(),
            topic: topic_s,
            partition,
            table_name: table_s,
            branch: "main".into(),
            checkpoint_file,
        })
    }

    pub fn load_next_offset(&self) -> i64 {
        if self.checkpoint_file.exists()
            && let Ok(bytes) = std::fs::read(&self.checkpoint_file)
            && let Ok(cp) = serde_json::from_slice::<KafkaOffsetCheckpoint>(&bytes)
        {
            return cp.next_offset;
        }
        0
    }

    fn save_next_offset(&self, next_offset: i64) -> Result<()> {
        let cp = KafkaOffsetCheckpoint { next_offset };
        let tmp = self
            .checkpoint_file
            .with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
        std::fs::write(&tmp, serde_json::to_vec_pretty(&cp)?)?;
        std::fs::rename(&tmp, &self.checkpoint_file)?;
        Ok(())
    }

    /// Executes one Fetch RPC (preferring modern Fetch v4 RecordBatch v2, with automatic v0 fallback)
    /// against the Kafka/Redpanda TCP broker and ingests fetched CDC events atomically.
    pub async fn poll_once(&self) -> Result<KafkaPollReport> {
        let fetch_offset = self.load_next_offset();
        let records = match fetch_kafka_v4_records(
            &self.broker_addr,
            &self.topic,
            self.partition,
            fetch_offset,
            1024 * 1024,
        )
        .await
        {
            Ok(recs) => recs,
            Err(_) => {
                fetch_kafka_v0_records(
                    &self.broker_addr,
                    &self.topic,
                    self.partition,
                    fetch_offset,
                    1024 * 1024,
                )
                .await?
            }
        };

        if records.is_empty() {
            return Ok(KafkaPollReport {
                messages_fetched: 0,
                applied: 0,
                skipped_duplicates: 0,
                next_offset: fetch_offset,
            });
        }

        let mut events: Vec<Value> = Vec::with_capacity(records.len());
        let mut max_seen_offset = fetch_offset - 1;
        for (offset, payload) in &records {
            if *offset > max_seen_offset {
                max_seen_offset = *offset;
            }
            if let Ok(mut v) = serde_json::from_slice::<Value>(payload) {
                if let Some(obj) = v.as_object_mut() {
                    obj.entry("offset".to_string())
                        .or_insert_with(|| Value::from((*offset + 1).max(1) as u64));
                }
                events.push(v);
            }
        }

        let stream_id = format!("kafka:{}:p{}", self.topic, self.partition);
        let cdc_res = self
            .coord
            .ingest_cdc_events(&self.table_name, &self.branch, &stream_id, &events)
            .await?;

        let new_next = (max_seen_offset + 1).max(fetch_offset);
        self.save_next_offset(new_next)?;

        Ok(KafkaPollReport {
            messages_fetched: records.len(),
            applied: cdc_res["applied"].as_u64().unwrap_or(0) as usize,
            skipped_duplicates: cdc_res["skipped_duplicates"].as_u64().unwrap_or(0) as usize,
            next_offset: new_next,
        })
    }

    pub async fn run_loop(&self, poll_interval: Duration) -> Result<()> {
        loop {
            match self.poll_once().await {
                Ok(rep) if rep.messages_fetched > 0 => {
                    tracing::info!(
                        topic = %self.topic,
                        partition = self.partition,
                        table = %self.table_name,
                        fetched = rep.messages_fetched,
                        applied = rep.applied,
                        next_offset = rep.next_offset,
                        "Kafka CDC consumer committed Iceberg v3 snapshot"
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(error = %e, broker = %self.broker_addr, "Kafka CDC poll error");
                }
            }
            tokio::time::sleep(poll_interval).await;
        }
    }
}

/// Discovers partition IDs for a Kafka/Redpanda topic via **Metadata v1** (`api_key=3, version=1`, Wave 17).
pub async fn fetch_kafka_topic_partitions(broker_addr: &str, topic: &str) -> Result<Vec<i32>> {
    let mut stream = TcpStream::connect(broker_addr)
        .await
        .map_err(|e| RustLakeError::Execution(format!("kafka connect '{broker_addr}': {e}")))?;

    let client_id = b"rustlakedb-cdc";
    let mut body = BytesMut::with_capacity(64);
    body.put_i16(3); // Metadata
    body.put_i16(1); // v1
    body.put_i32(100); // correlation_id
    body.put_i16(client_id.len() as i16);
    body.put_slice(client_id);
    // topics array: len=1
    body.put_i32(1);
    body.put_i16(topic.len() as i16);
    body.put_slice(topic.as_bytes());

    let resp = send_kafka_frame(&mut stream, &body).await?;
    let mut cur = &resp[..];
    if cur.remaining() < 8 {
        return Ok(vec![]);
    }
    let _corr_id = cur.get_i32();
    // brokers array: node_id(i32), host(str), port(i32), rack(nullable str)
    let num_brokers = cur.get_i32().max(0);
    for _ in 0..num_brokers {
        if cur.remaining() < 6 {
            return Ok(vec![]);
        }
        let _node_id = cur.get_i32();
        skip_kafka_string(&mut cur);
        if cur.remaining() < 4 {
            return Ok(vec![]);
        }
        let _port = cur.get_i32();
        skip_kafka_string(&mut cur); // rack
    }
    if cur.remaining() < 8 {
        return Ok(vec![]);
    }
    let _controller_id = cur.get_i32();
    let num_topics = cur.get_i32().max(0);
    let mut partitions = Vec::new();
    for _ in 0..num_topics {
        if cur.remaining() < 4 {
            break;
        }
        let err = cur.get_i16();
        let tname = read_kafka_string(&mut cur).unwrap_or_default();
        if cur.remaining() < 5 {
            break;
        }
        let _is_internal = cur.get_u8();
        let num_parts = cur.get_i32().max(0);
        for _ in 0..num_parts {
            if cur.remaining() < 10 {
                break;
            }
            let p_err = cur.get_i16();
            let pid = cur.get_i32();
            let _leader = cur.get_i32();
            // replicas array + isr array
            skip_i32_array(&mut cur);
            skip_i32_array(&mut cur);
            if err == 0 && p_err == 0 && tname == topic {
                partitions.push(pid);
            }
        }
    }
    partitions.sort_unstable();
    Ok(partitions)
}

/// Queries earliest (`timestamp = -2`) or latest (`timestamp = -1`) offset via **ListOffsets v1**
/// (`api_key=2, version=1`, Wave 17).
pub async fn list_kafka_offset(
    broker_addr: &str,
    topic: &str,
    partition: i32,
    timestamp: i64,
) -> Result<i64> {
    let mut stream = TcpStream::connect(broker_addr)
        .await
        .map_err(|e| RustLakeError::Execution(format!("kafka connect '{broker_addr}': {e}")))?;

    let client_id = b"rustlakedb-cdc";
    let mut body = BytesMut::with_capacity(64);
    body.put_i16(2); // ListOffsets
    body.put_i16(1); // v1
    body.put_i32(101);
    body.put_i16(client_id.len() as i16);
    body.put_slice(client_id);
    body.put_i32(-1); // replica_id
    body.put_i32(1); // topics len
    body.put_i16(topic.len() as i16);
    body.put_slice(topic.as_bytes());
    body.put_i32(1); // partitions len
    body.put_i32(partition);
    body.put_i64(timestamp);

    let resp = send_kafka_frame(&mut stream, &body).await?;
    let mut cur = &resp[..];
    if cur.remaining() < 8 {
        return Ok(0);
    }
    let _corr = cur.get_i32();
    let num_topics = cur.get_i32().max(0);
    for _ in 0..num_topics {
        skip_kafka_string(&mut cur);
        if cur.remaining() < 4 {
            break;
        }
        let num_parts = cur.get_i32().max(0);
        for _ in 0..num_parts {
            if cur.remaining() < 22 {
                break;
            }
            let pid = cur.get_i32();
            let err = cur.get_i16();
            let _ts = cur.get_i64();
            let offset = cur.get_i64();
            if pid == partition && err == 0 {
                return Ok(offset);
            }
        }
    }
    Ok(0)
}

/// Encodes and sends a modern **Apache Kafka / Redpanda Fetch v4** request (`api_key=1, version=4`,
/// Wave 17) supporting **RecordBatch v2 (`magic=2`, zigzag varints, Zstd & uncompressed)** as well
/// as legacy MessageSet v0/v1!
pub async fn fetch_kafka_v4_records(
    broker_addr: &str,
    topic: &str,
    partition: i32,
    fetch_offset: i64,
    max_bytes: i32,
) -> Result<Vec<(i64, Vec<u8>)>> {
    let mut stream = TcpStream::connect(broker_addr)
        .await
        .map_err(|e| RustLakeError::Execution(format!("kafka connect '{broker_addr}': {e}")))?;

    let client_id = b"rustlakedb-cdc";
    let mut body = BytesMut::with_capacity(128);
    // RequestHeader v1: api_key(1)=Fetch, api_version(4), correlation_id(2), client_id
    body.put_i16(1);
    body.put_i16(4);
    body.put_i32(2);
    body.put_i16(client_id.len() as i16);
    body.put_slice(client_id);
    // FetchRequest v4 body: replica_id(-1), max_wait_ms(250), min_bytes(1), max_bytes, isolation_level(0)
    body.put_i32(-1);
    body.put_i32(250);
    body.put_i32(1);
    body.put_i32(max_bytes);
    body.put_i8(0); // READ_UNCOMMITTED
    // topics array: len=1
    body.put_i32(1);
    body.put_i16(topic.len() as i16);
    body.put_slice(topic.as_bytes());
    // partitions array: len=1 -> partition(i32), fetch_offset(i64), max_bytes(i32)
    body.put_i32(1);
    body.put_i32(partition);
    body.put_i64(fetch_offset);
    body.put_i32(max_bytes);

    let resp = send_kafka_frame(&mut stream, &body).await?;
    let mut cur = &resp[..];
    if cur.remaining() < 12 {
        return Err(RustLakeError::Execution("truncated Kafka Fetch v4 response".into()));
    }
    let corr_id = cur.get_i32();
    if corr_id != 2 {
        return Err(RustLakeError::Execution(format!(
            "unexpected Kafka Fetch v4 correlation_id {corr_id}"
        )));
    }
    let _throttle_time_ms = cur.get_i32();
    let num_topics = cur.get_i32();
    if !(0..=10_000).contains(&num_topics) {
        return Err(RustLakeError::Execution(format!(
            "invalid Kafka Fetch v4 num_topics={num_topics} (likely v0 broker)"
        )));
    }
    let mut out = Vec::new();
    for _ in 0..num_topics {
        let Some(_tname) = read_kafka_string(&mut cur) else {
            return Err(RustLakeError::Execution(
                "malformed topic string in Kafka Fetch v4 response".into(),
            ));
        };
        if cur.remaining() < 4 {
            return Err(RustLakeError::Execution(
                "truncated partition array in Kafka Fetch v4 response".into(),
            ));
        }
        let num_parts = cur.get_i32();
        if !(0..=10_000).contains(&num_parts) {
            return Err(RustLakeError::Execution(format!(
                "invalid Kafka Fetch v4 num_parts={num_parts}"
            )));
        }
        for _ in 0..num_parts {
            // partition(4) + error_code(2) + high_watermark(8) + last_stable_offset(8) + aborted_tx_len(4)
            if cur.remaining() < 26 {
                break;
            }
            let _pid = cur.get_i32();
            let err_code = cur.get_i16();
            let _hwm = cur.get_i64();
            let _lso = cur.get_i64();
            let aborted_len = cur.get_i32();
            if aborted_len > 0 {
                let skip = (aborted_len as usize) * 16; // producer_id(8) + first_offset(8)
                if cur.remaining() < skip {
                    break;
                }
                cur.advance(skip);
            }
            if cur.remaining() < 4 {
                break;
            }
            let rec_bytes_len = cur.get_i32().max(0) as usize;
            if err_code != 0 || cur.remaining() < rec_bytes_len {
                if cur.remaining() >= rec_bytes_len {
                    cur.advance(rec_bytes_len);
                }
                continue;
            }
            let rec_slice = &cur[..rec_bytes_len];
            cur.advance(rec_bytes_len);
            let mut decoded = decode_kafka_record_set(rec_slice)?;
            decoded.retain(|(off, _)| *off >= fetch_offset);
            out.extend(decoded);
        }
    }
    if cur.remaining() != 0 {
        return Err(RustLakeError::Execution(format!(
            "Kafka Fetch v4 response has {} unconsumed trailing bytes (possible v0 broker)",
            cur.remaining()
        )));
    }
    Ok(out)
}

/// Decodes a Kafka RecordSet buffer containing either **RecordBatch v2 (`magic=2`)** frames
/// (Kafka 2.x/3.x/4.x & Redpanda) or legacy **MessageSet v0/v1** frames!
pub fn decode_kafka_record_set(mut buf: &[u8]) -> Result<Vec<(i64, Vec<u8>)>> {
    let mut out = Vec::new();
    while buf.remaining() >= 12 {
        // Peek magic byte at offset 16 (RecordBatch v2) vs offset 16 in MessageSet (crc=4, magic=1)
        let base_offset = i64::from_be_bytes(buf[0..8].try_into().unwrap());
        let batch_or_msg_len = i32::from_be_bytes(buf[8..12].try_into().unwrap()).max(0) as usize;
        if buf.len() < 12 + batch_or_msg_len {
            break;
        }
        let magic = if buf.len() > 16 { buf[16] } else { 0 };
        if magic == 2 && batch_or_msg_len >= 49 {
            // Kafka RecordBatch v2 header (total 61 bytes: 12 prefix + 49 header fields)
            let batch_body = &buf[12..12 + batch_or_msg_len];
            buf.advance(12 + batch_or_msg_len);

            let mut hdr = batch_body;
            let _leader_epoch = hdr.get_i32();
            let _magic = hdr.get_i8();
            let _crc = hdr.get_u32();
            let attributes = hdr.get_i16();
            let _last_offset_delta = hdr.get_i32();
            let _base_ts = hdr.get_i64();
            let _max_ts = hdr.get_i64();
            let _producer_id = hdr.get_i64();
            let _producer_epoch = hdr.get_i16();
            let _base_seq = hdr.get_i32();
            let records_count = hdr.get_i32().max(0) as usize;

            let codec = attributes & 0x07;
            let decompressed: Vec<u8>;
            let mut rec_cur: &[u8] = match codec {
                0 => hdr,
                4 => {
                    decompressed = zstd::bulk::decompress(hdr, 64 * 1024 * 1024).map_err(|e| {
                        RustLakeError::Execution(format!("kafka zstd decompress: {e}"))
                    })?;
                    &decompressed
                }
                other => {
                    return Err(RustLakeError::Execution(format!(
                        "unsupported Kafka RecordBatch v2 compression codec {other} (supported: none=0, zstd=4)"
                    )));
                }
            };

            for _ in 0..records_count {
                let rec_len = match read_varint_zigzag(&mut rec_cur) {
                    Some(n) if n > 0 => n as usize,
                    _ => break,
                };
                if rec_cur.len() < rec_len {
                    break;
                }
                let mut r = &rec_cur[..rec_len];
                rec_cur.advance(rec_len);

                if r.is_empty() {
                    continue;
                }
                let _rec_attrs = r.get_i8();
                let _ts_delta = read_varint_zigzag(&mut r).unwrap_or(0);
                let offset_delta = read_varint_zigzag(&mut r).unwrap_or(0);
                let key_len = read_varint_zigzag(&mut r).unwrap_or(-1);
                if key_len > 0 && r.len() >= key_len as usize {
                    r.advance(key_len as usize);
                }
                let val_len = read_varint_zigzag(&mut r).unwrap_or(-1);
                if val_len >= 0 && r.len() >= val_len as usize {
                    let val = r[..val_len as usize].to_vec();
                    out.push((base_offset + offset_delta, val));
                }
            }
        } else {
            // Legacy MessageSet v0/v1
            buf.advance(12);
            let mut msg = &buf[..batch_or_msg_len];
            buf.advance(batch_or_msg_len);
            if msg.remaining() < 14 {
                continue;
            }
            let _crc = msg.get_i32();
            let msg_magic = msg.get_i8();
            let _attrs = msg.get_i8();
            if msg_magic == 1 && msg.remaining() >= 8 {
                let _ts = msg.get_i64();
            }
            if msg.remaining() < 4 {
                continue;
            }
            let klen = msg.get_i32();
            if klen > 0 && msg.remaining() >= klen as usize {
                msg.advance(klen as usize);
            }
            if msg.remaining() < 4 {
                continue;
            }
            let vlen = msg.get_i32();
            if vlen >= 0 && msg.remaining() >= vlen as usize {
                out.push((base_offset, msg[..vlen as usize].to_vec()));
            }
        }
    }
    Ok(out)
}

/// Encodes records into a valid **Apache Kafka RecordBatch v2 (`magic=2`)** binary payload
/// (optional Zstd compression `codec=4`), compatible with Kafka 3.x/4.x & Redpanda (Wave 17).
pub fn encode_kafka_record_batch_v2(
    base_offset: i64,
    values: &[&[u8]],
    zstd_compress: bool,
) -> Result<Vec<u8>> {
    let mut rec_bytes = BytesMut::new();
    for (idx, val) in values.iter().enumerate() {
        let mut r = BytesMut::new();
        r.put_i8(0); // attributes
        write_varint_zigzag(&mut r, 0); // timestamp_delta
        write_varint_zigzag(&mut r, idx as i64); // offset_delta
        write_varint_zigzag(&mut r, -1); // null key
        write_varint_zigzag(&mut r, val.len() as i64);
        r.put_slice(val);
        write_varint_zigzag(&mut r, 0); // headers count
        write_varint_zigzag(&mut rec_bytes, r.len() as i64);
        rec_bytes.put_slice(&r);
    }

    let (attributes, payload): (i16, Vec<u8>) = if zstd_compress {
        let compressed = zstd::bulk::compress(&rec_bytes, 3)
            .map_err(|e| RustLakeError::Execution(format!("kafka zstd compress: {e}")))?;
        (4, compressed)
    } else {
        (0, rec_bytes.to_vec())
    };

    let mut after_crc = BytesMut::with_capacity(40 + payload.len());
    after_crc.put_i16(attributes);
    after_crc.put_i32(values.len().saturating_sub(1) as i32); // last_offset_delta
    after_crc.put_i64(1_700_000_000_000); // base_timestamp
    after_crc.put_i64(1_700_000_000_000); // max_timestamp
    after_crc.put_i64(-1); // producer_id
    after_crc.put_i16(-1); // producer_epoch
    after_crc.put_i32(-1); // base_sequence
    after_crc.put_i32(values.len() as i32); // records_count
    after_crc.put_slice(&payload);

    let crc = crc32c(&after_crc);

    let mut batch_body = BytesMut::with_capacity(9 + after_crc.len());
    batch_body.put_i32(0); // partition_leader_epoch
    batch_body.put_i8(2); // magic = 2
    batch_body.put_u32(crc);
    batch_body.put_slice(&after_crc);

    let mut out = BytesMut::with_capacity(12 + batch_body.len());
    out.put_i64(base_offset);
    out.put_i32(batch_body.len() as i32);
    out.put_slice(&batch_body);
    Ok(out.to_vec())
}

/// Pure-Rust CRC-32C (Castagnoli) checksum matching Kafka RecordBatch v2 specification.
pub fn crc32c(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0x82F6_3B78;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

fn read_varint_zigzag(buf: &mut &[u8]) -> Option<i64> {
    let mut v: u64 = 0;
    let mut shift = 0u32;
    loop {
        if buf.is_empty() || shift >= 64 {
            return None;
        }
        let b = buf[0];
        buf.advance(1);
        v |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Some(((v >> 1) as i64) ^ -((v & 1) as i64))
}

fn write_varint_zigzag(out: &mut BytesMut, n: i64) {
    let mut u = ((n << 1) ^ (n >> 63)) as u64;
    while u >= 0x80 {
        out.put_u8(((u as u8) & 0x7F) | 0x80);
        u >>= 7;
    }
    out.put_u8(u as u8);
}

async fn send_kafka_frame(stream: &mut TcpStream, body: &[u8]) -> Result<Vec<u8>> {
    let mut frame = BytesMut::with_capacity(4 + body.len());
    frame.put_i32(body.len() as i32);
    frame.put_slice(body);
    stream.write_all(&frame).await?;

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let resp_len = i32::from_be_bytes(len_buf).max(0) as usize;
    let mut resp = vec![0u8; resp_len];
    stream.read_exact(&mut resp).await?;
    Ok(resp)
}

fn skip_kafka_string(cur: &mut &[u8]) {
    if cur.remaining() < 2 {
        return;
    }
    let len = cur.get_i16();
    if len > 0 && cur.remaining() >= len as usize {
        cur.advance(len as usize);
    }
}

fn read_kafka_string(cur: &mut &[u8]) -> Option<String> {
    if cur.remaining() < 2 {
        return None;
    }
    let len = cur.get_i16();
    if len < 0 || cur.remaining() < len as usize {
        return None;
    }
    let s = String::from_utf8_lossy(&cur[..len as usize]).to_string();
    cur.advance(len as usize);
    Some(s)
}

fn skip_i32_array(cur: &mut &[u8]) {
    if cur.remaining() < 4 {
        return;
    }
    let n = cur.get_i32().max(0) as usize;
    let bytes = n * 4;
    if cur.remaining() >= bytes {
        cur.advance(bytes);
    }
}

/// Encodes and sends a legacy Apache Kafka Binary Protocol **Fetch v0** request (API Key = 1, Version = 0)
/// and decodes the returned `(offset, value_bytes)` records from the Kafka MessageSet.
pub async fn fetch_kafka_v0_records(
    broker_addr: &str,
    topic: &str,
    partition: i32,
    fetch_offset: i64,
    max_bytes: i32,
) -> Result<Vec<(i64, Vec<u8>)>> {
    let mut stream = TcpStream::connect(broker_addr)
        .await
        .map_err(|e| RustLakeError::Execution(format!("kafka connect '{broker_addr}': {e}")))?;

    let client_id = b"rustlakedb-cdc";
    let mut body = BytesMut::with_capacity(128);
    body.put_i16(1);
    body.put_i16(0);
    body.put_i32(1);
    body.put_i16(client_id.len() as i16);
    body.put_slice(client_id);
    body.put_i32(-1);
    body.put_i32(250);
    body.put_i32(1);
    body.put_i32(1);
    body.put_i16(topic.len() as i16);
    body.put_slice(topic.as_bytes());
    body.put_i32(1);
    body.put_i32(partition);
    body.put_i64(fetch_offset);
    body.put_i32(max_bytes);

    let resp = send_kafka_frame(&mut stream, &body).await?;
    let mut cur = &resp[..];
    if cur.remaining() < 8 {
        return Ok(vec![]);
    }
    let _corr_id = cur.get_i32();
    let num_topics = cur.get_i32();
    let mut out = Vec::new();
    for _ in 0..num_topics {
        skip_kafka_string(&mut cur);
        if cur.remaining() < 4 {
            break;
        }
        let num_parts = cur.get_i32();
        for _ in 0..num_parts {
            if cur.remaining() < 18 {
                break;
            }
            let _pid = cur.get_i32();
            let err_code = cur.get_i16();
            let _high_watermark = cur.get_i64();
            let msg_set_size = cur.get_i32().max(0) as usize;
            if err_code != 0 || cur.remaining() < msg_set_size {
                if cur.remaining() >= msg_set_size {
                    cur.advance(msg_set_size);
                }
                continue;
            }
            let mset = &cur[..msg_set_size];
            cur.advance(msg_set_size);
            out.extend(decode_kafka_record_set(mset)?);
        }
    }

    Ok(out)
}
