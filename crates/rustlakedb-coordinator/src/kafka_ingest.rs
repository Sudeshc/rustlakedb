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

/// Pure-Rust Apache Kafka / Redpanda Binary Protocol Streaming CDC Consumer (Wave 15):
/// Connects directly to a Kafka/Redpanda TCP broker using the Kafka Fetch v0 binary wire protocol
/// (zero C `librdkafka` dependency!), decodes MessageSet records containing Debezium CDC JSON
/// envelopes (`op: c|u|d`), ingests them atomically into Apache Iceberg v3 via `ingest_cdc_events`,
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

    /// Executes one Fetch v0 RPC against the Kafka/Redpanda TCP broker and ingests fetched CDC events.
    pub async fn poll_once(&self) -> Result<KafkaPollReport> {
        let fetch_offset = self.load_next_offset();
        let records = fetch_kafka_v0_records(
            &self.broker_addr,
            &self.topic,
            self.partition,
            fetch_offset,
            1024 * 1024,
        )
        .await?;

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
                    // Stamp 1-based monotonic Kafka offset for exact deduplication in `ingest_cdc_events`.
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

/// Encodes and sends a real Apache Kafka Binary Protocol **Fetch v0** request (API Key = 1, Version = 0)
/// and decodes the returned `(offset, value_bytes)` records from the Kafka MessageSet!
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
    // RequestHeader v0: api_key(i16)=1 (Fetch), api_version(i16)=0, correlation_id(i32)=1, client_id(string)
    body.put_i16(1);
    body.put_i16(0);
    body.put_i32(1);
    body.put_i16(client_id.len() as i16);
    body.put_slice(client_id);
    // FetchRequest v0: replica_id(i32)=-1, max_wait_ms(i32)=250, min_bytes(i32)=1
    body.put_i32(-1);
    body.put_i32(250);
    body.put_i32(1);
    // topics array: len(i32)=1
    body.put_i32(1);
    body.put_i16(topic.len() as i16);
    body.put_slice(topic.as_bytes());
    // partitions array: len(i32)=1 -> partition(i32), fetch_offset(i64), max_bytes(i32)
    body.put_i32(1);
    body.put_i32(partition);
    body.put_i64(fetch_offset);
    body.put_i32(max_bytes);

    let mut frame = BytesMut::with_capacity(4 + body.len());
    frame.put_i32(body.len() as i32);
    frame.put_slice(&body);
    stream.write_all(&frame).await?;

    // Read length-prefixed FetchResponse v0
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let resp_len = i32::from_be_bytes(len_buf) as usize;
    let mut resp = vec![0u8; resp_len];
    stream.read_exact(&mut resp).await?;

    let mut cur = &resp[..];
    if cur.remaining() < 8 {
        return Ok(vec![]);
    }
    let _corr_id = cur.get_i32();
    let num_topics = cur.get_i32();
    let mut out = Vec::new();
    for _ in 0..num_topics {
        if cur.remaining() < 2 {
            break;
        }
        let tlen = cur.get_i16() as usize;
        if cur.remaining() < tlen + 4 {
            break;
        }
        cur.advance(tlen);
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
            let mut mset = &cur[..msg_set_size];
            cur.advance(msg_set_size);

            // Decode Kafka v0 MessageSet entries: offset(i64) + msg_size(i32) + crc(i32) + magic(i8) + attr(i8) + key(bytes) + val(bytes)
            while mset.remaining() >= 12 {
                let offset = mset.get_i64();
                let msize = mset.get_i32().max(0) as usize;
                if mset.remaining() < msize || msize < 14 {
                    break;
                }
                let mut msg = &mset[..msize];
                mset.advance(msize);
                let _crc = msg.get_i32();
                let _magic = msg.get_i8();
                let _attrs = msg.get_i8();
                let klen = msg.get_i32();
                if klen > 0 && msg.remaining() >= klen as usize {
                    msg.advance(klen as usize);
                }
                if msg.remaining() < 4 {
                    continue;
                }
                let vlen = msg.get_i32();
                if vlen > 0 && msg.remaining() >= vlen as usize {
                    let val = msg[..vlen as usize].to_vec();
                    out.push((offset, val));
                }
            }
        }
    }

    Ok(out)
}
