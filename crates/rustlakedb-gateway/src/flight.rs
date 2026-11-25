// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use arrow_array::builder::StringBuilder;
use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::{GetCatalogsBuilder, SqlInfoDataBuilder};
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, Any, Command, CommandGetCatalogs, CommandGetDbSchemas,
    CommandGetSqlInfo, CommandGetTables, CommandPreparedStatementQuery, CommandStatementIngest,
    CommandStatementQuery, CommandStatementUpdate, DoPutUpdateResult, ProstMessageExt, SqlInfo,
    TicketStatementQuery,
};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use futures::TryStreamExt;
use parking_lot::RwLock;
use prost::Message;
use rustlakedb_catalog::TableCommitProposal;
use rustlakedb_common::{Result, RustLakeError};
use rustlakedb_coordinator::Coordinator;
use rustlakedb_protocol::RowBatch;
use rustlakedb_storage::{record_batch_to_row_batch, row_batch_to_record_batch};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

/// Apache Arrow Flight & **Full Arrow Flight SQL** gRPC Server (Waves 15 & 18):
/// Supports BOTH:
/// 1. Standard **Apache Arrow Flight SQL** (`CommandStatementQuery`, `CommandStatementUpdate`,
///    `CommandStatementIngest` bulk `DoPut`, `CommandGetCatalogs`, `CommandGetDbSchemas`,
///    `CommandGetTables`, `CommandGetSqlInfo`, `ActionCreatePreparedStatement`,
///    `CommandPreparedStatementQuery`) for ADBC, JDBC Flight SQL, DuckDB, Tableau, Power BI!
/// 2. Direct **Apache Arrow Flight** (`FlightDescriptor::cmd` UTF-8 SQL + `path` table bulk `DoPut`)
///    for PyArrow and Polars clients!
pub struct FlightGateway {
    listener: TcpListener,
    coordinator: Arc<Coordinator>,
}

impl FlightGateway {
    pub async fn bind(addr: &str, coordinator: Arc<Coordinator>) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            coordinator,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    pub async fn serve(self) -> Result<()> {
        let svc = RustLakeFlightService {
            coordinator: self.coordinator,
            prepared: Arc::new(RwLock::new(HashMap::new())),
        };
        let incoming = TcpListenerStream::new(self.listener);
        Server::builder()
            .add_service(FlightServiceServer::new(svc))
            .serve_with_incoming(incoming)
            .await
            .map_err(|e| RustLakeError::Execution(format!("Arrow Flight gRPC server: {e}")))
    }
}

struct RustLakeFlightService {
    coordinator: Arc<Coordinator>,
    prepared: Arc<RwLock<HashMap<String, String>>>,
}

#[allow(clippy::result_large_err)]
impl RustLakeFlightService {
    fn encode_batch_stream(
        batch: RecordBatch,
    ) -> BoxStream<'static, std::result::Result<FlightData, Status>> {
        let schema = batch.schema();
        let batch_stream = stream::once(async move { Ok(batch) });
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map_err(|e| Status::internal(e.to_string()));
        Box::pin(flight_stream)
    }

    fn build_catalogs_batch(&self) -> std::result::Result<RecordBatch, Status> {
        let mut b = GetCatalogsBuilder::new();
        b.append("rustlakedb");
        b.build().map_err(|e| Status::internal(e.to_string()))
    }

    fn build_db_schemas_batch(
        &self,
        q: &CommandGetDbSchemas,
    ) -> std::result::Result<RecordBatch, Status> {
        let mut b = q.clone().into_builder();
        b.append("rustlakedb", "default");
        b.append("rustlakedb", "public");
        b.build().map_err(|e| Status::internal(e.to_string()))
    }

    async fn build_tables_batch(
        &self,
        q: &CommandGetTables,
    ) -> std::result::Result<RecordBatch, Status> {
        let mut b = q.clone().into_builder();
        let tables = self
            .coordinator
            .catalog
            .list_tables()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        for t in tables {
            let empty_schema = Schema::empty();
            b.append("rustlakedb", "default", &t, "TABLE", &empty_schema)
                .map_err(|e| Status::internal(e.to_string()))?;
        }
        b.build().map_err(|e| Status::internal(e.to_string()))
    }

    fn build_table_types_batch(&self) -> std::result::Result<RecordBatch, Status> {
        let mut sb = StringBuilder::new();
        sb.append_value("TABLE");
        sb.append_value("VIEW");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(sb.finish())])
            .map_err(|e| Status::internal(e.to_string()))
    }

    fn build_sql_info_batch(
        &self,
        q: &CommandGetSqlInfo,
    ) -> std::result::Result<RecordBatch, Status> {
        let mut b = SqlInfoDataBuilder::new();
        b.append(SqlInfo::FlightSqlServerName, "RustLakeDB");
        b.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
        b.append(SqlInfo::FlightSqlServerArrowVersion, "54");
        b.append(SqlInfo::FlightSqlServerReadOnly, false);
        let data = b.build().map_err(|e| Status::internal(e.to_string()))?;
        q.clone()
            .into_builder(&data)
            .build()
            .map_err(|e| Status::internal(e.to_string()))
    }

    async fn ingest_batches_to_table(
        &self,
        table: &str,
        batches: Vec<RecordBatch>,
    ) -> std::result::Result<i64, Status> {
        let meta = self
            .coordinator
            .catalog
            .get_table(table)
            .await
            .map_err(|e| Status::not_found(format!("table '{table}' not found: {e}")))?;
        let mut merged_rows = Vec::new();
        let cols: Vec<String> = meta.schema.iter().map(|f| f.name.clone()).collect();
        for rb in batches {
            let row_b = record_batch_to_row_batch(&rb);
            merged_rows.extend(row_b.rows);
        }
        let inserted = merged_rows.len() as i64;
        if inserted == 0 {
            return Ok(0);
        }
        let row_batch = RowBatch::new(cols, merged_rows);
        let snap_id = meta.refs.get("main").copied().unwrap_or(1);
        let path = format!("{table}/data/flight_ingest_{}.parquet", uuid::Uuid::new_v4());
        let df = self
            .coordinator
            .store
            .write_data_file(&path, &meta.schema, &row_batch)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let proposal = TableCommitProposal {
            table_name: table.to_string(),
            branch: "main".into(),
            expected_snapshot_id: snap_id,
            added_files: vec![df],
            removed_files: vec![],
            is_append_only: true,
        };
        self.coordinator
            .catalog
            .commit_table(proposal)
            .await
            .map_err(|e| Status::aborted(e.to_string()))?;
        Ok(inserted)
    }
}

fn try_decode_flight_sql(bytes: &[u8]) -> Option<Command> {
    let any = Any::decode(bytes).ok()?;
    if !any.type_url.contains("arrow.flight.protocol.sql") {
        return None;
    }
    Command::try_from(any).ok()
}

#[tonic::async_trait]
impl FlightService for RustLakeFlightService {
    type HandshakeStream = BoxStream<'static, std::result::Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, std::result::Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, std::result::Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, std::result::Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, std::result::Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, std::result::Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, std::result::Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        let resp = HandshakeResponse {
            protocol_version: 0,
            payload: b"rustlakedb-flightsql-v5".to_vec().into(),
        };
        Ok(Response::new(Box::pin(stream::once(async { Ok(resp) }))))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        let tables = self
            .coordinator
            .catalog
            .list_tables()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut infos = Vec::with_capacity(tables.len());
        for tbl in tables {
            let sql = format!("SELECT * FROM {tbl}");
            let rows = self
                .coordinator
                .catalog
                .get_table(&tbl)
                .await
                .map(|m| m.total_rows("main") as i64)
                .unwrap_or(-1);
            let info = FlightInfo::new()
                .with_descriptor(FlightDescriptor::new_path(vec![tbl]))
                .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(sql.into_bytes())))
                .with_total_records(rows);
            infos.push(Ok(info));
        }
        Ok(Response::new(Box::pin(stream::iter(infos))))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let desc = request.into_inner();

        // 1. Check for standard Arrow Flight SQL Protobuf Any commands (Wave 18)
        if let Some(cmd) = try_decode_flight_sql(&desc.cmd) {
            match cmd {
                Command::CommandStatementQuery(CommandStatementQuery { query, .. }) => {
                    let res = self
                        .coordinator
                        .execute_sql(&query, "main")
                        .await
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    let rb = row_batch_to_record_batch(&res.batch)
                        .map_err(|e| Status::internal(e.to_string()))?;
                    let ticket_msg = TicketStatementQuery {
                        statement_handle: Bytes::from(query.into_bytes()),
                    };
                    let ticket_bytes = ticket_msg.as_any().encode_to_vec();
                    let info = FlightInfo::new()
                        .try_with_schema(rb.schema().as_ref())
                        .map_err(|e| Status::internal(e.to_string()))?
                        .with_descriptor(desc)
                        .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(ticket_bytes)))
                        .with_total_records(rb.num_rows() as i64);
                    return Ok(Response::new(info));
                }
                Command::CommandPreparedStatementQuery(CommandPreparedStatementQuery {
                    prepared_statement_handle,
                }) => {
                    let handle_str =
                        String::from_utf8(prepared_statement_handle.to_vec()).map_err(|_| {
                            Status::invalid_argument("invalid prepared statement handle")
                        })?;
                    let query = self
                        .prepared
                        .read()
                        .get(&handle_str)
                        .cloned()
                        .ok_or_else(|| Status::not_found("prepared statement handle not found"))?;
                    let res = self
                        .coordinator
                        .execute_sql(&query, "main")
                        .await
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    let rb = row_batch_to_record_batch(&res.batch)
                        .map_err(|e| Status::internal(e.to_string()))?;
                    let info = FlightInfo::new()
                        .try_with_schema(rb.schema().as_ref())
                        .map_err(|e| Status::internal(e.to_string()))?
                        .with_descriptor(desc.clone())
                        .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(desc.cmd)))
                        .with_total_records(rb.num_rows() as i64);
                    return Ok(Response::new(info));
                }
                Command::CommandGetCatalogs(_) => {
                    let rb = self.build_catalogs_batch()?;
                    let info = FlightInfo::new()
                        .try_with_schema(rb.schema().as_ref())
                        .map_err(|e| Status::internal(e.to_string()))?
                        .with_descriptor(desc.clone())
                        .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(desc.cmd)))
                        .with_total_records(rb.num_rows() as i64);
                    return Ok(Response::new(info));
                }
                Command::CommandGetDbSchemas(q) => {
                    let rb = self.build_db_schemas_batch(&q)?;
                    let info = FlightInfo::new()
                        .try_with_schema(rb.schema().as_ref())
                        .map_err(|e| Status::internal(e.to_string()))?
                        .with_descriptor(desc.clone())
                        .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(desc.cmd)))
                        .with_total_records(rb.num_rows() as i64);
                    return Ok(Response::new(info));
                }
                Command::CommandGetTables(q) => {
                    let rb = self.build_tables_batch(&q).await?;
                    let info = FlightInfo::new()
                        .try_with_schema(rb.schema().as_ref())
                        .map_err(|e| Status::internal(e.to_string()))?
                        .with_descriptor(desc.clone())
                        .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(desc.cmd)))
                        .with_total_records(rb.num_rows() as i64);
                    return Ok(Response::new(info));
                }
                Command::CommandGetTableTypes(_) => {
                    let rb = self.build_table_types_batch()?;
                    let info = FlightInfo::new()
                        .try_with_schema(rb.schema().as_ref())
                        .map_err(|e| Status::internal(e.to_string()))?
                        .with_descriptor(desc.clone())
                        .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(desc.cmd)))
                        .with_total_records(rb.num_rows() as i64);
                    return Ok(Response::new(info));
                }
                Command::CommandGetSqlInfo(q) => {
                    let rb = self.build_sql_info_batch(&q)?;
                    let info = FlightInfo::new()
                        .try_with_schema(rb.schema().as_ref())
                        .map_err(|e| Status::internal(e.to_string()))?
                        .with_descriptor(desc.clone())
                        .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(desc.cmd)))
                        .with_total_records(rb.num_rows() as i64);
                    return Ok(Response::new(info));
                }
                _ => {}
            }
        }

        // 2. Direct Arrow Flight UTF-8 SQL / table path (PyArrow & Polars, Wave 15)
        let sql = if !desc.cmd.is_empty() {
            String::from_utf8(desc.cmd.to_vec())
                .map_err(|e| Status::invalid_argument(format!("invalid UTF-8 SQL cmd: {e}")))?
        } else if let Some(tbl) = desc.path.first() {
            format!("SELECT * FROM {tbl}")
        } else {
            return Err(Status::invalid_argument(
                "FlightDescriptor must contain SQL cmd or table path",
            ));
        };

        let res = self
            .coordinator
            .execute_sql(&sql, "main")
            .await
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let rb = row_batch_to_record_batch(&res.batch)
            .map_err(|e| Status::internal(e.to_string()))?;
        let info = FlightInfo::new()
            .try_with_schema(rb.schema().as_ref())
            .map_err(|e| Status::internal(e.to_string()))?
            .with_descriptor(desc)
            .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(sql.into_bytes())))
            .with_total_records(rb.num_rows() as i64);
        Ok(Response::new(info))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not implemented"))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        let info = self.get_flight_info(request).await?.into_inner();
        Ok(Response::new(SchemaResult {
            schema: info.schema,
        }))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        // 1. Flight SQL Protobuf Any tickets (Wave 18)
        if let Some(cmd) = try_decode_flight_sql(&ticket.ticket) {
            let rb = match cmd {
                Command::TicketStatementQuery(TicketStatementQuery { statement_handle }) => {
                    let sql = String::from_utf8(statement_handle.to_vec())
                        .map_err(|e| Status::invalid_argument(format!("invalid SQL handle: {e}")))?;
                    let res = self
                        .coordinator
                        .execute_sql(&sql, "main")
                        .await
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    row_batch_to_record_batch(&res.batch)
                        .map_err(|e| Status::internal(e.to_string()))?
                }
                Command::CommandPreparedStatementQuery(CommandPreparedStatementQuery {
                    prepared_statement_handle,
                }) => {
                    let handle_str =
                        String::from_utf8(prepared_statement_handle.to_vec()).map_err(|_| {
                            Status::invalid_argument("invalid prepared statement handle")
                        })?;
                    let sql = self
                        .prepared
                        .read()
                        .get(&handle_str)
                        .cloned()
                        .ok_or_else(|| Status::not_found("prepared statement handle not found"))?;
                    let res = self
                        .coordinator
                        .execute_sql(&sql, "main")
                        .await
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    row_batch_to_record_batch(&res.batch)
                        .map_err(|e| Status::internal(e.to_string()))?
                }
                Command::CommandGetCatalogs(CommandGetCatalogs {}) => self.build_catalogs_batch()?,
                Command::CommandGetDbSchemas(q) => self.build_db_schemas_batch(&q)?,
                Command::CommandGetTables(q) => self.build_tables_batch(&q).await?,
                Command::CommandGetTableTypes(_) => self.build_table_types_batch()?,
                Command::CommandGetSqlInfo(q) => self.build_sql_info_batch(&q)?,
                other => {
                    return Err(Status::unimplemented(format!(
                        "unsupported Flight SQL do_get command: {}",
                        other.type_url()
                    )));
                }
            };
            return Ok(Response::new(Self::encode_batch_stream(rb)));
        }

        // 2. Direct UTF-8 SQL ticket (Wave 15)
        let sql = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(format!("invalid ticket SQL: {e}")))?;
        let res = self
            .coordinator
            .execute_sql(&sql, "main")
            .await
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let rb = row_batch_to_record_batch(&res.batch)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(Self::encode_batch_stream(rb)))
    }

    /// Supports BOTH Flight SQL `CommandStatementUpdate` (DDL/DML), `CommandStatementIngest`
    /// (bulk Arrow streaming ingest), AND raw Flight `path=[table_name]` Arrow ingestion (Wave 18)!
    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        let mut stream = request.into_inner();
        let first = match stream.try_next().await? {
            Some(f) => f,
            None => return Err(Status::invalid_argument("empty DoPut stream")),
        };
        let desc = first
            .flight_descriptor
            .clone()
            .ok_or_else(|| Status::invalid_argument("DoPut requires FlightDescriptor"))?;

        // Check Flight SQL commands first
        if let Some(cmd) = try_decode_flight_sql(&desc.cmd) {
            match cmd {
                Command::CommandStatementUpdate(CommandStatementUpdate { query, .. }) => {
                    let res = self
                        .coordinator
                        .execute_sql(&query, "main")
                        .await
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    let affected = res
                        .command_tag
                        .split_whitespace()
                        .last()
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(res.batch.num_rows() as i64);
                    let put_res = PutResult {
                        app_metadata: DoPutUpdateResult {
                            record_count: affected,
                        }
                        .encode_to_vec()
                        .into(),
                    };
                    return Ok(Response::new(Box::pin(stream::once(async { Ok(put_res) }))));
                }
                Command::CommandStatementIngest(CommandStatementIngest { table, .. }) => {
                    let combined =
                        futures::StreamExt::chain(stream::once(async move { Ok(first) }), stream);
                    let rb_stream = FlightRecordBatchStream::new_from_flight_data(
                        combined.map_err(arrow_flight::error::FlightError::Tonic),
                    );
                    let batches: Vec<RecordBatch> = rb_stream
                        .try_collect()
                        .await
                        .map_err(|e| Status::invalid_argument(format!("decode Arrow stream: {e}")))?;
                    let inserted = self.ingest_batches_to_table(&table, batches).await?;
                    let put_res = PutResult {
                        app_metadata: DoPutUpdateResult {
                            record_count: inserted,
                        }
                        .encode_to_vec()
                        .into(),
                    };
                    return Ok(Response::new(Box::pin(stream::once(async { Ok(put_res) }))));
                }
                other => {
                    return Err(Status::unimplemented(format!(
                        "unsupported Flight SQL do_put command: {}",
                        other.type_url()
                    )));
                }
            }
        }

        // Raw Flight `DoPut` bulk Arrow ingestion using `FlightDescriptor::path = [table_name]`
        let table = desc
            .path
            .first()
            .cloned()
            .ok_or_else(|| Status::invalid_argument("DoPut path must specify target table name"))?;
        let combined = futures::StreamExt::chain(stream::once(async move { Ok(first) }), stream);
        let rb_stream = FlightRecordBatchStream::new_from_flight_data(
            combined.map_err(arrow_flight::error::FlightError::Tonic),
        );
        let batches: Vec<RecordBatch> = rb_stream
            .try_collect()
            .await
            .map_err(|e| Status::invalid_argument(format!("decode Arrow stream: {e}")))?;
        let inserted = self.ingest_batches_to_table(&table, batches).await?;
        let put_res = PutResult {
            app_metadata: DoPutUpdateResult {
                record_count: inserted,
            }
            .encode_to_vec()
            .into(),
        };
        Ok(Response::new(Box::pin(stream::once(async { Ok(put_res) }))))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not implemented"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        match action.r#type.as_str() {
            "CreatePreparedStatement" => {
                let any = Any::decode(&action.body[..])
                    .map_err(|e| Status::invalid_argument(format!("decode Any: {e}")))?;
                let req = any
                    .unpack::<ActionCreatePreparedStatementRequest>()
                    .map_err(|e| Status::invalid_argument(format!("unpack prepared req: {e}")))?
                    .ok_or_else(|| {
                        Status::invalid_argument("expected ActionCreatePreparedStatementRequest")
                    })?;
                let handle = format!("stmt-{}", uuid::Uuid::new_v4().simple());
                self.prepared
                    .write()
                    .insert(handle.clone(), req.query.clone());
                let res_msg = ActionCreatePreparedStatementResult {
                    prepared_statement_handle: Bytes::from(handle.into_bytes()),
                    dataset_schema: Bytes::new(),
                    parameter_schema: Bytes::new(),
                };
                let body = res_msg.as_any().encode_to_vec();
                let res = arrow_flight::Result { body: body.into() };
                Ok(Response::new(Box::pin(stream::once(async { Ok(res) }))))
            }
            "ClosePreparedStatement" => {
                if let Ok(any) = Any::decode(&action.body[..])
                    && let Ok(Some(req)) = any.unpack::<ActionClosePreparedStatementRequest>()
                    && let Ok(handle) = String::from_utf8(req.prepared_statement_handle.to_vec())
                {
                    self.prepared.write().remove(&handle);
                }
                Ok(Response::new(Box::pin(stream::empty())))
            }
            _ => Ok(Response::new(Box::pin(stream::empty()))),
        }
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![
            Ok(ActionType {
                r#type: "CreatePreparedStatement".into(),
                description: "Create a Flight SQL prepared statement".into(),
            }),
            Ok(ActionType {
                r#type: "ClosePreparedStatement".into(),
                description: "Close a Flight SQL prepared statement".into(),
            }),
            Ok(ActionType {
                r#type: "healthcheck".into(),
                description: "Check RustLakeDB Arrow Flight SQL status".into(),
            }),
        ];
        Ok(Response::new(Box::pin(stream::iter(actions))))
    }
}
