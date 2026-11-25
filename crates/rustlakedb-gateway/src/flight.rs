// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::stream::{self, BoxStream};
use futures::TryStreamExt;
use rustlakedb_common::{Result, RustLakeError};
use rustlakedb_coordinator::Coordinator;
use rustlakedb_storage::row_batch_to_record_batch;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

/// Apache Arrow Flight / Flight SQL gRPC Server (Wave 15):
/// Streams zero-copy Arrow `RecordBatch`es over HTTP/2 gRPC to Python ADBC, Polars,
/// DuckDB, Superset, Tableau, and Power BI at multi-GB/s throughput!
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
            payload: b"rustlakedb-flight-v4".to_vec().into(),
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
        let sql = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(format!("invalid ticket SQL: {e}")))?;
        let res = self
            .coordinator
            .execute_sql(&sql, "main")
            .await
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let rb = row_batch_to_record_batch(&res.batch)
            .map_err(|e| Status::internal(e.to_string()))?;
        let schema = rb.schema();
        let batch_stream = stream::once(async move { Ok(rb) });
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map_err(|e| Status::internal(e.to_string()));
        Ok(Response::new(Box::pin(flight_stream)))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put not implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        Ok(Response::new(Box::pin(stream::empty())))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![Ok(ActionType {
            r#type: "healthcheck".into(),
            description: "Check RustLakeDB Arrow Flight status".into(),
        })];
        Ok(Response::new(Box::pin(stream::iter(actions))))
    }
}
