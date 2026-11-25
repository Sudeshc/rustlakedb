// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::CommandStatementIngest;
use arrow_schema::{DataType, Field, Schema};
use futures::{stream, TryStreamExt};
use rustlakedb_catalog::{Catalog, InMemoryRaftCatalog};
use rustlakedb_coordinator::Coordinator;
use rustlakedb_gateway::flight::FlightGateway;
use rustlakedb_planner::PlannerConfig;
use rustlakedb_shuffle::{HybridShuffleManager, ShuffleTransport};
use rustlakedb_storage::IcebergObjectStore;
use rustlakedb_worker::WorkerNode;
use std::sync::Arc;
use tempfile::TempDir;
use tonic::transport::Channel;

#[tokio::test]
async fn wave18_arrow_flight_sql_full_protocol_and_doput_bulk_ingest() {
    let dir = TempDir::new().unwrap();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryRaftCatalog::open(dir.path()).unwrap());
    let store = Arc::new(IcebergObjectStore::local_fs(dir.path()).unwrap());
    let shuffle: Arc<dyn ShuffleTransport> =
        Arc::new(HybridShuffleManager::new(dir.path().join("_shuffle")).unwrap());
    let worker = Arc::new(WorkerNode::new(
        "w1",
        catalog.clone(),
        shuffle.clone(),
        store.clone(),
    ));
    let coord = Arc::new(Coordinator::new(
        catalog,
        store,
        shuffle,
        vec![worker],
        PlannerConfig::default(),
    ));

    let gateway = FlightGateway::bind("127.0.0.1:0", coord.clone())
        .await
        .unwrap();
    let addr = gateway.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve().await;
    });

    let channel = Channel::from_shared(format!("http://127.0.0.1:{}", addr.port()))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightSqlServiceClient::new(channel);

    // 1. Flight SQL Handshake & Metadata Discovery (GetCatalogs, GetDbSchemas, GetSqlInfo)
    let _token = client.handshake("postgres", "").await.unwrap();

    let cat_info = client.get_catalogs().await.unwrap();
    let cat_stream = client
        .do_get(cat_info.endpoint[0].ticket.clone().unwrap())
        .await
        .unwrap();
    let cat_batches: Vec<RecordBatch> = cat_stream.try_collect().await.unwrap();
    let cats = cat_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(cats.value(0), "rustlakedb");

    let sch_info = client
        .get_db_schemas(arrow_flight::sql::CommandGetDbSchemas {
            catalog: Some("rustlakedb".into()),
            db_schema_filter_pattern: None,
        })
        .await
        .unwrap();
    let sch_batches: Vec<RecordBatch> = client
        .do_get(sch_info.endpoint[0].ticket.clone().unwrap())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(sch_batches[0].num_rows() >= 2);

    // 2. Flight SQL DDL via CommandStatementUpdate (DoPut)
    let _ = client
        .execute_update(
            "CREATE TABLE flight_sales (id BIGINT, region TEXT, amount DOUBLE)".into(),
            None,
        )
        .await
        .unwrap();

    // 3. Flight SQL Bulk Zero-Copy Arrow Ingestion (CommandStatementIngest via DoPut!)
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec![
                "us-east", "us-east", "eu-west", "eu-west",
            ])),
            Arc::new(Float64Array::from(vec![100.0, 250.0, 400.0, 50.0])),
        ],
    )
    .unwrap();

    let ingest_cmd = CommandStatementIngest {
        table_definition_options: None,
        table: "flight_sales".into(),
        schema: None,
        catalog: None,
        temporary: false,
        transaction_id: None,
        options: Default::default(),
    };
    let ingested = client
        .execute_ingest(ingest_cmd, stream::once(async move { Ok(batch) }))
        .await
        .unwrap();
    assert_eq!(ingested, 4);

    // 4. Flight SQL GetTables metadata discovery sees the newly created table!
    let tbl_info = client
        .get_tables(arrow_flight::sql::CommandGetTables {
            catalog: None,
            db_schema_filter_pattern: None,
            table_name_filter_pattern: None,
            table_types: vec![],
            include_schema: false,
        })
        .await
        .unwrap();
    let tbl_batches: Vec<RecordBatch> = client
        .do_get(tbl_info.endpoint[0].ticket.clone().unwrap())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let tbl_names = tbl_batches[0]
        .column_by_name("table_name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(
        (0..tbl_names.len()).any(|i| tbl_names.value(i) == "flight_sales"),
        "GetTables must include flight_sales"
    );

    // 5. Flight SQL CommandStatementQuery + DoGet
    let q_info = client
        .execute(
            "SELECT region, SUM(amount) AS total FROM flight_sales GROUP BY region ORDER BY region ASC"
                .into(),
            None,
        )
        .await
        .unwrap();
    let q_stream: FlightRecordBatchStream = client
        .do_get(q_info.endpoint[0].ticket.clone().unwrap())
        .await
        .unwrap();
    let q_batches: Vec<RecordBatch> = q_stream.try_collect().await.unwrap();
    assert_eq!(q_batches[0].num_rows(), 2);
    let regions = q_batches[0]
        .column_by_name("region")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let totals = q_batches[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(regions.value(0), "eu-west");
    assert_eq!(totals.value(0), 450.0);
    assert_eq!(regions.value(1), "us-east");
    assert_eq!(totals.value(1), 350.0);

    // 6. Flight SQL Prepared Statement (CreatePreparedStatement -> Execute -> DoGet -> Close)
    let mut prep = client
        .prepare(
            "SELECT id, amount FROM flight_sales WHERE region = 'us-east' ORDER BY id ASC".into(),
            None,
        )
        .await
        .unwrap();
    let p_info = prep.execute().await.unwrap();
    let p_batches: Vec<RecordBatch> = client
        .do_get(p_info.endpoint[0].ticket.clone().unwrap())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(p_batches[0].num_rows(), 2);
    let ids = p_batches[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
    prep.close().await.unwrap();
}
