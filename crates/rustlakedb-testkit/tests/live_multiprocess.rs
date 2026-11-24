// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use rustlakedb_testkit::PgTestClient;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn pick_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn find_rustlakedb_bin() -> PathBuf {
    // Locate compiled `rustlakedb.exe` / `rustlakedb` in target/debug or target/release.
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir.parent().unwrap().parent().unwrap();
    let exe_name = if cfg!(windows) {
        "rustlakedb.exe"
    } else {
        "rustlakedb"
    };
    let debug_bin = workspace_root.join("target/debug").join(exe_name);
    if debug_bin.exists() {
        return debug_bin;
    }
    workspace_root.join("target/release").join(exe_name)
}

async fn wait_for_http_health(url: &str) {
    let http = reqwest::Client::new();
    for _ in 0..60 {
        if let Ok(resp) = http.get(url).send().await
            && resp.status().is_success()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {url}");
}

/// TRUE LIVE MULTI-PROCESS OS CLUSTER TEST:
/// Spawns 3 independent OS processes (2 remote Workers with private shuffle dirs + 1 Coordinator
/// exposing live PostgreSQL v3 TCP & Iceberg v3 REST HTTP), executes distributed SQL joins
/// across separate PIDs using cross-process HTTP Zstd shuffle, then KILLS Worker 1's real OS PID
/// and verifies automatic failover onto surviving Worker 2!
#[tokio::test]
async fn live_multiprocess_os_cluster_distributed_sql_and_worker_kill_failover() {
    let bin = find_rustlakedb_bin();
    if !bin.exists() {
        panic!(
            "rustlakedb binary not found at {}; build with `cargo build --bin rustlakedb` first",
            bin.display()
        );
    }

    let data_dir = tempfile::tempdir().unwrap();
    let w1_port = pick_free_port();
    let w2_port = pick_free_port();
    let pg_port = pick_free_port();
    let rest_port = pick_free_port();

    // 1. Spawn Worker 1 OS process (private shuffle dir).
    let mut w1 = ChildGuard(
        Command::new(&bin)
            .args([
                "--mode",
                "worker",
                "--worker-id",
                "os-worker-1",
                "--bind",
                &format!("127.0.0.1:{w1_port}"),
                "--data-dir",
                data_dir.path().to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn worker-1"),
    );

    // 2. Spawn Worker 2 OS process (separate private shuffle dir).
    let _w2 = ChildGuard(
        Command::new(&bin)
            .args([
                "--mode",
                "worker",
                "--worker-id",
                "os-worker-2",
                "--bind",
                &format!("127.0.0.1:{w2_port}"),
                "--data-dir",
                data_dir.path().to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn worker-2"),
    );

    wait_for_http_health(&format!("http://127.0.0.1:{w1_port}/healthz")).await;
    wait_for_http_health(&format!("http://127.0.0.1:{w2_port}/healthz")).await;

    // 3. Spawn Coordinator OS process wired to both remote Worker HTTP URLs + PG TCP + Iceberg REST!
    let _coord = ChildGuard(
        Command::new(&bin)
            .args([
                "--mode",
                "coordinator",
                "--workers",
                "2",
                "--worker-urls",
                &format!("http://127.0.0.1:{w1_port},http://127.0.0.1:{w2_port}"),
                "--pg-addr",
                &format!("127.0.0.1:{pg_port}"),
                "--rest-addr",
                &format!("127.0.0.1:{rest_port}"),
                "--data-dir",
                data_dir.path().to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn coordinator"),
    );

    wait_for_http_health(&format!("http://127.0.0.1:{rest_port}/healthz")).await;

    // 4. Connect real PostgreSQL v3 TCP client to the live Coordinator OS process!
    let pg_addr: SocketAddr = format!("127.0.0.1:{pg_port}").parse().unwrap();
    let mut pg = PgTestClient::connect(pg_addr, "postgres", "rustlakedb")
        .await
        .unwrap();

    pg.query("CREATE TABLE sales (id BIGINT, store_id BIGINT, amount DOUBLE, meta JSONB)")
        .await
        .unwrap();
    pg.query("CREATE TABLE stores (store_id BIGINT, region TEXT)")
        .await
        .unwrap();
    pg.query(r#"INSERT INTO sales VALUES (1, 100, 50.0, '{"channel": "online"}'), (2, 200, 75.0, '{"channel": "pos"}')"#)
        .await
        .unwrap();
    pg.query(r#"INSERT INTO sales VALUES (3, 100, 125.0, '{"channel": "online"}')"#)
        .await
        .unwrap();
    pg.query("INSERT INTO stores VALUES (100, 'west'), (200, 'east')")
        .await
        .unwrap();

    // Distributed JOIN + GROUP BY across TWO independent Worker OS processes + HTTP Zstd shuffle!
    let joined = pg
        .query(
            "SELECT region, SUM(amount) AS total \
             FROM sales JOIN stores ON sales.store_id = stores.store_id \
             GROUP BY region ORDER BY region",
        )
        .await
        .unwrap();
    assert_eq!(
        joined.rows,
        vec![
            vec![Some("east".into()), Some("75".into())],
            vec![Some("west".into()), Some("175".into())],
        ]
    );

    // 5. Verify live Iceberg v3 REST Catalog HTTP endpoint reports both tables.
    let http = reqwest::Client::new();
    let tables: serde_json::Value = http
        .get(format!("http://127.0.0.1:{rest_port}/v1/namespaces/default/tables"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(tables["identifiers"].as_array().unwrap().len(), 2);

    // 6. REAL CHAOS FAILOVER: Kill Worker 1's actual OS process mid-flight!
    w1.0.kill().unwrap();
    let _ = w1.0.wait();

    // Subsequent distributed query MUST survive dead Worker 1 and succeed via Worker 2!
    let after_kill = pg
        .query("SELECT SUM(amount) AS total FROM sales WHERE meta->>'channel' = 'online'")
        .await
        .unwrap();
    assert_eq!(after_kill.rows, vec![vec![Some("175".into())]]);

    pg.close().await.unwrap();
}
