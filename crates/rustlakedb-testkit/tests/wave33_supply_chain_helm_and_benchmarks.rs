// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

//! Wave 33: Supply-Chain Security (SPDX SBOM + GPLv3 Check), Node 24 GitHub Actions Hygiene,
//! OCI Helm Chart Publishing (`oci://ghcr.io/<owner>/charts`), and Reproducible Benchmark Harness.

use std::path::PathBuf;

#[test]
fn wave33_ci_release_sbom_oci_helm_and_benchmark_pack_valid() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();

    let ci = std::fs::read_to_string(root.join(".github/workflows/ci.yml")).unwrap();
    assert!(ci.contains("actions/checkout@v7"), "CI must use Node 24 actions/checkout@v7");
    assert!(ci.contains("SPDX-License-Identifier: GPL-3.0-or-later"));
    assert!(ci.contains("cargo metadata --locked"));

    let rel = std::fs::read_to_string(root.join(".github/workflows/release.yml")).unwrap();
    assert!(rel.contains("actions/checkout@v7"));
    assert!(rel.contains("actions/upload-artifact@v7"));
    assert!(rel.contains("actions/download-artifact@v8"));
    assert!(rel.contains("docker/login-action@v4"));
    assert!(rel.contains("docker/setup-buildx-action@v4"));
    assert!(rel.contains("docker/build-push-action@v7"));
    assert!(rel.contains("publish-helm-oci"), "Release must publish OCI Helm chart");
    assert!(rel.contains("oci://ghcr.io/"), "Helm push must target GHCR OCI");
    assert!(rel.contains("SPDX-2.3"), "Release must generate SPDX 2.3 SBOM");

    let chart = std::fs::read_to_string(root.join("deploy/helm/rustlakedb/Chart.yaml")).unwrap();
    assert!(chart.contains("version: 8.0.0"));

    let bench_ps1 = std::fs::read_to_string(root.join("scripts/run-tpch-bench.ps1")).unwrap();
    assert!(bench_ps1.contains("wave19_tpch_22_and_cbo"));
    assert!(bench_ps1.contains("seed-large"));

    let bench_md = std::fs::read_to_string(root.join("docs/BENCHMARKS.md")).unwrap();
    assert!(bench_md.contains("18.9×"));
    assert!(bench_md.contains("27.0M rows/s"));
}
