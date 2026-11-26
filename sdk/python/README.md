# RustLakeDB Python ML & Zero-Copy Arrow SDK

Zero-copy Python connector for training PyTorch, Ray Data, Polars, and Pandas models directly on RustLakeDB Apache Iceberg v3 tables with Time-Travel snapshot pinning, point-in-time `ASOF JOIN` feature serving, Model Registry lineage, and governed MCP agent access.

## Quickstart

```python
from rustlakedb_ml import RustLakeMLClient

client = RustLakeMLClient(
    http_url="http://localhost:8181",
    bearer_token="<oidc_jwt>",  # inherits RBAC, RLS & Tag-ABAC PII masking
)

# 1. Leakage-free point-in-time Feature Store ASOF JOIN -> PyArrow / Polars
features = client.asof_features(
    events_table="tx_labels",
    features_table="user_risk_features",
    entity_col="user_id",
    event_ts_col="tx_ts",
    feature_ts_col="feature_ts",
)

# 2. Reproducible PyTorch DataLoader pinned to an immutable Iceberg snapshot ID
ds = client.to_torch_dataset(
    table="training_features",
    feature_cols=["amount", "velocity_1h", "risk_score"],
    label_col="is_fraud",
    snapshot_id=42,
    batch_size=2048,
)

# 3. Register trained model with automatic dataset@snapshot_id Raft lineage
client.register_model(
    name="fraud_detector",
    version=3,
    artifact_uri="s3://ml-artifacts/fraud_detector/v3.onnx",
    framework="pytorch",
    training_table="training_features",
    metrics={"auc": 0.972, "f1": 0.904},
    stage="staging",
)
```
