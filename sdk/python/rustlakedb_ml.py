# Copyright (C) 2026 Sudeshc
# SPDX-License-Identifier: GPL-3.0-or-later
"""
RustLakeDB Python ML & Zero-Copy Arrow Connector (Wave 42).

Streams Apache Iceberg v3 snapshots directly into PyArrow, Polars, Pandas, Ray Data,
and PyTorch IterableDatasets over zero-copy Apache Arrow IPC (`/v1/sql/arrow`) or
Apache Arrow Flight SQL (`:8815`), with Iceberg Time-Travel snapshot pinning and
Model Registry / Feature Store helpers.
"""

from __future__ import annotations

import io
import json
import urllib.request
from typing import Any, Dict, Iterator, List, Optional


class RustLakeMLClient:
    """Zero-copy Python ML/AI client for RustLakeDB."""

    def __init__(
        self,
        http_url: str = "http://127.0.0.1:8181",
        flight_url: str = "grpc://127.0.0.1:8815",
        bearer_token: Optional[str] = None,
        user: str = "postgres",
        branch: str = "main",
    ) -> None:
        self.http_url = http_url.rstrip("/")
        self.flight_url = flight_url
        self.bearer_token = bearer_token
        self.user = user
        self.branch = branch

    def _headers(self, content_type: str = "application/json") -> Dict[str, str]:
        h = {"Content-Type": content_type}
        if self.bearer_token:
            h["Authorization"] = f"Bearer {self.bearer_token}"
        return h

    def sql_json(self, sql: str) -> Dict[str, Any]:
        payload = json.dumps(
            {"sql": sql, "user": self.user, "branch": self.branch}
        ).encode("utf-8")
        req = urllib.request.Request(
            f"{self.http_url}/v1/sql",
            data=payload,
            headers=self._headers(),
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def sql_arrow_bytes(self, sql: str) -> bytes:
        """Fetches raw zero-copy Apache Arrow IPC stream bytes from `/v1/sql/arrow`."""
        payload = json.dumps(
            {"sql": sql, "user": self.user, "branch": self.branch}
        ).encode("utf-8")
        req = urllib.request.Request(
            f"{self.http_url}/v1/sql/arrow",
            data=payload,
            headers=self._headers(),
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:
            return resp.read()

    def to_pyarrow(self, sql: str):
        """Executes SQL and returns a zero-copy `pyarrow.Table`."""
        import pyarrow.ipc as pa_ipc

        raw = self.sql_arrow_bytes(sql)
        reader = pa_ipc.open_stream(io.BytesIO(raw))
        return reader.read_all()

    def to_polars(self, sql: str):
        """Executes SQL and returns a zero-copy `polars.DataFrame`."""
        import polars as pl

        return pl.from_arrow(self.to_pyarrow(sql))

    def to_pandas(self, sql: str):
        """Executes SQL and returns a `pandas.DataFrame` via PyArrow."""
        return self.to_pyarrow(sql).to_pandas()

    def to_ray_dataset(self, sql: str):
        """Loads an immutable Iceberg query directly into a Ray Dataset for distributed training."""
        import ray

        return ray.data.from_arrow(self.to_pyarrow(sql))

    def to_torch_dataset(
        self,
        table: str,
        feature_cols: List[str],
        label_col: Optional[str] = None,
        snapshot_id: Optional[int] = None,
        batch_size: int = 1024,
    ):
        """
        Returns a PyTorch `IterableDataset` pinned to an immutable Iceberg snapshot ID
        for 100% reproducible training runs.
        """
        import torch
        from torch.utils.data import IterableDataset

        cols = ", ".join(feature_cols + ([label_col] if label_col else []))
        ver = f" VERSION AS OF {snapshot_id}" if snapshot_id is not None else ""
        sql = f"SELECT {cols} FROM {table}{ver}"
        client = self

        class _RustLakeIterableDataset(IterableDataset):
            def __iter__(self) -> Iterator[Any]:
                pa_table = client.to_pyarrow(sql)
                for rb in pa_table.to_batches(max_chunksize=batch_size):
                    feats = [
                        torch.tensor(rb.column(c).to_pylist(), dtype=torch.float32)
                        for c in feature_cols
                    ]
                    x = torch.stack(feats, dim=1)
                    if label_col:
                        y = torch.tensor(
                            rb.column(label_col).to_pylist(), dtype=torch.float32
                        )
                        yield x, y
                    else:
                        yield x

        return _RustLakeIterableDataset()

    def asof_features(
        self,
        events_table: str,
        features_table: str,
        entity_col: str,
        event_ts_col: str,
        feature_ts_col: str,
        select_cols: str = "*",
    ):
        """Runs a leakage-free point-in-time `ASOF JOIN` and returns a PyArrow Table."""
        sql = (
            f"SELECT {select_cols} FROM {events_table} e "
            f"LEFT ASOF JOIN {features_table} f "
            f"ON e.{entity_col} = f.{entity_col} AND f.{feature_ts_col} <= e.{event_ts_col}"
        )
        return self.to_pyarrow(sql)

    def register_model(
        self,
        name: str,
        version: int,
        artifact_uri: str,
        framework: str = "pytorch",
        training_table: Optional[str] = None,
        metrics: Optional[Dict[str, float]] = None,
        stage: str = "staging",
    ) -> Dict[str, Any]:
        """Registers a model version in RustLakeDB's Raft-replicated Model Registry."""
        parts = [
            f"REGISTER MODEL {name} VERSION {version}",
            f"FRAMEWORK '{framework}'",
            f"ARTIFACT '{artifact_uri}'",
        ]
        if training_table:
            parts.append(f"FROM TABLE {training_table}")
        if metrics:
            m_str = ", ".join(f"'{k}' = {v}" for k, v in metrics.items())
            parts.append(f"METRICS ({m_str})")
        parts.append(f"STAGE '{stage}'")
        return self.sql_json(" ".join(parts))

    def mcp_call(self, method: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Invokes RustLakeDB's governed MCP (Model Context Protocol) JSON-RPC 2.0 endpoint."""
        payload = json.dumps(
            {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}
        ).encode("utf-8")
        req = urllib.request.Request(
            f"{self.http_url}/v1/mcp",
            data=payload,
            headers=self._headers(),
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode("utf-8"))
