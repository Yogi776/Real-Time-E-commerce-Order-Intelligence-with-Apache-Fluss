"""
Flink SQL Gateway REST API client.

Provides a clean interface to execute Flink SQL queries through the
SQL Gateway REST endpoint and return results as pandas DataFrames.

Performance optimizations:
  - Session reuse: initialized session persists across queries
  - Minimal polling interval for fast status checks
  - Pagination support for complete result retrieval
"""

import time
import logging
import requests
import pandas as pd
from typing import Optional

logger = logging.getLogger(__name__)


class FlinkGatewayClient:
    """Manages a persistent session with the Flink SQL Gateway."""

    def __init__(
        self,
        gateway_url: str = "http://localhost:8085",
        fluss_bootstrap: str = "coordinator-server:9123",
        poll_interval: float = 0.3,
        max_wait: float = 30.0,
    ):
        self.gateway_url = gateway_url.rstrip("/")
        self.fluss_bootstrap = fluss_bootstrap
        self.poll_interval = poll_interval
        self.max_wait = max_wait
        self._session_id: Optional[str] = None
        self._initialized = False

    @property
    def session_id(self) -> str:
        if not self._session_id:
            self._create_session()
        return self._session_id

    def _create_session(self):
        resp = requests.post(
            f"{self.gateway_url}/v1/sessions",
            json={"properties": {}},
            timeout=10,
        )
        resp.raise_for_status()
        self._session_id = resp.json()["sessionHandle"]
        self._initialized = False
        logger.info("Created SQL Gateway session: %s", self._session_id)

    def _ensure_initialized(self):
        if self._initialized:
            return
        setup_stmts = [
            f"CREATE CATALOG fluss_catalog WITH ('type' = 'fluss', "
            f"'bootstrap.servers' = '{self.fluss_bootstrap}')",
            "USE CATALOG fluss_catalog",
            "USE ecommerce",
            "SET 'execution.runtime-mode' = 'batch'",
        ]
        for stmt in setup_stmts:
            self._execute_and_wait(stmt)
        self._initialized = True

    def _submit_statement(self, statement: str) -> str:
        resp = requests.post(
            f"{self.gateway_url}/v1/sessions/{self.session_id}/statements",
            json={"statement": statement},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()["operationHandle"]

    def _poll_status(self, operation_id: str) -> str:
        resp = requests.get(
            f"{self.gateway_url}/v1/sessions/{self.session_id}/operations/{operation_id}/status",
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json().get("status", "UNKNOWN")

    def _fetch_result(self, operation_id: str) -> pd.DataFrame:
        """Fetch all result pages and combine into a single DataFrame."""
        all_rows = []
        columns = []
        token = 0
        max_pages = 50

        for _ in range(max_pages):
            resp = requests.get(
                f"{self.gateway_url}/v1/sessions/{self.session_id}"
                f"/operations/{operation_id}/result/{token}",
                timeout=15,
            )
            resp.raise_for_status()
            result = resp.json()

            results_data = result.get("results", {})
            if not columns:
                columns = [c["name"] for c in results_data.get("columns", [])]

            for row in results_data.get("data", []):
                all_rows.append(row.get("fields", []))

            if "nextResultUri" not in result:
                break
            token += 1

        if not all_rows:
            return pd.DataFrame(columns=columns)
        return pd.DataFrame(all_rows, columns=columns)

    def _execute_and_wait(self, statement: str) -> str:
        op_id = self._submit_statement(statement)
        elapsed = 0.0
        while elapsed < self.max_wait:
            status = self._poll_status(op_id)
            if status == "FINISHED":
                return op_id
            if status == "ERROR":
                raise RuntimeError(f"Statement failed: {statement}")
            time.sleep(self.poll_interval)
            elapsed += self.poll_interval
        raise TimeoutError(f"Statement timed out after {self.max_wait}s: {statement}")

    def query(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query and return results as a DataFrame."""
        self._ensure_initialized()
        try:
            op_id = self._execute_and_wait(sql)
        except (requests.RequestException, RuntimeError):
            self._session_id = None
            self._initialized = False
            self._create_session()
            self._ensure_initialized()
            op_id = self._execute_and_wait(sql)

        return self._fetch_result(op_id)

    def is_healthy(self) -> bool:
        try:
            resp = requests.get(f"{self.gateway_url}/v1/info", timeout=3)
            return resp.status_code == 200
        except requests.RequestException:
            return False
