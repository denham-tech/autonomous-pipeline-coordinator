"""
Pipeline Coordinator
Runs: Scrape → Validate → Delta → Alert
Ensures atomic execution, rigid failure stops, and audit telemetry.
"""

import argparse
import csv
import logging
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("PipelineCoordinator")

# Base directory anchor for deterministic sibling path resolution
BASE_DIR = Path(__file__).resolve().parent.parent


class PipelineCoordinator:
    def __init__(self, db_path: str = "orchestration_audit.db"):
        self.db_path = Path(db_path).resolve()
        self.conn = sqlite3.connect(self.db_path)
        self._init_audit_schema()

    def _init_audit_schema(self):
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pipeline_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    extracted_records INTEGER DEFAULT 0,
                    anomalies_detected INTEGER DEFAULT 0,
                    execution_time_sec REAL NOT NULL,
                    executed_at TEXT NOT NULL,
                    notes TEXT
                )
            """)

    def _run_command(self, command: list, step_name: str) -> tuple[bool, str]:
        """Run an external process with timeout and strict error capture."""
        logger.info(f"Executing step: {step_name}")
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=300,
                check=False
            )
            if result.returncode == 0:
                logger.info(f"✓ {step_name} completed successfully")
                return True, result.stdout
            
            logger.error(f"✗ {step_name} exited with code {result.returncode}\nStderr: {result.stderr.strip()}")
            return False, result.stderr.strip()
        except subprocess.TimeoutExpired:
            msg = f"{step_name} timed out after 300 seconds"
            logger.error(f"✗ {msg}")
            return False, msg
        except Exception as e:
            logger.error(f"✗ {step_name} runtime exception: {e}")
            return False, str(e)

    def _count_csv_records(self, csv_path: str | Path) -> int:
        """Fast, dependency-free row counter (excluding header)."""
        p = Path(csv_path)
        if not p.exists() or p.stat().st_size == 0:
            return 0
        try:
            with open(p, mode="r", encoding="utf-8", errors="ignore") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if not header:
                    return 0
                return sum(1 for _ in reader)
        except Exception as e:
            logger.warning(f"Could not count records in {csv_path}: {e}")
            return 0

    def execute_lifecycle(
        self,
        baseline_csv: str,
        current_csv: str,
        delta_output: str = "delta_results.csv"
    ) -> bool:
        start_time = time.time()
        run_timestamp = datetime.now(timezone.utc).isoformat()
        notes = []
        anomalies = 0
        status = "FAILED"
        extracted_records = self._count_csv_records(current_csv)

        logger.info("=== Starting Orchestration Lifecycle ===")

        # Resolve sibling module paths dynamically
        validator_script = BASE_DIR / "catalog-validation-sentinel" / "schema_validator.py"
        delta_script = BASE_DIR / "ecommerce-delta-engine" / "delta_engine.py"
        alert_script = BASE_DIR / "ecom-telemetry-alerts" / "alert_dispatcher.py"

        try:
            # Step 1: Validate Schema & Types
            val_success, val_output = self._run_command(
                [
                    sys.executable, str(validator_script),
                    "--input", current_csv,
                    "--output", "validation_report.json"
                ],
                "Schema Validation"
            )
            if not val_success:
                notes.append("Validation failed; lifecycle halted to prevent dirty delta.")
                logger.error("Halting pipeline: Current snapshot failed validation checks.")
                return False

            # Step 2: Compute Deltas (Only reached if validation passes)
            delta_success, delta_output_log = self._run_command(
                [
                    sys.executable, str(delta_script),
                    "--baseline", baseline_csv,
                    "--current", current_csv,
                    "--output", delta_output
                ],
                "Delta Engine"
            )
            if not delta_success:
                notes.append("Delta engine computation failed.")
                return False

            anomalies = self._count_csv_records(delta_output)

            # Step 3: Alerts (Only triggered if actionable anomalies exist)
            if anomalies > 0:
                logger.info(f"Detected {anomalies} anomaly records. Triggering alerts...")
                alert_success, alert_log = self._run_command(
                    [
                        sys.executable, str(alert_script),
                        "--deltas", str(delta_output),
                        "--send"
                    ],
                    "Alert Dispatcher"
                )
                if not alert_success:
                    notes.append("Alert dispatch failed.")
            else:
                logger.info("No delta anomalies detected. Skipping alert dispatch.")

            status = "SUCCESS"
            return True

        except Exception as err:
            notes.append(f"Fatal orchestration error: {str(err)}")
            logger.critical(f"Orchestration crashed: {err}")
            return False

        finally:
            elapsed = round(time.time() - start_time, 3)
            with self.conn:
                self.conn.execute("""
                    INSERT INTO pipeline_runs 
                    (pipeline_name, status, extracted_records, anomalies_detected, 
                     execution_time_sec, executed_at, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    "ecom_monitor",
                    status,
                    extracted_records,
                    anomalies,
                    elapsed,
                    run_timestamp,
                    "; ".join(notes) if notes else None
                ))
            logger.info(f"=== Run Complete | Status: {status} | Duration: {elapsed}s | Anomalies: {anomalies} ===")

    def close(self):
        if self.conn:
            self.conn.close()


def main():
    parser = argparse.ArgumentParser(description="Run the e-commerce monitoring pipeline coordinator")
    parser.add_argument("--baseline", required=True, help="Path to previous baseline CSV snapshot")
    parser.add_argument("--current", required=True, help="Path to current run CSV snapshot")
    parser.add_argument("--output", default="delta_results.csv", help="Destination path for delta results")
    parser.add_argument("--db", default="orchestration_audit.db", help="Audit SQLite database path")
    args = parser.parse_args()

    coordinator = PipelineCoordinator(db_path=args.db)
    try:
        success = coordinator.execute_lifecycle(
            baseline_csv=args.baseline,
            current_csv=args.current,
            delta_output=args.output
        )
        sys.exit(0 if success else 1)
    finally:
        coordinator.close()


if __name__ == "__main__":
    main()