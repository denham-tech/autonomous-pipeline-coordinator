# Autonomous E-Commerce Pipeline Coordinator

Production orchestration engine engineered in **Python** and **SQLite** to manage the end-to-end data lifecycle for dynamic retail intelligence feeds. Coordinates extraction, snapshot delta calculations, and telemetry dispatch with integrated runtime auditing.

## Core Capabilities
- **Lifecycle Orchestration:** Coordinates asynchronous crawl jobs, Pandas-driven snapshot diffing, and webhook notifications sequentially.
- **Fault-Tolerant Execution:** Encapsulates pipeline steps in structured exception handling to ensure uninterrupted data delivery.
- **Operational Auditing:** Automatically logs execution latency, extraction record counts, and anomaly counts into a persistent SQLite runtime database.

## Architecture
- `coordinator.py` - Master orchestration and lifecycle controller.
- `orchestration_audit.db` - Persistent audit warehouse tracking pipeline reliability and performance metrics (untracked).