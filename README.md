# Autonomous Pipeline Coordinator

End-to-end execution orchestrator for e-commerce catalog monitoring. Automates the full sequential lifecycle: live Shopify extraction, tabular schema validation, relational delta computing, and telemetry event logging.

## Workflow
1. **Catalog Ingestion:** Extracts live product variants directly from storefront endpoints.
2. **Schema Hygiene Gate:** Validates primary keys, checks null rates, and enforces relational integrity.
3. **State Diffing:** Performs outer merges against baseline snapshots to isolate price adjustments, stockouts, and SKU changes.
4. **Telemetry Logging:** Prepares structured event payloads for alerting channels.

## Usage

```bash
# Execute monitoring cycle against a live storefront
python coordinator.py --url [https://colourpop.com](https://colourpop.com)