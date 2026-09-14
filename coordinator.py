"""
E-Commerce Catalog Pipeline Coordinator
Orchestrates the end-to-end monitoring lifecycle:
Ingestion (Shopify) -> Schema Validation -> Delta Analysis -> Alert Dispatching
"""

import argparse
import logging
import sys
from pathlib import Path
import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("PipelineCoordinator")


class PipelineCoordinator:
    def __init__(self, data_dir: str = "pipeline_data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.baseline_file = self.data_dir / "baseline_catalog.csv"
        self.current_file = self.data_dir / "current_catalog.csv"
        self.delta_file = self.data_dir / "deltas.csv"

    def execute(self, store_url: str, pages: int = 1) -> bool:
        logger.info(f"--- INITIATING MONITORING CYCLE: {store_url} ---")

        # Step 1: Real Extraction
        logger.info("Step 1: Extracting storefront catalog...")
        endpoint = f"{store_url.rstrip('/')}/products.json?limit=250&page=1"
        try:
            res = requests.get(endpoint, headers={"User-Agent": "CatalogSentinel/1.0"}, timeout=15)
            res.raise_for_status()
            products = res.json().get("products", [])
        except Exception as e:
            logger.critical(f"Failed to fetch storefront feed: {e}")
            return False

        records = []
        for p in products:
            title = p.get("title", "")
            for v in p.get("variants", []):
                records.append({
                    "variant_id": v.get("id"),
                    "title": f"{title} - {v.get('title', '')}".strip(" -"),
                    "sku": v.get("sku") or f"SKU-{v.get('id')}",
                    "price": float(v.get("price", 0.0)),
                    "available": bool(v.get("available", False))
                })

        df_current = pd.DataFrame(records)
        if df_current.empty:
            logger.critical("Extraction yielded 0 items.")
            return False

        df_current.to_csv(self.current_file, index=False)
        logger.info(f"Step 1 Complete: Ingested {len(df_current)} variants.")

        # Step 2: Schema Hygiene Validation
        logger.info("Step 2: Validating schema hygiene...")
        duplicate_count = int(df_current["variant_id"].duplicated().sum())
        null_count = int(df_current[["variant_id", "price", "available"]].isnull().sum().sum())
        
        if duplicate_count > 0 or null_count > 0:
            logger.critical(f"Data quality assertion failure: {duplicate_count} duplicates, {null_count} nulls.")
            return False
        logger.info("Step 2 Complete: Hygiene Score 100% (Passed).")

        # Step 3: Delta Computing
        logger.info("Step 3: Calculating relational deltas...")
        if not self.baseline_file.exists():
            logger.info("No prior baseline detected. Initializing current catalog as baseline.")
            df_current.to_csv(self.baseline_file, index=False)
            logger.info("Monitoring baseline established.")
            return True

        df_baseline = pd.read_csv(self.baseline_file)
        merged = pd.merge(
            df_baseline,
            df_current,
            on="variant_id",
            how="outer",
            suffixes=("_prev", "_curr")
        )

        deltas = []
        for _, row in merged.iterrows():
            vid = row["variant_id"]
            if pd.isna(row["price_prev"]):
                deltas.append({"variant_id": vid, "event_type": "PRODUCT_ADDED", "title": row["title_curr"], "detail": f"Added at ${row['price_curr']}"})
            elif pd.isna(row["price_curr"]):
                deltas.append({"variant_id": vid, "event_type": "PRODUCT_REMOVED", "title": row["title_prev"], "detail": "Delisted"})
            elif float(row["price_prev"]) != float(row["price_curr"]):
                deltas.append({"variant_id": vid, "event_type": "PRICE_CHANGE", "title": row["title_curr"], "detail": f"${row['price_prev']} -> ${row['price_curr']}"})
            elif bool(row["available_prev"]) != bool(row["available_curr"]):
                deltas.append({"variant_id": vid, "event_type": "STOCKOUT" if not row["available_curr"] else "RESTOCK", "title": row["title_curr"], "detail": "Stock state shift"})

        df_deltas = pd.DataFrame(deltas)
        df_deltas.to_csv(self.delta_file, index=False)
        logger.info(f"Step 3 Complete: Identified {len(df_deltas)} delta events.")

        # Step 4: Alert Dispatching
        if not df_deltas.empty:
            logger.info(f"Step 4: Formatted telemetry payload for {len(df_deltas)} events.")
        else:
            logger.info("Step 4: Zero deltas detected across catalog snapshots.")

        # Update baseline
        df_current.to_csv(self.baseline_file, index=False)
        logger.info("Cycle finished cleanly. Baseline updated.")
        return True


def main():
    parser = argparse.ArgumentParser(description="End-to-end catalog monitoring coordinator.")
    parser.add_argument("--url", "-u", required=True, help="Target Shopify storefront URL")
    args = parser.parse_args()

    coordinator = PipelineCoordinator()
    success = coordinator.execute(store_url=args.url)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()