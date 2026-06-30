"""
Configuration for the orders-etl pipeline.

All paths are local /tmp paths for this demo. In production, swap to
s3://your-bucket/... and read from environment.
"""

from dataclasses import dataclass


@dataclass
class Config:
    landing_dir: str       = "/tmp/orders_etl/landing"
    bronze_path: str       = "/tmp/orders_etl/bronze/raw_orders"
    silver_orders_path: str = "/tmp/orders_etl/silver/orders"
    silver_customers_path: str = "/tmp/orders_etl/silver/customers"
    silver_dlt_path: str   = "/tmp/orders_etl/silver/dead_letter"
    gold_revenue_path: str = "/tmp/orders_etl/gold/revenue_by_region_daily"
    gold_top_skus_path: str = "/tmp/orders_etl/gold/top_skus_30d"
    checkpoint_root: str   = "/tmp/orders_etl/checkpoints"


CONFIG = Config()
