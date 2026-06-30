"""
Schemas for the orders-etl pipeline.

Defined once, reused everywhere — this is how schema drift gets caught early.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, TimestampType, DateType, LongType
)


# Source JSON schema (bronze ingestion)
ORDER_JSON_SCHEMA = StructType([
    StructField("order_id",    StringType(),       nullable=False),
    StructField("customer_id", StringType(),       nullable=False),
    StructField("sku",         StringType(),       nullable=False),
    StructField("amount",      DecimalType(18, 2), nullable=False),
    StructField("region",      StringType(),       nullable=True),
    StructField("order_time",  TimestampType(),    nullable=False),
])


# Silver orders schema (typed, with added columns)
SILVER_ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "sku",
    "amount",
    "region",
    "order_time",
    "order_date",   # derived from order_time
    "updated_at",   # ingestion time
    "batch_id",     # source batch
]


# Silver customers schema (a tiny dim table)
SILVER_CUSTOMER_COLUMNS = [
    "customer_id",
    "country",
    "tier",
]
