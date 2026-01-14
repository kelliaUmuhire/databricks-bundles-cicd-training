from databricks.sdk.runtime import spark
from pyspark.sql import DataFrame


def find_all_taxis(limit: int | None = None) -> DataFrame:
    """Find taxi data with an optional row limit."""
    df = spark.read.table("samples.nyctaxi.trips")
    return df.limit(limit) if limit else df
