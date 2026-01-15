import argparse
import json
from datetime import datetime
from databricks.sdk.runtime import spark
from pyspark.dbutils import DBUtils
from cicd_dbs import taxis


RUN_MODE_LIMITS = {
    "sample": 1000,
    "full": None,
}


def main():
    # Process command-line arguments
    parser = argparse.ArgumentParser(
        description="Databricks job with catalog and schema parameters",
    )
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument(
        "--run-mode",
        dest="run_mode",
        choices=RUN_MODE_LIMITS.keys(),
        default="sample",
        help="Controls how much data the job processes.",
    )
    args = parser.parse_args()

    # Set the default catalog and schema
    spark.sql(f"USE CATALOG {args.catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")
    spark.sql(f"USE SCHEMA {args.schema}")

    # Example: read sample taxi data with optional limit based on run mode
    limit = RUN_MODE_LIMITS[args.run_mode]
    df = taxis.find_all_taxis(limit=limit)
    df.show(5)

    row_count = df.count()

    # Persist metadata for CI verification
    output_dir = f"dbfs:/tmp/cicd_dbs/run_mode={args.run_mode}"
    dbutils = DBUtils(spark)
    dbutils.fs.mkdirs(output_dir)
    payload = {
        "run_mode": args.run_mode,
        "row_count": row_count,
        "catalog": args.catalog,
        "schema": args.schema,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    dbutils.fs.put(f"{output_dir}/latest.json", json.dumps(payload), True)
    print(
        f"Verification payload written to {output_dir}/latest.json with row_count={row_count}",
    )


if __name__ == "__main__":
    main()
