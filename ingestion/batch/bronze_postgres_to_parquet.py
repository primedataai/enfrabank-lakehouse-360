import argparse
import os
import tempfile
from pathlib import Path
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lit, current_timestamp, to_date
)

ROOT = Path(__file__).resolve().parents[2]


def load_env():
    env_file = ROOT / ".env"
    if not env_file.exists():
        raise FileNotFoundError(".env file not found in repository root")

    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


TABLES = {
    "reference_data": ["currency", "branch"],
    "consumer_banking": [
        "customer", "deposit_account", "deposit_transaction",
        "credit_card_account", "card_transaction", "loan_account",
        "loan_payment", "atm_terminal", "atm_transaction"
    ],
    "wholesale_banking": [
        "corporate_customer", "corporate_account", "credit_facility"
    ],
}


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("RUSTFS_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ["RUSTFS_ACCESS_KEY"],
        aws_secret_access_key=os.environ["RUSTFS_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"}
        ),
    )


def ensure_bucket(s3, bucket):
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)
        print(f"Created bucket: {bucket}")
    else:
        print(f"Bucket already exists: {bucket}")


def main():
    parser = argparse.ArgumentParser(
        description="Extract PostgreSQL banking tables to Parquet in RustFS"
    )
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--load-type", choices=["FULL", "INCREMENTAL"], default="FULL")
    parser.add_argument("--load-date", default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--tables", nargs="*", help="Optional table names to extract")
    args = parser.parse_args()

    if args.load_type == "INCREMENTAL":
        print("WARNING: INCREMENTAL is currently metadata only; this script performs a full table read.")

    load_env()

    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "enfrabank")
    db_user = os.getenv("POSTGRES_USER", "bankadmin")
    db_password = os.environ["POSTGRES_PASSWORD"]

    bucket = os.getenv("BRONZE_BUCKET", "enfrabank-bronze")
    s3 = get_s3_client()
    ensure_bucket(s3, bucket)

    selected = []
    for schema, tables in TABLES.items():
        for table in tables:
            if not args.tables or table in args.tables:
                selected.append((schema, table))

    if args.tables:
        found = {table for _, table in selected}
        missing = set(args.tables) - found
        if missing:
            raise ValueError(f"Unknown table name(s): {', '.join(sorted(missing))}")

    spark = (
        SparkSession.builder
        .appName("EnfraBank-PostgreSQL-to-Bronze")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.5"
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    results = []

    try:
        for schema, table in selected:
            print(f"\nExtracting {schema}.{table}")

            query = f'"{schema}"."{table}"'

            df = (
                spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", query)
                .option("user", db_user)
                .option("password", db_password)
                .option("driver", "org.postgresql.Driver")
                .option("fetchsize", "10000")
                .load()
            )

            row_count = df.count()

            enriched = (
                df.withColumn("_source_system", lit("PostgreSQL"))
                .withColumn("_source_schema", lit(schema))
                .withColumn("_source_table", lit(table))
                .withColumn("_batch_id", lit(args.batch_id))
                .withColumn("_load_type", lit(args.load_type))
                .withColumn("_ingestion_timestamp", current_timestamp())
                .withColumn("load_date", to_date(lit(args.load_date)))
                .withColumn("_batch_created_at", lit(
                    datetime.now(timezone.utc).isoformat()
                ))
            )

            with tempfile.TemporaryDirectory(prefix="enfrabank_bronze_") as temp_dir:
                local_output = Path(temp_dir) / "output"

                (
                    enriched.write
                    .mode("overwrite")
                    .partitionBy("load_date", "_batch_id")
                    .parquet(str(local_output))
                )

                uploaded = 0
                for file_path in local_output.rglob("*.parquet"):
                    relative_path = file_path.relative_to(local_output)
                    key = f"{schema}/{table}/{relative_path.as_posix()}"
                    s3.upload_file(str(file_path), bucket, key)
                    uploaded += 1

            results.append((schema, table, row_count, uploaded))
            print(
                f"Completed {schema}.{table}: "
                f"{row_count:,} rows, {uploaded} Parquet file(s)"
            )

        print("\n===== BRONZE INGESTION SUMMARY =====")
        for schema, table, rows, files in results:
            print(f"{schema}.{table}: {rows:,} rows | {files} file(s)")

        print(f"\nBucket: s3://{bucket}/")
        print("Bronze batch completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
