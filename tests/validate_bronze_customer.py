import os
import tempfile
from pathlib import Path

import boto3
from botocore.config import Config
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]

def load_env():
    for line in (ROOT / ".env").read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(
                key.strip(), value.strip().strip('"').strip("'")
            )

load_env()

bucket = os.getenv("BRONZE_BUCKET", "enfrabank-bronze")
prefix = "consumer_banking/customer/"

s3 = boto3.client(
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

objects = s3.list_objects_v2(
    Bucket=bucket, Prefix=prefix
).get("Contents", [])

parquet_objects = [
    obj for obj in objects if obj["Key"].endswith(".parquet")
]

if not parquet_objects:
    raise RuntimeError(f"No Parquet files found under {prefix}")

print("===== RUSTFS OBJECT VALIDATION =====")

total_bytes = sum(obj["Size"] for obj in parquet_objects)

for obj in parquet_objects:
    print(f"Object: {obj['Key']}")
    print(f"Size: {obj['Size']:,} bytes")

print(f"Parquet files: {len(parquet_objects)}")
print(f"Total size: {total_bytes:,} bytes")

if total_bytes == 0:
    raise RuntimeError("Parquet objects have zero bytes")

spark = (
    SparkSession.builder
    .appName("EnfraBank-Bronze-Validation")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

try:
    with tempfile.TemporaryDirectory(
        prefix="bronze_validation_"
    ) as temp_dir:

        # Recreate the original S3 folder hierarchy.
        table_dir = Path(temp_dir) / "customer"

        for obj in parquet_objects:
            relative_key = obj["Key"][len(prefix):]
            local_path = table_dir / relative_key
            local_path.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket, obj["Key"], str(local_path))

        # Reading the table directory allows Spark to discover
        # load_date and _batch_id from the partition folders.
        df = spark.read.parquet(str(table_dir))
        actual_rows = df.count()

        print("\n===== PARQUET DATA VALIDATION =====")
        print(f"Rows read by Spark: {actual_rows:,}")
        print("Columns:")
        print(", ".join(df.columns))

        required_columns = [
            "customer_id",
            "_source_system",
            "_source_schema",
            "_source_table",
            "_load_type",
            "_ingestion_timestamp",
            "_batch_created_at",
            "_batch_id",
            "load_date",
        ]

        missing = [
            col for col in required_columns if col not in df.columns
        ]

        if missing:
            raise RuntimeError(f"Missing columns: {missing}")

        print("\n===== METADATA VALIDATION =====")
        df.select(
            "_source_system",
            "_source_schema",
            "_source_table",
            "_batch_id",
            "_load_type",
            "load_date",
        ).distinct().show(truncate=False)

        if actual_rows != 10000:
            raise RuntimeError(
                f"Expected 10,000 rows, found {actual_rows:,}"
            )

        print(
            "\nPASS: RustFS objects exist, partition columns "
            "were recovered, and Spark read 10,000 rows."
        )

finally:
    spark.stop()
