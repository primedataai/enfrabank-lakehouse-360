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
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))

load_env()

bucket = os.getenv("BRONZE_BUCKET", "enfrabank-bronze")
prefix = "consumer_banking/customer/"

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("RUSTFS_ENDPOINT", "http://localhost:9000"),
    aws_access_key_id=os.environ["RUSTFS_ACCESS_KEY"],
    aws_secret_access_key=os.environ["RUSTFS_SECRET_KEY"],
    region_name="us-east-1",
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)

objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
parquet_objects = [obj for obj in objects if obj["Key"].endswith(".parquet")]

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
    with tempfile.TemporaryDirectory(prefix="bronze_validation_") as temp_dir:
        local_files = []

        for index, obj in enumerate(parquet_objects):
            local_path = Path(temp_dir) / f"customer_{index}.parquet"
            s3.download_file(bucket, obj["Key"], str(local_path))
            local_files.append(str(local_path))

        df = spark.read.parquet(*local_files)
        actual_rows = df.count()

        print("\n===== PARQUET DATA VALIDATION =====")
        print(f"Rows read by Spark: {actual_rows:,}")
        print("Columns:")
        print(", ".join(df.columns))

        required_metadata = [
            "_source_system", "_source_schema", "_source_table",
            "_batch_id", "_load_type", "_ingestion_timestamp", "load_date"
        ]

        missing = [c for c in required_metadata if c not in df.columns]
        if missing:
            raise RuntimeError(f"Missing metadata columns: {missing}")

        if actual_rows != 10000:
            raise RuntimeError(f"Expected 10,000 rows, found {actual_rows:,}")

        print("\nPASS: RustFS objects exist and Spark successfully read 10,000 rows.")
finally:
    spark.stop()
