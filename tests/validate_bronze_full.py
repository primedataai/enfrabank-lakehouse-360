import argparse
import csv
import os
import sys
import tempfile
from pathlib import Path

import boto3
import psycopg
from botocore.config import Config
from psycopg import sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

ROOT = Path(__file__).resolve().parents[1]

TABLES = {
    "reference_data": {
        "currency": "currency_code",
        "branch": "branch_id",
    },
    "consumer_banking": {
        "customer": "customer_id",
        "deposit_account": "account_id",
        "deposit_transaction": "transaction_id",
        "credit_card_account": "card_account_id",
        "card_transaction": "card_transaction_id",
        "loan_account": "loan_id",
        "loan_payment": "payment_id",
        "atm_terminal": "atm_id",
        "atm_transaction": "atm_transaction_id",
    },
    "wholesale_banking": {
        "corporate_customer": "corporate_customer_id",
        "corporate_account": "corporate_account_id",
        "credit_facility": "facility_id",
    },
}

REQUIRED_METADATA = [
    "_source_system",
    "_source_schema",
    "_source_table",
    "_batch_id",
    "_load_type",
    "_ingestion_timestamp",
    "_batch_created_at",
    "load_date",
]


def load_env():
    env_file = ROOT / ".env"
    if not env_file.exists():
        raise FileNotFoundError(".env file not found in repository root")

    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(
            key.strip(),
            value.strip().strip('"').strip("'")
        )


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv(
            "RUSTFS_ENDPOINT", "http://localhost:9000"
        ),
        aws_access_key_id=os.environ["RUSTFS_ACCESS_KEY"],
        aws_secret_access_key=os.environ["RUSTFS_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )


def get_source_count(connection, schema, table):
    query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
        sql.Identifier(schema),
        sql.Identifier(table),
    )

    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchone()[0]


def list_batch_objects(s3, bucket, schema, table, batch_id, load_date):
    prefix = f"{schema}/{table}/"
    paginator = s3.get_paginator("list_objects_v2")

    objects = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            is_parquet = key.endswith(".parquet")
            has_date = f"load_date={load_date}/" in key
            has_batch = f"_batch_id={batch_id}/" in key

            if is_parquet and has_date and has_batch:
                objects.append(obj)

    return objects


def validate_table(
    spark,
    s3,
    connection,
    bucket,
    schema,
    table,
    primary_key,
    batch_id,
    load_date,
    temp_root,
):
    result = {
        "schema": schema,
        "table": table,
        "primary_key": primary_key,
        "source_rows": "",
        "bronze_rows": "",
        "difference": "",
        "parquet_files": 0,
        "object_bytes": 0,
        "null_primary_keys": "",
        "duplicate_primary_keys": "",
        "metadata_check": "NOT_CHECKED",
        "status": "FAIL",
        "details": "",
    }

    try:
        source_rows = get_source_count(connection, schema, table)
        result["source_rows"] = source_rows

        objects = list_batch_objects(
            s3, bucket, schema, table, batch_id, load_date
        )

        result["parquet_files"] = len(objects)
        result["object_bytes"] = sum(obj["Size"] for obj in objects)

        if not objects:
            raise RuntimeError("No Parquet objects found for this batch")

        if result["object_bytes"] <= 0:
            raise RuntimeError("Parquet objects have zero total bytes")

        # Recreate the S3 partition hierarchy locally so Spark can
        # discover load_date and _batch_id from the folder names.
        table_dir = Path(temp_root) / schema / table

        for index, obj in enumerate(objects):
            prefix = f"{schema}/{table}/"
            relative_key = obj["Key"][len(prefix):]
            local_path = table_dir / relative_key
            local_path.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket, obj["Key"], str(local_path))

        df = spark.read.parquet(str(table_dir))
        bronze_rows = df.count()

        result["bronze_rows"] = bronze_rows
        result["difference"] = bronze_rows - source_rows

        # Required metadata columns
        missing_columns = [
            name for name in REQUIRED_METADATA
            if name not in df.columns
        ]

        if missing_columns:
            raise RuntimeError(
                f"Missing metadata columns: {missing_columns}"
            )

        # Verify that all rows have the expected batch metadata.
        metadata_values = (
            df.select(
                "_source_system",
                "_source_schema",
                "_source_table",
                "_batch_id",
                "_load_type",
                "load_date",
            )
            .distinct()
            .limit(2)
            .collect()
        )

        if len(metadata_values) != 1:
            raise RuntimeError(
                "Expected exactly one distinct metadata combination"
            )

        metadata = metadata_values[0].asDict()

        expected_metadata = {
            "_source_system": "PostgreSQL",
            "_source_schema": schema,
            "_source_table": table,
            "_batch_id": batch_id,
            "_load_type": "FULL",
            "load_date": load_date,
        }

        metadata_errors = []

        for key, expected in expected_metadata.items():
            actual = metadata.get(key)
            if str(actual) != str(expected):
                metadata_errors.append(
                    f"{key}: expected {expected}, got {actual}"
                )

        if metadata_errors:
            raise RuntimeError("; ".join(metadata_errors))

        result["metadata_check"] = "PASS"

        # Primary-key quality checks
        null_pk = df.filter(col(primary_key).isNull()).count()

        duplicate_pk = (
            df.groupBy(primary_key)
            .count()
            .filter(col("count") > 1)
            .limit(1)
            .count()
        )

        result["null_primary_keys"] = null_pk
        result["duplicate_primary_keys"] = duplicate_pk

        failures = []

        if bronze_rows != source_rows:
            failures.append(
                f"Row-count mismatch: source={source_rows}, "
                f"bronze={bronze_rows}"
            )

        if null_pk:
            failures.append(f"{null_pk} null primary key(s)")

        if duplicate_pk:
            failures.append("Duplicate primary key(s) detected")

        if failures:
            result["details"] = "; ".join(failures)
            result["status"] = "FAIL"
        else:
            result["details"] = "All table checks passed"
            result["status"] = "PASS"

    except Exception as exc:
        result["details"] = str(exc)
        result["status"] = "FAIL"

    print(
        f"{schema}.{table}: "
        f"source={result['source_rows']} "
        f"bronze={result['bronze_rows']} "
        f"status={result['status']}"
    )

    if result["details"] and result["status"] == "FAIL":
        print(f"  Details: {result['details']}")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Full PostgreSQL-to-RustFS Bronze reconciliation"
    )
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--load-date", required=True)
    args = parser.parse_args()

    load_env()

    bucket = os.getenv("BRONZE_BUCKET", "enfrabank-bronze")

    connection = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "enfrabank"),
        user=os.getenv("POSTGRES_USER", "bankadmin"),
        password=os.environ["POSTGRES_PASSWORD"],
    )

    s3 = get_s3_client()

    spark = (
        SparkSession.builder
        .appName("EnfraBank-Full-Bronze-Reconciliation")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    results = []

    try:
        with tempfile.TemporaryDirectory(
            prefix="enfrabank_full_reconciliation_"
        ) as temp_root:

            print("\n================================================")
            print("ENFRABANK BRONZE RECONCILIATION")
            print("================================================")
            print(f"Batch ID : {args.batch_id}")
            print(f"Load date: {args.load_date}")
            print(f"Bucket   : {bucket}")
            print("================================================\n")

            for schema, table_map in TABLES.items():
                for table, primary_key in table_map.items():
                    result = validate_table(
                        spark=spark,
                        s3=s3,
                        connection=connection,
                        bucket=bucket,
                        schema=schema,
                        table=table,
                        primary_key=primary_key,
                        batch_id=args.batch_id,
                        load_date=args.load_date,
                        temp_root=temp_root,
                    )
                    results.append(result)

        report_dir = ROOT / "reports"
        report_dir.mkdir(exist_ok=True)

        report_file = (
            report_dir
            / f"bronze_reconciliation_{args.batch_id}.csv"
        )

        fieldnames = list(results[0].keys())

        with report_file.open("w", newline="") as csv_file:
            writer = csv.DictWriter(
                csv_file, fieldnames=fieldnames
            )
            writer.writeheader()
            writer.writerows(results)

        passed = sum(r["status"] == "PASS" for r in results)
        failed = len(results) - passed
        source_total = sum(
            r["source_rows"] for r in results
            if isinstance(r["source_rows"], int)
        )
        bronze_total = sum(
            r["bronze_rows"] for r in results
            if isinstance(r["bronze_rows"], int)
        )

        print("\n================================================")
        print("BRONZE RECONCILIATION SUMMARY")
        print("================================================")
        print(f"Tables checked : {len(results)}")
        print(f"Passed         : {passed}")
        print(f"Failed         : {failed}")
        print(f"Source rows    : {source_total:,}")
        print(f"Bronze rows    : {bronze_total:,}")
        print(f"Difference     : {bronze_total - source_total:,}")
        print(f"Report         : {report_file.relative_to(ROOT)}")
        print("================================================")

        if failed:
            print("OVERALL RESULT: FAIL")
            sys.exit(1)

        print("OVERALL RESULT: PASS")

    finally:
        spark.stop()
        connection.close()


if __name__ == "__main__":
    main()
