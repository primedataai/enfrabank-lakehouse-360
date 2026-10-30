#!/usr/bin/env python3
"""
EnfraBank Lakehouse 360
Generate reproducible synthetic banking data and load it into PostgreSQL.

WARNING:
Use only with the EnfraBank demo database. The --reset option clears
the existing demo tables before loading a fresh dataset.
"""

import argparse
import os
import random
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal

try:
    import psycopg
except ImportError:
    print("Missing dependency. Install it with: pip install 'psycopg[binary]'")
    sys.exit(1)


FIRST_NAMES = [
    "Aarav", "Aisha", "Amir", "Daniel", "Farah", "Hannah", "Ismail",
    "James", "Kavitha", "Lim", "Mei", "Nur", "Oliver", "Priya",
    "Ravi", "Sofia", "Wei", "Yusuf", "Zara", "Adam",
]

LAST_NAMES = [
    "Abdullah", "Ahmed", "Brown", "Chong", "Das", "Fernandez",
    "Hassan", "Ismail", "Khan", "Lee", "Lim", "Martin",
    "Rahman", "Singh", "Tan", "Wong", "Yap", "Zain",
]

MERCHANTS = [
    "FreshMart", "City Grocer", "Metro Fuel", "Digital World",
    "Food Express", "Travel Hub", "HealthPlus", "Fashion House",
    "Online Marketplace", "Coffee Corner",
]

INDUSTRIES = [
    "Manufacturing", "Retail", "Technology", "Healthcare",
    "Logistics", "Construction", "Hospitality", "Education",
]

COUNTRIES = ["MY", "SG", "BD"]
CURRENCIES = ["MYR", "SGD", "BDT", "USD"]


def load_dotenv():
    """Load simple KEY=VALUE entries from the repository .env file."""
    env_path = os.path.join(os.getcwd(), ".env")
    if not os.path.isfile(env_path):
        return

    with open(env_path, encoding="utf-8") as env_file:
        for line in env_file:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def money(rng, minimum=10, maximum=10000):
    return round(rng.uniform(minimum, maximum), 2)


def random_timestamp(rng, start_days=730):
    now = datetime.now()
    return now - timedelta(
        days=rng.randint(0, start_days),
        seconds=rng.randint(0, 86399),
    )


def insert_rows(cur, table, columns, rows, batch_size=2000):
    """Insert rows in manageable batches."""
    if not rows:
        return

    column_sql = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})"

    for start in range(0, len(rows), batch_size):
        cur.executemany(sql, rows[start:start + batch_size])

    print(f"  Loaded {len(rows):,} rows into {table}")


def generate_data(args):
    rng = random.Random(args.seed)
    today = date.today()

    load_dotenv()

    db_config = {
        "host": os.getenv("PGHOST", "localhost"),
        "port": int(os.getenv("PGPORT", "5432")),
        "dbname": os.getenv("PGDATABASE", "enfrabank"),
        "user": os.getenv("PGUSER", "bankadmin"),
        "password": os.getenv("PGPASSWORD") or os.getenv("POSTGRES_PASSWORD"),
    }

    if not db_config["password"]:
        raise RuntimeError(
            "Database password not found. Check POSTGRES_PASSWORD in .env."
        )

    print("Connecting to EnfraBank PostgreSQL...")
    print(
        f"Target: {db_config['host']}:{db_config['port']}/"
        f"{db_config['dbname']} as {db_config['user']}"
    )

    with psycopg.connect(**db_config) as conn:
        with conn.cursor() as cur:
            # Confirm the expected database schemas exist.
            cur.execute("""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name IN (
                    'reference_data', 'consumer_banking',
                    'wholesale_banking', 'integration_audit'
                )
            """)
            schemas = {row[0] for row in cur.fetchall()}
            expected = {
                "reference_data", "consumer_banking",
                "wholesale_banking", "integration_audit",
            }
            if schemas != expected:
                raise RuntimeError(
                    f"Expected schemas {sorted(expected)}, found {sorted(schemas)}"
                )

            cur.execute("""
                SELECT COUNT(*)
                FROM consumer_banking.customer
            """)
            existing_customers = cur.fetchone()[0]

            if existing_customers and not args.reset:
                raise RuntimeError(
                    "The database already contains customer records. "
                    "Use --reset to clear and regenerate the demo dataset."
                )

            if args.reset:
                print("Resetting existing EnfraBank demo tables...")
                cur.execute("""
                    TRUNCATE TABLE
                        integration_audit.ingestion_log,
                        consumer_banking.atm_transaction,
                        consumer_banking.atm_terminal,
                        consumer_banking.loan_payment,
                        consumer_banking.loan_account,
                        consumer_banking.card_transaction,
                        consumer_banking.credit_card_account,
                        consumer_banking.deposit_transaction,
                        consumer_banking.deposit_account,
                        consumer_banking.customer,
                        wholesale_banking.credit_facility,
                        wholesale_banking.corporate_account,
                        wholesale_banking.corporate_customer
                    RESTART IDENTITY CASCADE
                """)

            # Read reference data already loaded by the schema SQL.
            cur.execute("SELECT currency_code FROM reference_data.currency")
            currencies = [row[0] for row in cur.fetchall()]
            cur.execute("SELECT branch_id FROM reference_data.branch")
            branch_ids = [row[0] for row in cur.fetchall()]

            if not currencies or not branch_ids:
                raise RuntimeError(
                    "Reference currency or branch data is missing. "
                    "Run the banking schema SQL first."
                )

            print("\nGenerating consumer banking data...")

            # 1. Customers
            customers = []
            for customer_id in range(1, args.customers + 1):
                first = rng.choice(FIRST_NAMES)
                last = rng.choice(LAST_NAMES)
                customers.append((
                    customer_id,
                    first,
                    last,
                    f"customer{customer_id}@example.com",
                    rng.choice(["Retail", "Priority", "Mass Affluent"]),
                    rng.choice(COUNTRIES),
                    random_timestamp(rng, 1800),
                    datetime.now(),
                ))

            insert_rows(
                cur, "consumer_banking.customer",
                ["customer_id", "first_name", "last_name", "email",
                 "customer_segment", "country_code", "created_at", "updated_at"],
                customers,
            )

            # 2. Deposit accounts
            deposit_accounts = []
            for account_id in range(1, args.deposit_accounts + 1):
                deposit_accounts.append((
                    account_id,
                    rng.randint(1, args.customers),
                    rng.choice(["Savings", "Current", "Fixed Deposit"]),
                    rng.choice(currencies),
                    rng.choice(["Active", "Active", "Active", "Dormant"]),
                    money(rng, 100, 150000),
                    random_timestamp(rng, 1800),
                    datetime.now(),
                ))

            insert_rows(
                cur, "consumer_banking.deposit_account",
                ["account_id", "customer_id", "account_type", "currency_code",
                 "account_status", "current_balance", "opened_at", "updated_at"],
                deposit_accounts,
            )

            # 3. Deposit transactions
            deposit_transactions = []
            for transaction_id in range(1, args.deposit_transactions + 1):
                deposit_transactions.append((
                    transaction_id,
                    rng.randint(1, args.deposit_accounts),
                    rng.choice(["Credit", "Debit", "Transfer"]),
                    money(rng, 5, 12000),
                    rng.choice(currencies),
                    random_timestamp(rng),
                    rng.choice(["Completed", "Completed", "Completed", "Pending"]),
                    datetime.now(),
                ))

            insert_rows(
                cur, "consumer_banking.deposit_transaction",
                ["transaction_id", "account_id", "transaction_type", "amount",
                 "currency_code", "transaction_timestamp",
                 "transaction_status", "updated_at"],
                deposit_transactions,
            )

            # 4. Credit card accounts
            card_accounts = []
            for card_id in range(1, args.credit_card_accounts + 1):
                limit = money(rng, 2000, 50000)
                card_accounts.append((
                    card_id,
                    rng.randint(1, args.customers),
                    limit,
                    round(limit * rng.uniform(0, 0.85), 2),
                    rng.choice(currencies),
                    rng.choice(["Active", "Active", "Blocked"]),
                    datetime.now(),
                ))

            insert_rows(
                cur, "consumer_banking.credit_card_account",
                ["card_account_id", "customer_id", "credit_limit",
                 "outstanding_balance", "currency_code", "card_status",
                 "updated_at"],
                card_accounts,
            )

            # 5. Card transactions
            card_transactions = []
            for transaction_id in range(1, args.card_transactions + 1):
                card_transactions.append((
                    transaction_id,
                    rng.randint(1, args.credit_card_accounts),
                    rng.choice(MERCHANTS),
                    money(rng, 2, 5000),
                    random_timestamp(rng),
                    rng.choice(["Approved", "Approved", "Approved", "Declined"]),
                    datetime.now(),
                ))

            insert_rows(
                cur, "consumer_banking.card_transaction",
                ["card_transaction_id", "card_account_id", "merchant_name",
                 "transaction_amount", "transaction_timestamp",
                 "transaction_status", "updated_at"],
                card_transactions,
            )

            # 6. Loan accounts
            loans = []
            for loan_id in range(1, args.loan_accounts + 1):
                principal = money(rng, 5000, 500000)
                loans.append((
                    loan_id,
                    rng.randint(1, args.customers),
                    rng.choice(["Personal", "Mortgage", "Auto", "Education"]),
                    principal,
                    round(principal * rng.uniform(0.1, 1.0), 2),
                    round(rng.uniform(2.5, 12.5), 4),
                    rng.choice(["Active", "Active", "Closed", "Delinquent"]),
                    today - timedelta(days=rng.randint(30, 2500)),
                    datetime.now(),
                ))

            insert_rows(
                cur, "consumer_banking.loan_account",
                ["loan_id", "customer_id", "loan_type", "principal_amount",
                 "outstanding_principal", "interest_rate", "loan_status",
                 "origination_date", "updated_at"],
                loans,
            )

            # 7. Loan payments
            loan_payments = []
            for payment_id in range(1, args.loan_payments + 1):
                loan_payments.append((
                    payment_id,
                    rng.randint(1, args.loan_accounts),
                    money(rng, 100, 8000),
                    today - timedelta(days=rng.randint(0, 900)),
                    rng.choice(["Paid", "Paid", "Paid", "Late"]),
                    datetime.now(),
                ))

            insert_rows(
                cur, "consumer_banking.loan_payment",
                ["payment_id", "loan_id", "payment_amount", "payment_date",
                 "payment_status", "updated_at"],
                loan_payments,
            )

            # 8. ATM terminals
            atm_terminals = []
            for atm_id in range(1, args.atm_terminals + 1):
                atm_terminals.append((
                    atm_id,
                    rng.choice(branch_ids),
                    f"ATM Location {atm_id:03d}",
                    rng.choice(COUNTRIES),
                    rng.choice(["Active", "Active", "Maintenance"]),
                ))

            insert_rows(
                cur, "consumer_banking.atm_terminal",
                ["atm_id", "branch_id", "location_name",
                 "country_code", "terminal_status"],
                atm_terminals,
            )

            # 9. ATM transactions
            atm_transactions = []
            for transaction_id in range(1, args.atm_transactions + 1):
                atm_transactions.append((
                    transaction_id,
                    rng.randint(1, args.atm_terminals),
                    rng.randint(1, args.deposit_accounts),
                    rng.choice(["Cash Withdrawal", "Balance Inquiry", "Cash Deposit"]),
                    money(rng, 20, 3000),
                    random_timestamp(rng),
                    rng.choice(["Completed", "Completed", "Failed"]),
                    datetime.now(),
                ))

            insert_rows(
                cur, "consumer_banking.atm_transaction",
                ["atm_transaction_id", "atm_id", "account_id",
                 "transaction_type", "amount", "transaction_timestamp",
                 "transaction_status", "updated_at"],
                atm_transactions,
            )

            print("\nGenerating wholesale banking data...")

            # 10. Corporate customers
            corporates = []
            for corporate_id in range(1, args.corporate_customers + 1):
                corporates.append((
                    corporate_id,
                    f"Demo Corporation {corporate_id:04d}",
                    rng.choice(INDUSTRIES),
                    rng.choice(COUNTRIES),
                    rng.choice(["Low", "Medium", "High"]),
                    random_timestamp(rng, 1800),
                    datetime.now(),
                ))

            insert_rows(
                cur, "wholesale_banking.corporate_customer",
                ["corporate_customer_id", "company_name", "industry",
                 "country_code", "risk_rating", "created_at", "updated_at"],
                corporates,
            )

            # 11. Corporate accounts
            corporate_accounts = []
            for account_id in range(1, args.corporate_accounts + 1):
                corporate_accounts.append((
                    account_id,
                    rng.randint(1, args.corporate_customers),
                    rng.choice(["Operating", "Payroll", "Collection"]),
                    rng.choice(currencies),
                    money(rng, 10000, 5000000),
                    rng.choice(["Active", "Active", "Dormant"]),
                    datetime.now(),
                ))

            insert_rows(
                cur, "wholesale_banking.corporate_account",
                ["corporate_account_id", "corporate_customer_id",
                 "account_type", "currency_code", "current_balance",
                 "account_status", "updated_at"],
                corporate_accounts,
            )

            # 12. Credit facilities
            facilities = []
            for facility_id in range(1, args.credit_facilities + 1):
                approved = money(rng, 100000, 10000000)
                facilities.append((
                    facility_id,
                    rng.randint(1, args.corporate_customers),
                    rng.choice(["Term Loan", "Revolving Credit", "Trade Finance"]),
                    approved,
                    round(approved * rng.uniform(0, 0.9), 2),
                    rng.choice(currencies),
                    rng.choice(["Active", "Active", "Expired"]),
                    datetime.now(),
                ))

            insert_rows(
                cur, "wholesale_banking.credit_facility",
                ["facility_id", "corporate_customer_id", "facility_type",
                 "approved_limit", "utilised_amount", "currency_code",
                 "facility_status", "updated_at"],
                facilities,
            )

            # Record this run in the ingestion audit table.
            cur.execute("""
                INSERT INTO integration_audit.ingestion_log
                (source_name, entity_name, ingestion_type,
                 records_received, records_loaded, ingestion_status,
                 started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                "EnfraBank Synthetic Generator",
                "Banking Demo Dataset",
                "Batch",
                sum([
                    args.customers, args.deposit_accounts,
                    args.deposit_transactions, args.credit_card_accounts,
                    args.card_transactions, args.loan_accounts,
                    args.loan_payments, args.atm_terminals,
                    args.atm_transactions, args.corporate_customers,
                    args.corporate_accounts, args.credit_facilities,
                ]),
                sum([
                    args.customers, args.deposit_accounts,
                    args.deposit_transactions, args.credit_card_accounts,
                    args.card_transactions, args.loan_accounts,
                    args.loan_payments, args.atm_terminals,
                    args.atm_transactions, args.corporate_customers,
                    args.corporate_accounts, args.credit_facilities,
                ]),
                "Success",
                datetime.now(),
                datetime.now(),
            ))

        # The connection context commits the transaction on success.
        print("\nSynthetic banking data loaded successfully.")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate synthetic EnfraBank banking data."
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--customers", type=int, default=10000)
    parser.add_argument("--deposit-accounts", type=int, default=15000)
    parser.add_argument("--deposit-transactions", type=int, default=100000)
    parser.add_argument("--credit-card-accounts", type=int, default=5000)
    parser.add_argument("--card-transactions", type=int, default=50000)
    parser.add_argument("--loan-accounts", type=int, default=3000)
    parser.add_argument("--loan-payments", type=int, default=20000)
    parser.add_argument("--atm-terminals", type=int, default=100)
    parser.add_argument("--atm-transactions", type=int, default=25000)
    parser.add_argument("--corporate-customers", type=int, default=500)
    parser.add_argument("--corporate-accounts", type=int, default=800)
    parser.add_argument("--credit-facilities", type=int, default=600)
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Clear existing demo data before generating a new dataset.",
    )
    args = parser.parse_args()

    positive_fields = [
        "customers", "deposit_accounts", "deposit_transactions",
        "credit_card_accounts", "card_transactions", "loan_accounts",
        "loan_payments", "atm_terminals", "atm_transactions",
        "corporate_customers", "corporate_accounts", "credit_facilities",
    ]
    for field in positive_fields:
        if getattr(args, field) < 1:
            parser.error(f"--{field.replace('_', '-')} must be at least 1")

    return args


if __name__ == "__main__":
    try:
        generate_data(parse_args())
    except Exception as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        sys.exit(1)
