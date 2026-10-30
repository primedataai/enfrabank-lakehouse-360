-- EnfraBank Lakehouse 360
-- Full Dataset Validation

-- 1. Expected record counts
WITH actual_counts AS (
    SELECT 'customers' AS entity, COUNT(*) AS actual
    FROM consumer_banking.customer
    UNION ALL
    SELECT 'deposit_accounts', COUNT(*)
    FROM consumer_banking.deposit_account
    UNION ALL
    SELECT 'deposit_transactions', COUNT(*)
    FROM consumer_banking.deposit_transaction
    UNION ALL
    SELECT 'credit_card_accounts', COUNT(*)
    FROM consumer_banking.credit_card_account
    UNION ALL
    SELECT 'card_transactions', COUNT(*)
    FROM consumer_banking.card_transaction
    UNION ALL
    SELECT 'loan_accounts', COUNT(*)
    FROM consumer_banking.loan_account
    UNION ALL
    SELECT 'loan_payments', COUNT(*)
    FROM consumer_banking.loan_payment
    UNION ALL
    SELECT 'atm_terminals', COUNT(*)
    FROM consumer_banking.atm_terminal
    UNION ALL
    SELECT 'atm_transactions', COUNT(*)
    FROM consumer_banking.atm_transaction
    UNION ALL
    SELECT 'corporate_customers', COUNT(*)
    FROM wholesale_banking.corporate_customer
    UNION ALL
    SELECT 'corporate_accounts', COUNT(*)
    FROM wholesale_banking.corporate_account
    UNION ALL
    SELECT 'credit_facilities', COUNT(*)
    FROM wholesale_banking.credit_facility
),
expected_counts(entity, expected) AS (
    VALUES
        ('customers', 10000::BIGINT),
        ('deposit_accounts', 15000::BIGINT),
        ('deposit_transactions', 100000::BIGINT),
        ('credit_card_accounts', 5000::BIGINT),
        ('card_transactions', 50000::BIGINT),
        ('loan_accounts', 3000::BIGINT),
        ('loan_payments', 20000::BIGINT),
        ('atm_terminals', 100::BIGINT),
        ('atm_transactions', 25000::BIGINT),
        ('corporate_customers', 500::BIGINT),
        ('corporate_accounts', 800::BIGINT),
        ('credit_facilities', 600::BIGINT)
)
SELECT
    a.entity,
    a.actual,
    e.expected,
    CASE WHEN a.actual = e.expected THEN 'PASS' ELSE 'FAIL' END AS status
FROM actual_counts a
JOIN expected_counts e USING (entity)
ORDER BY a.entity;


-- 2. Referential integrity: orphan record counts
SELECT 'deposit_account_customer' AS check_name, COUNT(*) AS orphan_records
FROM consumer_banking.deposit_account a
LEFT JOIN consumer_banking.customer c
    ON a.customer_id = c.customer_id
WHERE c.customer_id IS NULL

UNION ALL
SELECT 'deposit_transaction_account', COUNT(*)
FROM consumer_banking.deposit_transaction t
LEFT JOIN consumer_banking.deposit_account a
    ON t.account_id = a.account_id
WHERE a.account_id IS NULL

UNION ALL
SELECT 'card_transaction_account', COUNT(*)
FROM consumer_banking.card_transaction t
LEFT JOIN consumer_banking.credit_card_account a
    ON t.card_account_id = a.card_account_id
WHERE a.card_account_id IS NULL

UNION ALL
SELECT 'loan_payment_loan', COUNT(*)
FROM consumer_banking.loan_payment p
LEFT JOIN consumer_banking.loan_account l
    ON p.loan_id = l.loan_id
WHERE l.loan_id IS NULL

UNION ALL
SELECT 'atm_transaction_account', COUNT(*)
FROM consumer_banking.atm_transaction t
LEFT JOIN consumer_banking.deposit_account a
    ON t.account_id = a.account_id
WHERE a.account_id IS NULL

UNION ALL
SELECT 'atm_transaction_terminal', COUNT(*)
FROM consumer_banking.atm_transaction t
LEFT JOIN consumer_banking.atm_terminal a
    ON t.atm_id = a.atm_id
WHERE a.atm_id IS NULL

UNION ALL
SELECT 'atm_terminal_branch', COUNT(*)
FROM consumer_banking.atm_terminal t
LEFT JOIN reference_data.branch b
    ON t.branch_id = b.branch_id
WHERE b.branch_id IS NULL

UNION ALL
SELECT 'corporate_account_customer', COUNT(*)
FROM wholesale_banking.corporate_account a
LEFT JOIN wholesale_banking.corporate_customer c
    ON a.corporate_customer_id = c.corporate_customer_id
WHERE c.corporate_customer_id IS NULL

UNION ALL
SELECT 'credit_facility_customer', COUNT(*)
FROM wholesale_banking.credit_facility f
LEFT JOIN wholesale_banking.corporate_customer c
    ON f.corporate_customer_id = c.corporate_customer_id
WHERE c.corporate_customer_id IS NULL
ORDER BY check_name;


-- 3. Total generated records
SELECT
    (SELECT COUNT(*) FROM consumer_banking.customer)
  + (SELECT COUNT(*) FROM consumer_banking.deposit_account)
  + (SELECT COUNT(*) FROM consumer_banking.deposit_transaction)
  + (SELECT COUNT(*) FROM consumer_banking.credit_card_account)
  + (SELECT COUNT(*) FROM consumer_banking.card_transaction)
  + (SELECT COUNT(*) FROM consumer_banking.loan_account)
  + (SELECT COUNT(*) FROM consumer_banking.loan_payment)
  + (SELECT COUNT(*) FROM consumer_banking.atm_terminal)
  + (SELECT COUNT(*) FROM consumer_banking.atm_transaction)
  + (SELECT COUNT(*) FROM wholesale_banking.corporate_customer)
  + (SELECT COUNT(*) FROM wholesale_banking.corporate_account)
  + (SELECT COUNT(*) FROM wholesale_banking.credit_facility)
    AS total_banking_records;
