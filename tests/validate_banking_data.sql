-- EnfraBank Lakehouse 360
-- Synthetic Banking Data Validation

-- 1. Record count validation
SELECT 'customers' AS entity, COUNT(*) AS actual, 100 AS expected
FROM consumer_banking.customer
UNION ALL
SELECT 'deposit_accounts', COUNT(*), 150
FROM consumer_banking.deposit_account
UNION ALL
SELECT 'deposit_transactions', COUNT(*), 500
FROM consumer_banking.deposit_transaction
UNION ALL
SELECT 'credit_card_accounts', COUNT(*), 50
FROM consumer_banking.credit_card_account
UNION ALL
SELECT 'card_transactions', COUNT(*), 200
FROM consumer_banking.card_transaction
UNION ALL
SELECT 'loan_accounts', COUNT(*), 30
FROM consumer_banking.loan_account
UNION ALL
SELECT 'loan_payments', COUNT(*), 100
FROM consumer_banking.loan_payment
UNION ALL
SELECT 'atm_terminals', COUNT(*), 10
FROM consumer_banking.atm_terminal
UNION ALL
SELECT 'atm_transactions', COUNT(*), 100
FROM consumer_banking.atm_transaction
UNION ALL
SELECT 'corporate_customers', COUNT(*), 10
FROM wholesale_banking.corporate_customer
UNION ALL
SELECT 'corporate_accounts', COUNT(*), 15
FROM wholesale_banking.corporate_account
UNION ALL
SELECT 'credit_facilities', COUNT(*), 10
FROM wholesale_banking.credit_facility
ORDER BY entity;


-- 2. Referential integrity checks
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
