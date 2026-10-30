CREATE SCHEMA IF NOT EXISTS reference_data;
CREATE SCHEMA IF NOT EXISTS consumer_banking;
CREATE SCHEMA IF NOT EXISTS wholesale_banking;
CREATE SCHEMA IF NOT EXISTS integration_audit;

CREATE TABLE reference_data.currency (
    currency_code VARCHAR(3) PRIMARY KEY,
    currency_name VARCHAR(50) NOT NULL
);

CREATE TABLE reference_data.branch (
    branch_id SERIAL PRIMARY KEY,
    branch_name VARCHAR(100) NOT NULL,
    country_code VARCHAR(2) NOT NULL
);

CREATE TABLE consumer_banking.customer (
    customer_id BIGINT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    customer_segment VARCHAR(30),
    country_code VARCHAR(2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE consumer_banking.deposit_account (
    account_id BIGINT PRIMARY KEY,
    customer_id BIGINT REFERENCES consumer_banking.customer(customer_id),
    account_type VARCHAR(30),
    currency_code VARCHAR(3) REFERENCES reference_data.currency(currency_code),
    account_status VARCHAR(20),
    current_balance NUMERIC(18,2),
    opened_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE consumer_banking.deposit_transaction (
    transaction_id BIGINT PRIMARY KEY,
    account_id BIGINT REFERENCES consumer_banking.deposit_account(account_id),
    transaction_type VARCHAR(30),
    amount NUMERIC(18,2),
    currency_code VARCHAR(3),
    transaction_timestamp TIMESTAMP,
    transaction_status VARCHAR(20),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE consumer_banking.credit_card_account (
    card_account_id BIGINT PRIMARY KEY,
    customer_id BIGINT REFERENCES consumer_banking.customer(customer_id),
    credit_limit NUMERIC(18,2),
    outstanding_balance NUMERIC(18,2),
    currency_code VARCHAR(3),
    card_status VARCHAR(20),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE consumer_banking.card_transaction (
    card_transaction_id BIGINT PRIMARY KEY,
    card_account_id BIGINT REFERENCES consumer_banking.credit_card_account(card_account_id),
    merchant_name VARCHAR(150),
    transaction_amount NUMERIC(18,2),
    transaction_timestamp TIMESTAMP,
    transaction_status VARCHAR(20),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE consumer_banking.loan_account (
    loan_id BIGINT PRIMARY KEY,
    customer_id BIGINT REFERENCES consumer_banking.customer(customer_id),
    loan_type VARCHAR(30),
    principal_amount NUMERIC(18,2),
    outstanding_principal NUMERIC(18,2),
    interest_rate NUMERIC(8,4),
    loan_status VARCHAR(20),
    origination_date DATE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE consumer_banking.loan_payment (
    payment_id BIGINT PRIMARY KEY,
    loan_id BIGINT REFERENCES consumer_banking.loan_account(loan_id),
    payment_amount NUMERIC(18,2),
    payment_date DATE,
    payment_status VARCHAR(20),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE consumer_banking.atm_terminal (
    atm_id BIGINT PRIMARY KEY,
    branch_id INT REFERENCES reference_data.branch(branch_id),
    location_name VARCHAR(150),
    country_code VARCHAR(2),
    terminal_status VARCHAR(20)
);

CREATE TABLE consumer_banking.atm_transaction (
    atm_transaction_id BIGINT PRIMARY KEY,
    atm_id BIGINT REFERENCES consumer_banking.atm_terminal(atm_id),
    account_id BIGINT REFERENCES consumer_banking.deposit_account(account_id),
    transaction_type VARCHAR(30),
    amount NUMERIC(18,2),
    transaction_timestamp TIMESTAMP,
    transaction_status VARCHAR(20),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE wholesale_banking.corporate_customer (
    corporate_customer_id BIGINT PRIMARY KEY,
    company_name VARCHAR(200),
    industry VARCHAR(100),
    country_code VARCHAR(2),
    risk_rating VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE wholesale_banking.corporate_account (
    corporate_account_id BIGINT PRIMARY KEY,
    corporate_customer_id BIGINT REFERENCES wholesale_banking.corporate_customer(corporate_customer_id),
    account_type VARCHAR(30),
    currency_code VARCHAR(3),
    current_balance NUMERIC(18,2),
    account_status VARCHAR(20),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE wholesale_banking.credit_facility (
    facility_id BIGINT PRIMARY KEY,
    corporate_customer_id BIGINT REFERENCES wholesale_banking.corporate_customer(corporate_customer_id),
    facility_type VARCHAR(30),
    approved_limit NUMERIC(18,2),
    utilised_amount NUMERIC(18,2),
    currency_code VARCHAR(3),
    facility_status VARCHAR(20),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE integration_audit.ingestion_log (
    ingestion_id BIGSERIAL PRIMARY KEY,
    source_name VARCHAR(100),
    entity_name VARCHAR(100),
    ingestion_type VARCHAR(30),
    records_received BIGINT,
    records_loaded BIGINT,
    ingestion_status VARCHAR(20),
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);

INSERT INTO reference_data.currency VALUES
('MYR', 'Malaysian Ringgit'),
('USD', 'US Dollar'),
('BDT', 'Bangladeshi Taka'),
('SGD', 'Singapore Dollar'),
('EUR', 'Euro');

INSERT INTO reference_data.branch (branch_name, country_code) VALUES
('Kuala Lumpur Main Branch', 'MY'),
('Penang Branch', 'MY'),
('Singapore Branch', 'SG'),
('Dhaka Branch', 'BD');
