CREATE TABLE IF NOT EXISTS bank_accounts (
    "number"    VARCHAR(255) PRIMARY KEY,
    "data"      JSONB NOT NULL
);

CREATE INDEX bank_accounts_number ON bank_accounts((data->>'number'));

CREATE TABLE IF NOT EXISTS cohort_snapshot (
    "id"        CHAR(9) PRIMARY KEY,       -- The value is always 'SINGLETON'
	"version"   BIGINT NOT NULL
);
