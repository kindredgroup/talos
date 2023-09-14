CREATE TABLE IF NOT EXISTS bank_accounts (
	"number" VARCHAR(20) NOT NULL,
	"name" VARCHAR(20) NOT NULL,
	amount NUMERIC(20, 5) NOT NULL,
	"version" BIGINT NOT NULL,
	CONSTRAINT bank_accounts_pkey PRIMARY KEY (NUMBER)
);

CREATE TABLE IF NOT EXISTS cohort_snapshot (
    "id"        CHAR(9) PRIMARY KEY,       -- The value is always 'SINGLETON'
	"version"   BIGINT NOT NULL
);
