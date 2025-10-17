SET plpgsql.extra_warnings TO 'all';
SET plpgsql.extra_errors TO 'all';

CREATE SCHEMA IF NOT EXISTS evently;

DROP TYPE IF EXISTS evently.event_id CASCADE;

CREATE TYPE evently.event_id AS
(
    timestamp   BIGINT,
    checksum    BIGINT,
    ledger_id   TEXT
);


DROP TYPE IF EXISTS evently.event CASCADE;

CREATE TYPE evently.event AS
(
    timestamp   BIGINT,
    checksum    BIGINT,
    event       TEXT,
    entities    JSONB,
    meta        JSONB,
    data        JSONB
)
