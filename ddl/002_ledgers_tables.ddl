DROP TABLE IF EXISTS evently.ledgers CASCADE;

CREATE TABLE evently.ledgers
(
    id          TEXT        PRIMARY KEY,
    created     TIMESTAMPTZ NOT NULL,
    name        TEXT        UNIQUE NOT NULL CHECK(char_length(name) <= 500),
    description TEXT        NOT NULL        CHECK(char_length(description) <= 2000)
);



-- This will drop inherited tables too
DROP TABLE IF EXISTS evently.ledger_base CASCADE;

-- This design has one ledger per table, rather than using a ledger ID as a column in the
-- one table. The good is that each ledger can have its own indexes, and can be moved around.
-- however, it complicates the codebase by requiring the functions to use string math to construct
-- statements. This isn't all that bad, as any query with a selector is already doing this.



CREATE TABLE evently.ledger_base
(
    -- no two events can have the same timestamp. Timestamp is also a sort key.
    timestamp       BIGINT  PRIMARY KEY,
    --- https://blog.rustprooflabs.com/2021/06/postgres-bigint-by-default
    checksum        BIGINT  NOT NULL,
    -- no two events can have the previous timestamp. Serialized append keeps ledger consistent.
    previous_ts     BIGINT  NOT NULL UNIQUE,
    -- check length so apps don't push garbage into the ledger
    -- append key, used for idempotency tests
    append_key      TEXT    NOT NULL UNIQUE CHECK(char_length(append_key) <= 100),
    event           TEXT    NOT NULL        CHECK(char_length(event) <= 500),
    entities        JSONB   NOT NULL, -- key is entity name, value is key(s); pass single value as an array
    meta            JSONB   NOT NULL,
    data            JSONB   NOT NULL
);


-- https://dzone.com/articles/using-jsonb-in-postgresql-how-to-effectively-store
CREATE INDEX ledger_entities_index ON evently.ledger_base USING gin (entities jsonb_path_ops);
CREATE INDEX ledger_meta_index ON evently.ledger_base USING gin (meta jsonb_path_ops);
CREATE INDEX ledger_data_index ON evently.ledger_base USING gin (data jsonb_path_ops);


-- cannot modify the ledger_base table, but can DELETE on the individual ledger tables
REVOKE INSERT ON evently.ledger_base FROM public, evently;
REVOKE UPDATE ON evently.ledger_base FROM public, evently;
REVOKE DELETE ON evently.ledger_base FROM public, evently;
REVOKE TRUNCATE ON evently.ledger_base FROM public, evently;
