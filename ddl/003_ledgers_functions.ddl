------------------------
--  Ledger Functions  --
------------------------


CREATE OR REPLACE FUNCTION evently.create_ledger(name TEXT, description TEXT)
    RETURNS TEXT
    LANGUAGE plpgsql
    VOLATILE AS $$
DECLARE
    event           CONSTANT    TEXT        = '📒𒃻'; -- "Ledger" emoji, "Establish" Sumerian
    meta            CONSTANT    TEXT        = jsonb_build_object('notes', 'Ledger established by Evently');
    data            CONSTANT    TEXT        = jsonb_build_object('description', description, 'name', name);
    created         CONSTANT    TIMESTAMPTZ = clock_timestamp();
    ts              CONSTANT    BIGINT      = extract(epoch FROM created) * 1000000;  -- microseconds
    ts_hex          CONSTANT    TEXT        = evently._padded_hex(ts, 16);
    entities        CONSTANT    TEXT        = '{}';
    ledger_id       CONSTANT    BIGINT      = evently._calc_crc32c(0, concat(event, entities, meta, data, ts_hex)) & 0xfffffffc;
    ledger_hex      CONSTANT    TEXT        = evently._padded_hex(ledger_id, 8);
    prev_event_id   CONSTANT    TEXT        = evently._calc_crc32c(0, concat(ts_hex, evently._padded_hex(0, 8), ledger_hex));
    checksum        CONSTANT    BIGINT      = evently._calc_crc32c(0, concat(event, entities, meta, data, ts_hex, ledger_hex, prev_event_id));
    append_key      CONSTANT    TEXT        = gen_random_uuid()::TEXT;
    ledger_table    CONSTANT    TEXT        = evently._ledger_table(ledger_hex);
BEGIN
    -- insert here first to see if any validations are triggered
    INSERT INTO evently.ledgers (id, created, name, description)
    VALUES (ledger_hex, created, name, description);

    -- Create a ledger table with inheritance
    EXECUTE format('CREATE TABLE %I (LIKE evently.ledger_base
                INCLUDING CONSTRAINTS
                INCLUDING INDEXES)
                INHERITS (evently.ledger_base)',
           ledger_table);

    EXECUTE format('INSERT INTO %I (timestamp, previous_ts, checksum, append_key, event, entities, meta, data)
                VALUES ($1, $2, $3, $4, $5, $6::JSONB, $7::JSONB, $8::JSONB)',
           ledger_table)
    USING ts, 0, checksum, append_key, event, entities, meta, data;

    -- Revoke does not inherit
    EXECUTE format('REVOKE UPDATE ON %I FROM public, evently', ledger_table);
    EXECUTE format('REVOKE TRUNCATE ON %I FROM public, evently', ledger_table);

    -- Add triggers. Can't inherit them, need to recreate them for the ledger.
    EXECUTE format('CREATE TRIGGER trigger_publish_new_event_%s
                AFTER INSERT ON %I
                FOR EACH ROW EXECUTE PROCEDURE evently.publish_new_event(%L)',
            ledger_table, ledger_table, ledger_hex);

    RETURN ledger_hex;
END
$$;


CREATE OR REPLACE FUNCTION evently.ledger_event_count(ledger_id_in TEXT)
    RETURNS BIGINT
    LANGUAGE plpgsql
    STABLE AS $$
DECLARE
    count BIGINT;
BEGIN
    EXECUTE format('SELECT count(*) FROM %I',
               evently._ledger_table(ledger_id_in))
        INTO count;
    RETURN count;
END
$$;


CREATE OR REPLACE FUNCTION evently.reset_ledger_events(ledger_id_in TEXT,
                                                       after_in     BIGINT)
    RETURNS VOID
    LANGUAGE plpgsql
    AS $$
BEGIN
    EXECUTE format('DELETE FROM %I WHERE timestamp > $1',
               evently._ledger_table(ledger_id_in))
    USING after_in;
END
$$;


CREATE OR REPLACE FUNCTION evently.remove_ledger(ledger_id_in TEXT)
    RETURNS VOID
    LANGUAGE plpgsql
    AS $$
BEGIN
    EXECUTE 'DELETE FROM evently.ledgers WHERE id = $1'
        USING ledger_id_in;
    EXECUTE format('DROP TABLE %I', evently._ledger_table(ledger_id_in));
END
$$;
