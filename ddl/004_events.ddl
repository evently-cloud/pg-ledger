-----------------------
--  Event Functions  --
-----------------------


CREATE OR REPLACE FUNCTION evently.fetch_event_id(ledger_id_in TEXT,
                                                  selector_in  BYTEA,
                                                  after_in     BIGINT,
                                                  limit_in     INT)
    RETURNS SETOF evently.event_id
    LANGUAGE plpgsql
    STABLE AS $$
DECLARE
    limit_stmt      CONSTANT    TEXT = evently._limit_query(limit_in, null);
    selector_stmt               TEXT = convert_from(selector_in, 'UTF8');

BEGIN
    IF (after_in IS NOT NULL) THEN
        selector_stmt = format('%s AND timestamp > %s', selector_stmt, after_in);
    END IF;

    RETURN QUERY
    EXECUTE format('SELECT timestamp, checksum, %L AS ledger_id
             FROM (SELECT timestamp, checksum FROM %I WHERE %s
             ORDER BY timestamp%s) AS t ORDER BY timestamp DESC LIMIT 1',
           ledger_id_in,
           evently._ledger_table(ledger_id_in),
           selector_stmt,
           limit_stmt);
END
$$;


-- Verifies that selector has no new matching events.
CREATE OR REPLACE FUNCTION evently.selector_quiescent(ledger_id_in TEXT,
                                                      timestamp_in BIGINT,
                                                      selector_in  TEXT)
    RETURNS BOOLEAN
    LANGUAGE plpgsql
    STABLE AS $$
DECLARE
    has_events  BOOLEAN;
BEGIN
    EXECUTE format('SELECT true FROM %I WHERE timestamp > $1 AND %s LIMIT 1',
                   evently._ledger_table(ledger_id_in),
                   selector_in)
    USING timestamp_in
    INTO has_events;

    RETURN has_events IS NULL;
END
$$;


CREATE OR REPLACE FUNCTION evently.after_exists(event_id_in evently.event_id)
    RETURNS VOID
    LANGUAGE plpgsql
    STABLE AS $$
DECLARE
    ledger_table    CONSTANT    TEXT = evently._ledger_table(event_id_in.ledger_id);
    exists                      BOOLEAN;
BEGIN
    -- append fact will just have ledger_id in the event_id, so skip check.
    IF (event_id_in.timestamp = 0) THEN
        RETURN;
    END IF;

    EXECUTE format('SELECT true FROM %I WHERE timestamp = $1 AND checksum = $2',
                   ledger_table)
    USING event_id_in.timestamp, event_id_in.checksum
    INTO exists;

    IF exists IS NULL THEN
        RAISE EXCEPTION 'AFTER not found: {ledger: %, ledger_id: %, timestamp: %, checksum: %}',
            ledger_table, event_id_in.ledger_id, event_id_in.timestamp, event_id_in.checksum;
    END IF;
END
$$;


CREATE OR REPLACE FUNCTION evently.append_event(previous_eid_in UUID,
                                                event_in        TEXT,
                                                entities_in     JSONB,
                                                meta_in         JSONB,
                                                data_in         JSONB,
                                                append_key_in   TEXT,
                                                selector_in     BYTEA)
    RETURNS TEXT
    LANGUAGE plpgsql
    VOLATILE AS $$
    DECLARE
        previous_event_id   CONSTANT    evently.event_id    = evently._parse_event_id(previous_eid_in);
        ledger_table        CONSTANT    TEXT                = evently._ledger_table(previous_event_id.ledger_id);

        previous_ts                     BIGINT;
        previous_checksum               BIGINT;
        mono_ts                         BIGINT;
        mono_ts_hex                     TEXT;
        checksum                        BIGINT;
        previous_id_hex                 TEXT;
        inserted                        BIGINT;
BEGIN
    ASSERT length(selector_in) > 0, 'selector cannot be empty';
    PERFORM evently.after_exists(previous_event_id);

    LOOP
        BEGIN
            -- select latest ledger timestamp & checksum to make previous_id
            EXECUTE format('SELECT timestamp, checksum FROM %I ORDER BY timestamp DESC LIMIT 1',
                           ledger_table)
            INTO previous_ts, previous_checksum;

            -- create monotonic timestamp (or clock_timestamp)
            mono_ts = greatest(
                    1 + previous_ts,
                    extract(epoch FROM clock_timestamp()) * 1000000);

            mono_ts_hex = evently._padded_hex(mono_ts, 16);

            -- create the previous_id
            previous_id_hex = concat(evently._padded_hex(previous_ts, 16),
                                     evently._padded_hex(previous_checksum, 8),
                                     previous_event_id.ledger_id);

            -- calc checksum
            checksum = evently._calc_crc32c(0, concat(
                    event_in,
                    evently._sorted_json(entities_in),
                    evently._sorted_json(meta_in),
                    evently._sorted_json(data_in),
                    mono_ts_hex,
                    previous_event_id.ledger_id,
                    previous_id_hex));

            EXECUTE format('INSERT INTO %I (timestamp, previous_ts, checksum, append_key, event, entities, meta, data)
                     SELECT $1, $2, $3, $4, $5, $6, $7, $8
                     WHERE evently.selector_quiescent($9, $10, $11) IS TRUE',
                   ledger_table)
            USING
                mono_ts,
                previous_ts,
                checksum,
                append_key_in,
                event_in,
                entities_in,
                meta_in,
                data_in,
                previous_event_id.ledger_id,
                previous_event_id.timestamp,
                convert_from(selector_in, 'UTF8');

            -- Cannot use FOUND with EXECUTE: https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-DIAGNOSTICS
            GET DIAGNOSTICS inserted = ROW_COUNT;

            IF inserted > 0 THEN
                RETURN concat(mono_ts_hex, evently._padded_hex(checksum, 8), previous_event_id.ledger_id);
            END IF;

            -- nothing inserted because events exist after the mark in the selector (not quiescent); no Exception raised
            RAISE EXCEPTION 'RACE CONDITION after mark';
        EXCEPTION
            -- Adding an Exception handler slows down the function by 35%, roughly:
            -- https://devently.to/serpent7776/the-performance-cost-of-plpgsql-exception-block-in-postgres-4h22
            -- However, this is cheaper and less frustrating than sending the
            -- race condition exception back to the client for them to resubmit.
            WHEN unique_violation THEN
                -- _pkey is the timestamp column.
                IF (SQLERRM LIKE '%_previous_ts_key%' OR SQLERRM LIKE '%_pkey%') THEN
                    CONTINUE; -- INSERT race condition due to a different append(), try again
                END IF;
                RAISE; -- re-raise the original exception
        END;
    END LOOP;
END
$$;



CREATE OR REPLACE FUNCTION evently.find_with_append_key(ledger_id_in    TEXT,
                                                        append_key_in   TEXT)
    RETURNS SETOF evently.event
    LANGUAGE plpgsql
    STABLE AS $$
BEGIN
    RETURN QUERY
    EXECUTE format('SELECT timestamp, checksum, event, entities, meta, data FROM %I WHERE append_key = $1',
                   evently._ledger_table(ledger_id_in))
    USING append_key_in;
END
$$;



CREATE OR REPLACE FUNCTION evently.run_selector(ledger_id_in        TEXT,
                                                after_ts_in         BIGINT,
                                                after_checksum_in   BIGINT,
                                                limit_in            INT,
                                                selector_in         BYTEA,
                                                max_rows            INT)
    RETURNS SETOF evently.event
    LANGUAGE plpgsql
    STABLE AS $$
DECLARE
    ledger_table        CONSTANT    TEXT    = evently._ledger_table(ledger_id_in);
    -- Can't send selector statement as TEXT because drivers escape the SQL. Send as byte array and convert to TEXT to preserve content.
    selector_stmt                   TEXT    = convert_from(selector_in, 'UTF8');
    limit_stmt                      TEXT    = evently._limit_query(limit_in, max_rows);
    latest_timestamp                BIGINT;
    latest_checksum                 BIGINT;
    stmt                            TEXT;
BEGIN
    IF (after_ts_in > 0) THEN
        PERFORM evently.after_exists((after_ts_in, after_checksum_in, ledger_id_in));
        IF (length(selector_stmt) > 0) THEN
            selector_stmt = selector_stmt || ' AND ';
        END IF;
        selector_stmt = selector_stmt || 'timestamp > ' || after_ts_in;
    END IF;

    -- find the latest timestamp and checksum for this selector
    stmt = format('SELECT timestamp, checksum FROM (SELECT timestamp, checksum FROM %I
                WHERE %s ORDER BY TIMESTAMP%s) AS t ORDER BY timestamp DESC LIMIT 1',
           ledger_table,
           selector_stmt,
           limit_stmt);
--     RAISE INFO 'find lastest timestamp statement: %', stmt;
    EXECUTE stmt
        INTO latest_timestamp, latest_checksum;
--     RAISE INFO 'lastest_timestamp: %, checksum: %', latest_timestamp, latest_checksum;

    IF (latest_timestamp IS NOT NULL) THEN
        -- first row is the timestamp to use to create the last event id; do not return if no results
        RETURN NEXT (
                 latest_timestamp::BIGINT,
                 latest_checksum::BIGINT,
                 NULL::TEXT,
                 NULL::JSONB,
                 NULL::JSONB,
                 NULL::JSONB);

        stmt = format('SELECT timestamp, checksum, event, entities, meta, data FROM %I
                   WHERE timestamp <= $1 AND %s ORDER BY timestamp%s',
                ledger_table,
                selector_stmt,
                limit_stmt);
--         RAISE INFO 'full selector statement: %', stmt;
        RETURN QUERY EXECUTE stmt
            USING latest_timestamp;
    END IF;
END
$$;


CREATE OR REPLACE FUNCTION evently.fetch_selected(ledger_id_in  TEXT,
                                                  after_in      BIGINT,
                                                  limit_in      INT,
                                                  selector_in   BYTEA)
RETURNS SETOF evently.event
LANGUAGE plpgsql
STABLE AS $$
BEGIN
    RETURN QUERY
        EXECUTE format('SELECT timestamp, checksum, event, entities, meta, data
                 FROM %I WHERE timestamp > $1 AND %s
                 ORDER BY timestamp LIMIT $2',
               evently._ledger_table(ledger_id_in),
               convert_from(selector_in, 'UTF8'))
        USING after_in, limit_in;
END
$$;
