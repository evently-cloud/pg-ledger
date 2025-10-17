CREATE OR REPLACE FUNCTION evently.publish_new_event()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    VOLATILE AS $$
DECLARE
    -- Trigger functions cannot have declared arguments
    ledger_id_in    TEXT = TG_ARGV[0];
    acc             TEXT;
    response        TEXT;
BEGIN
    acc = format('%s,%s,%s,%L,%L',
            ledger_id_in,
            NEW.timestamp,
            NEW.checksum,
            NEW.event,
            NEW.entities);
    response = acc;
    acc = format('%s,%L', acc, NEW.meta);
    -- docs say '< 8000 bytes` so add and check.
    IF (octet_length(acc) < 8000) THEN
        response = acc;
        acc = format('%s,%L', acc, NEW.data);
        IF (octet_length(acc) < 8000) THEN
            response = acc;
        END IF;
    END IF;
    PERFORM pg_notify('ALL_EVENTS', response);
    RETURN NEW;
END
$$;



CREATE OR REPLACE FUNCTION evently.fetch_missing_data(ledger_id_in  TEXT,
                                                      timestamp_in  BIGINT,
                                                      need_meta     BOOLEAN)
    RETURNS TABLE(meta JSONB, data JSONB)
    LANGUAGE plpgsql
    STABLE AS $$
DECLARE
    ledger_table    CONSTANT    TEXT    = evently._ledger_table(ledger_id_in);
    columns                     TEXT    = 'data';
BEGIN
    -- if we are here, then we need data for sure. Just add meta if required
    IF (need_meta) THEN
        columns = 'meta, ' || columns;
    ELSE
        columns = 'NULL::JSONB AS meta, ' || columns;
    END IF;
   RETURN QUERY
        EXECUTE format('SELECT %s FROM %I WHERE timestamp = $1',
                   columns, ledger_table)
        USING timestamp_in;
END
$$
