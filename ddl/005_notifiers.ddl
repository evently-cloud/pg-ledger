-- Payload format (<US> is char 31, unit separator):
-- ledger_id <US> timestamp <US> checksum <US> %L(event) <US> %L(entities) [<US> %L(meta)] [<US> %L(data)]
CREATE OR REPLACE FUNCTION evently.publish_new_event()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    VOLATILE AS $$
DECLARE
    -- docs say '< 8000 bytes`.
    PAYLOAD_LIMIT   CONSTANT    INT := 7999;
    -- Trigger functions cannot have declared arguments
    ledger_id_in                TEXT := TG_ARGV[0];
    acc                         TEXT;
    response                    TEXT;
BEGIN
    acc := format(E'%s\t%s\t%s\t%L\t%L',
            ledger_id_in,
            NEW.timestamp,
            NEW.checksum,
            NEW.event,
            NEW.entities);
    response := acc;
    acc := format(E'%s\t%L', acc, NEW.meta);
    IF (octet_length(acc) < PAYLOAD_LIMIT) THEN
        response := acc;
        acc := format(E'%s\t%L', acc, NEW.data);
        IF (octet_length(acc) < PAYLOAD_LIMIT) THEN
            response := acc;
        END IF;
    END IF;
    PERFORM pg_notify('ALL_EVENTS', response);
    RETURN NEW;
END;
$$;


CREATE OR REPLACE FUNCTION evently.fetch_missing_data(ledger_id_in  TEXT,
                                                      timestamp_in  BIGINT,
                                                      need_meta     BOOLEAN)
    RETURNS TABLE(meta JSONB, data JSONB)
    LANGUAGE plpgsql
    STABLE AS $$
DECLARE
    ledger_table    CONSTANT    TEXT    := evently._ledger_table(ledger_id_in);
    columns                     TEXT    := 'data';
BEGIN
    -- if we are here, then we need data for sure. Just add meta if required
    IF (need_meta) THEN
        columns := 'meta, ' || columns;
    ELSE
        columns := 'NULL::JSONB AS meta, ' || columns;
    END IF;
   RETURN QUERY
        EXECUTE format('SELECT %s FROM %I WHERE timestamp = $1',
                   columns, ledger_table)
        USING timestamp_in;
END;
$$;
