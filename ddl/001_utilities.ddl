-----------------------
-- Utility functions --
-----------------------


CREATE OR REPLACE FUNCTION evently._padded_hex(BIGINT, INT)
    RETURNS TEXT
    LANGUAGE sql
    IMMUTABLE PARALLEL SAFE
BEGIN ATOMIC;
    SELECT lpad(to_hex($1), $2, '0');
END;



CREATE OR REPLACE FUNCTION evently._ledger_table(TEXT)
    RETURNS TEXT
    LANGUAGE sql
    IMMUTABLE PARALLEL SAFE
BEGIN ATOMIC;
    SELECT 'ledger_' || $1;
END;


-- create LIMIT stanza if $1 is > 0. Use $2 to gate the limit between
-- what limit is sent as (could be very large) and app's batch size ($2). If null, use max INT
-- which will be the largest $1 can be too.
CREATE OR REPLACE FUNCTION evently._limit_query(INT, INT)
    RETURNS TEXT
    LANGUAGE sql
    IMMUTABLE PARALLEL SAFE
BEGIN ATOMIC;
    SELECT CASE
           WHEN $1 > 0
               THEN ' LIMIT ' || least($1, coalesce($2, 2147483647))   -- max INT value
           ELSE ''
    END;
END;


CREATE OR REPLACE FUNCTION evently._parse_event_id(UUID)
    RETURNS evently.event_id
    LANGUAGE sql
    IMMUTABLE PARALLEL SAFE
BEGIN ATOMIC;
    WITH id AS (SELECT replace($1::TEXT, '-', '') AS t)
    SELECT
        (('x' || substring(t, 1, 16))::BIT(64)::BIGINT,          -- timestamp
         ('x00000000' || substring(t, 17, 8))::BIT(64)::BIGINT,  -- checksum
         substring(t, 25))                                       -- ledger id
    FROM id;
END;


CREATE OR REPLACE FUNCTION evently._sorted_json(json_in JSONB)
    RETURNS TEXT
    LANGUAGE plpgsql
    IMMUTABLE PARALLEL SAFE AS $$
DECLARE
    acc     TEXT;
    value   TEXT;
BEGIN
    CASE
        WHEN json_in IS JSON ARRAY THEN
            acc = '[';
            FOR value IN SELECT jsonb_array_elements(json_in)
            LOOP
                acc = acc || evently._sorted_json(value::JSONB) || ',';
            END LOOP;
            acc = trim(TRAILING ',' FROM acc) || ']';
        WHEN json_in IS JSON OBJECT THEN
            acc = '{';
            -- sort by unicode code point
            FOR value IN SELECT jsonb_object_keys(json_in) COLLATE ucs_basic ORDER BY 1
            LOOP
                acc = acc || to_json(value) || ':' || evently._sorted_json(json_in->value) || ',';
            END LOOP;
            acc = trim(TRAILING ',' FROM acc) || '}';
        ELSE
            acc = json_in::TEXT;
    END CASE;
    RETURN acc;
END
$$;


CREATE OR REPLACE FUNCTION evently._crc32c(TEXT)
    RETURNS BIGINT
    LANGUAGE sql
    IMMUTABLE PARALLEL SAFE
BEGIN ATOMIC;
    SELECT crc32c(convert_to($1, 'UTF8'));
END;
