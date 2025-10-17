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



-- crc32c implementation
-- Table-based crc32 (not exactly right algorithm, but a good pattern to follow):
-- https://gist.github.com/cuber/bcf0a3a96fc9a790d96d
-- Google-created CRC32c table to run in the algorithm above:
-- https://github.com/googlearchive/crc32c-java/blob/master/src/com/google/cloud/Crc32c.java
CREATE TABLE IF NOT EXISTS evently.crc32c_lookup (
    index   INT     PRIMARY KEY,
    value   BIGINT  NOT NULL
);

TRUNCATE evently.crc32c_lookup;
INSERT INTO evently.crc32c_lookup (index, value) VALUES
    (0,  x'00000000'::BIGINT), (1,  x'f26b8303'::BIGINT), (2,  x'e13b70f7'::BIGINT), (3,  x'1350f3f4'::BIGINT),
    (4,  x'c79a971f'::BIGINT), (5,  x'35f1141c'::BIGINT), (6,  x'26a1e7e8'::BIGINT), (7,  x'd4ca64eb'::BIGINT),
    (8,  x'8ad958cf'::BIGINT), (9,  x'78b2dbcc'::BIGINT), (10, x'6be22838'::BIGINT), (11, x'9989ab3b'::BIGINT),
    (12, x'4d43cfd0'::BIGINT), (13, x'bf284cd3'::BIGINT), (14, x'ac78bf27'::BIGINT), (15, x'5e133c24'::BIGINT),
    (16, x'105ec76f'::BIGINT), (17, x'e235446c'::BIGINT), (18, x'f165b798'::BIGINT), (19, x'030e349b'::BIGINT),
    (20, x'd7c45070'::BIGINT), (21, x'25afd373'::BIGINT), (22, x'36ff2087'::BIGINT), (23, x'c494a384'::BIGINT),
    (24, x'9a879fa0'::BIGINT), (25, x'68ec1ca3'::BIGINT), (26, x'7bbcef57'::BIGINT), (27, x'89d76c54'::BIGINT),
    (28, x'5d1d08bf'::BIGINT), (29, x'af768bbc'::BIGINT), (30, x'bc267848'::BIGINT), (31, x'4e4dfb4b'::BIGINT),
    (32, x'20bd8ede'::BIGINT), (33, x'd2d60ddd'::BIGINT), (34, x'c186fe29'::BIGINT), (35, x'33ed7d2a'::BIGINT),
    (36, x'e72719c1'::BIGINT), (37, x'154c9ac2'::BIGINT), (38, x'061c6936'::BIGINT), (39, x'f477ea35'::BIGINT),
    (40, x'aa64d611'::BIGINT), (41, x'580f5512'::BIGINT), (42, x'4b5fa6e6'::BIGINT), (43, x'b93425e5'::BIGINT),
    (44, x'6dfe410e'::BIGINT), (45, x'9f95c20d'::BIGINT), (46, x'8cc531f9'::BIGINT), (47, x'7eaeb2fa'::BIGINT),
    (48, x'30e349b1'::BIGINT), (49, x'c288cab2'::BIGINT), (50, x'd1d83946'::BIGINT), (51, x'23b3ba45'::BIGINT),
    (52, x'f779deae'::BIGINT), (53, x'05125dad'::BIGINT), (54, x'1642ae59'::BIGINT), (55, x'e4292d5a'::BIGINT),
    (56, x'ba3a117e'::BIGINT), (57, x'4851927d'::BIGINT), (58, x'5b016189'::BIGINT), (59, x'a96ae28a'::BIGINT),
    (60, x'7da08661'::BIGINT), (61, x'8fcb0562'::BIGINT), (62, x'9c9bf696'::BIGINT), (63, x'6ef07595'::BIGINT),
    (64, x'417b1dbc'::BIGINT), (65, x'b3109ebf'::BIGINT), (66, x'a0406d4b'::BIGINT), (67, x'522bee48'::BIGINT),
    (68, x'86e18aa3'::BIGINT), (69, x'748a09a0'::BIGINT), (70, x'67dafa54'::BIGINT), (71, x'95b17957'::BIGINT),
    (72, x'cba24573'::BIGINT), (73, x'39c9c670'::BIGINT), (74, x'2a993584'::BIGINT), (75, x'd8f2b687'::BIGINT),
    (76, x'0c38d26c'::BIGINT), (77, x'fe53516f'::BIGINT), (78, x'ed03a29b'::BIGINT), (79, x'1f682198'::BIGINT),
    (80, x'5125dad3'::BIGINT), (81, x'a34e59d0'::BIGINT), (82, x'b01eaa24'::BIGINT), (83, x'42752927'::BIGINT),
    (84, x'96bf4dcc'::BIGINT), (85, x'64d4cecf'::BIGINT), (86, x'77843d3b'::BIGINT), (87, x'85efbe38'::BIGINT),
    (88, x'dbfc821c'::BIGINT), (89, x'2997011f'::BIGINT), (90, x'3ac7f2eb'::BIGINT), (91, x'c8ac71e8'::BIGINT),
    (92, x'1c661503'::BIGINT), (93, x'ee0d9600'::BIGINT), (94, x'fd5d65f4'::BIGINT), (95, x'0f36e6f7'::BIGINT),
    (96, x'61c69362'::BIGINT), (97, x'93ad1061'::BIGINT), (98, x'80fde395'::BIGINT), (99, x'72966096'::BIGINT),
    (100, x'a65c047d'::BIGINT), (101, x'5437877e'::BIGINT), (102, x'4767748a'::BIGINT), (103, x'b50cf789'::BIGINT),
    (104, x'eb1fcbad'::BIGINT), (105, x'197448ae'::BIGINT), (106, x'0a24bb5a'::BIGINT), (107, x'f84f3859'::BIGINT),
    (108, x'2c855cb2'::BIGINT), (109, x'deeedfb1'::BIGINT), (110, x'cdbe2c45'::BIGINT), (111, x'3fd5af46'::BIGINT),
    (112, x'7198540d'::BIGINT), (113, x'83f3d70e'::BIGINT), (114, x'90a324fa'::BIGINT), (115, x'62c8a7f9'::BIGINT),
    (116, x'b602c312'::BIGINT), (117, x'44694011'::BIGINT), (118, x'5739b3e5'::BIGINT), (119, x'a55230e6'::BIGINT),
    (120, x'fb410cc2'::BIGINT), (121, x'092a8fc1'::BIGINT), (122, x'1a7a7c35'::BIGINT), (123, x'e811ff36'::BIGINT),
    (124, x'3cdb9bdd'::BIGINT), (125, x'ceb018de'::BIGINT), (126, x'dde0eb2a'::BIGINT), (127, x'2f8b6829'::BIGINT),
    (128, x'82f63b78'::BIGINT), (129, x'709db87b'::BIGINT), (130, x'63cd4b8f'::BIGINT), (131, x'91a6c88c'::BIGINT),
    (132, x'456cac67'::BIGINT), (133, x'b7072f64'::BIGINT), (134, x'a457dc90'::BIGINT), (135, x'563c5f93'::BIGINT),
    (136, x'082f63b7'::BIGINT), (137, x'fa44e0b4'::BIGINT), (138, x'e9141340'::BIGINT), (139, x'1b7f9043'::BIGINT),
    (140, x'cfb5f4a8'::BIGINT), (141, x'3dde77ab'::BIGINT), (142, x'2e8e845f'::BIGINT), (143, x'dce5075c'::BIGINT),
    (144, x'92a8fc17'::BIGINT), (145, x'60c37f14'::BIGINT), (146, x'73938ce0'::BIGINT), (147, x'81f80fe3'::BIGINT),
    (148, x'55326b08'::BIGINT), (149, x'a759e80b'::BIGINT), (150, x'b4091bff'::BIGINT), (151, x'466298fc'::BIGINT),
    (152, x'1871a4d8'::BIGINT), (153, x'ea1a27db'::BIGINT), (154, x'f94ad42f'::BIGINT), (155, x'0b21572c'::BIGINT),
    (156, x'dfeb33c7'::BIGINT), (157, x'2d80b0c4'::BIGINT), (158, x'3ed04330'::BIGINT), (159, x'ccbbc033'::BIGINT),
    (160, x'a24bb5a6'::BIGINT), (161, x'502036a5'::BIGINT), (162, x'4370c551'::BIGINT), (163, x'b11b4652'::BIGINT),
    (164, x'65d122b9'::BIGINT), (165, x'97baa1ba'::BIGINT), (166, x'84ea524e'::BIGINT), (167, x'7681d14d'::BIGINT),
    (168, x'2892ed69'::BIGINT), (169, x'daf96e6a'::BIGINT), (170, x'c9a99d9e'::BIGINT), (171, x'3bc21e9d'::BIGINT),
    (172, x'ef087a76'::BIGINT), (173, x'1d63f975'::BIGINT), (174, x'0e330a81'::BIGINT), (175, x'fc588982'::BIGINT),
    (176, x'b21572c9'::BIGINT), (177, x'407ef1ca'::BIGINT), (178, x'532e023e'::BIGINT), (179, x'a145813d'::BIGINT),
    (180, x'758fe5d6'::BIGINT), (181, x'87e466d5'::BIGINT), (182, x'94b49521'::BIGINT), (183, x'66df1622'::BIGINT),
    (184, x'38cc2a06'::BIGINT), (185, x'caa7a905'::BIGINT), (186, x'd9f75af1'::BIGINT), (187, x'2b9cd9f2'::BIGINT),
    (188, x'ff56bd19'::BIGINT), (189, x'0d3d3e1a'::BIGINT), (190, x'1e6dcdee'::BIGINT), (191, x'ec064eed'::BIGINT),
    (192, x'c38d26c4'::BIGINT), (193, x'31e6a5c7'::BIGINT), (194, x'22b65633'::BIGINT), (195, x'd0ddd530'::BIGINT),
    (196, x'0417b1db'::BIGINT), (197, x'f67c32d8'::BIGINT), (198, x'e52cc12c'::BIGINT), (199, x'1747422f'::BIGINT),
    (200, x'49547e0b'::BIGINT), (201, x'bb3ffd08'::BIGINT), (202, x'a86f0efc'::BIGINT), (203, x'5a048dff'::BIGINT),
    (204, x'8ecee914'::BIGINT), (205, x'7ca56a17'::BIGINT), (206, x'6ff599e3'::BIGINT), (207, x'9d9e1ae0'::BIGINT),
    (208, x'd3d3e1ab'::BIGINT), (209, x'21b862a8'::BIGINT), (210, x'32e8915c'::BIGINT), (211, x'c083125f'::BIGINT),
    (212, x'144976b4'::BIGINT), (213, x'e622f5b7'::BIGINT), (214, x'f5720643'::BIGINT), (215, x'07198540'::BIGINT),
    (216, x'590ab964'::BIGINT), (217, x'ab613a67'::BIGINT), (218, x'b831c993'::BIGINT), (219, x'4a5a4a90'::BIGINT),
    (220, x'9e902e7b'::BIGINT), (221, x'6cfbad78'::BIGINT), (222, x'7fab5e8c'::BIGINT), (223, x'8dc0dd8f'::BIGINT),
    (224, x'e330a81a'::BIGINT), (225, x'115b2b19'::BIGINT), (226, x'020bd8ed'::BIGINT), (227, x'f0605bee'::BIGINT),
    (228, x'24aa3f05'::BIGINT), (229, x'd6c1bc06'::BIGINT), (230, x'c5914ff2'::BIGINT), (231, x'37faccf1'::BIGINT),
    (232, x'69e9f0d5'::BIGINT), (233, x'9b8273d6'::BIGINT), (234, x'88d28022'::BIGINT), (235, x'7ab90321'::BIGINT),
    (236, x'ae7367ca'::BIGINT), (237, x'5c18e4c9'::BIGINT), (238, x'4f48173d'::BIGINT), (239, x'bd23943e'::BIGINT),
    (240, x'f36e6f75'::BIGINT), (241, x'0105ec76'::BIGINT), (242, x'12551f82'::BIGINT), (243, x'e03e9c81'::BIGINT),
    (244, x'34f4f86a'::BIGINT), (245, x'c69f7b69'::BIGINT), (246, x'd5cf889d'::BIGINT), (247, x'27a40b9e'::BIGINT),
    (248, x'79b737ba'::BIGINT), (249, x'8bdcb4b9'::BIGINT), (250, x'988c474d'::BIGINT), (251, x'6ae7c44e'::BIGINT),
    (252, x'be2da0a5'::BIGINT), (253, x'4c4623a6'::BIGINT), (254, x'5f16d052'::BIGINT), (255, x'ad7d5351'::BIGINT);


CREATE OR REPLACE FUNCTION evently._calc_crc32c(crc     BIGINT,
                                                source  TEXT)
    RETURNS BIGINT
    LANGUAGE plpgsql
    IMMUTABLE PARALLEL SAFE AS $$
DECLARE
    bytes       CONSTANT    BYTEA   = convert_to(source, 'UTF8');
    len         CONSTANT    INT     = length(bytes);
    long_mask   CONSTANT    BIGINT  = x'ffffffff'::BIGINT;
    byte_mask   CONSTANT    BIGINT  = x'ff'::BIGINT;

    byte        BIGINT;
    crc_table   BIGINT[];
BEGIN
    SELECT array_agg(value ORDER BY index)
        INTO crc_table FROM evently.crc32c_lookup;
    crc = crc # long_mask;
    FOR i IN 0..len - 1
        LOOP
            byte = get_byte(bytes, i);
            -- Postgres arrays are 1-based
            crc = (crc_table[((crc # byte) & byte_mask) + 1] # (crc >> 8)) & long_mask;
        END LOOP;
    RETURN crc # long_mask;
END
$$;
