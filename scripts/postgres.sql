
DROP TABLE IF EXISTS schema CASCADE;
DROP TABLE IF EXISTS sample CASCADE;

CREATE DATABASE arraytestgis;

CREATE EXTENSION Postgis;

CREATE TABLE schema (
    schema_id serial PRIMARY KEY,
    name varchar(100) NOT NULL,
    fields json NOT NULL
);

CREATE TABLE sample (
    sample_id serial PRIMARY KEY,
    schema_id integer NOT NULL REFERENCES schema(schema_id),
    location geography(point,4326),
    number_vals real [],
    string_vals text []
);

CREATE INDEX schema_id_idx ON sample (schema_id);

CREATE INDEX location_gix ON sample USING GIST (location);


--CREATE TABLE sample (
--    sample_id serial PRIMARY KEY,
--    schema_id integer NOT NULL REFERENCES schema(schema_id)
--);
--
--CREATE TABLE field (
--    field_id serial PRIMARY KEY,
--    schema_id integer NOT NULL REFERENCES schema(schema_id),
--    sample_id integer NOT NULL REFERENCES sample(sample_id),
--    field_num integer NOT NULL,
--    string_value text,
--    number_value real
--);
--
--CREATE INDEX ON field (schema_id, field_num);
