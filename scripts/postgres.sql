
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
    string_vals text [],
    datetime_vals timestamp []
);

CREATE INDEX schema_id_idx ON sample (schema_id);

CREATE INDEX location_gix ON sample USING GIST (location);
