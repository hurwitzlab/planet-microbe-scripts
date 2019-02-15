
DROP TABLE IF EXISTS schema CASCADE;
DROP TABLE IF EXISTS sample CASCADE;

CREATE DATABASE arraytestgis;

CREATE EXTENSION Postgis;

CREATE TABLE schema (
    schema_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    fields JSON NOT NULL
);

CREATE TABLE sample (
    sample_id SERIAL PRIMARY KEY,
    schema_id INTEGER NOT NULL REFERENCES schema(schema_id),
    sample_accn VARCHAR(255)
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    location GEOGRAPHY(POINT,4326),

    -- Fields for storing dataset-specific attributes
    number_vals REAL [],
    string_vals TEXT [],
    datetime_vals TIMESTAMP []
);

CREATE INDEX schema_id_idx ON sample (schema_id);

CREATE INDEX location_gix ON sample USING GIST (location);
