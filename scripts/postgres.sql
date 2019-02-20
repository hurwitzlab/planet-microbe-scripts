
--DROP TABLE IF EXISTS schema CASCADE;
--DROP TABLE IF EXISTS sample CASCADE;
--DROP DATABASE IF EXISTS pm;

--CREATE DATABASE pm;

CREATE EXTENSION Postgis;

-- replaces project_group in iMicrobe
CREATE TABLE organization (
    organization_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    url VARCHAR(255),
    private BOOLEAN NOT NULL DEFAULT TRUE,
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE "user" (
    user_id SERIAL PRIMARY KEY,
    user_name VARCHAR(50) UNIQUE NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(255),
    role SMALLINT, -- normal user 0, power user 1, admin 127
    orcid VARCHAR(30),
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE project_type (
    project_type_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE project (
    project_id SERIAL PRIMARY KEY,
    project_type_id INTEGER NOT NULL REFERENCES project_type(project_type_id),
    accn VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    url VARCHAR(255),
    private BOOLEAN NOT NULL DEFAULT TRUE,
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
--    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE organization_to_user (
    organization_to_user_id SERIAL PRIMARY KEY,
    organization_id INTEGER NOT NULL REFERENCES organization(organization_id),
    user_id INTEGER NOT NULL REFERENCES "user"(user_id)
);

CREATE TABLE organization_to_project (
    organization_to_project_id SERIAL PRIMARY KEY,
    organization_id INTEGER NOT NULL REFERENCES organization(organization_id),
    project_id INTEGER NOT NULL REFERENCES project(project_id)
);

CREATE TABLE schema (
    schema_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    fields JSON NOT NULL,
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Fields for storing dataset-specific attributes
CREATE TABLE dataset (
    dataset_id SERIAL PRIMARY KEY,
    schema_id INTEGER NOT NULL REFERENCES schema(schema_id),
    number_vals REAL [],
    string_vals TEXT [],
    datetime_vals TIMESTAMP []
);

CREATE TABLE sampling_event_type (
    sampling_event_type_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE sampling_event (
    sampling_event_id SERIAL PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES project(project_id),
    dataset_id INTEGER NOT NULL REFERENCES dataset(dataset_id),
    sampling_event_type_id INTEGER NOT NULL REFERENCES sampling_event_type(sampling_event_type_id),
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
--    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE sample (
    sample_id SERIAL PRIMARY KEY,
    dataset_id INTEGER NOT NULL REFERENCES dataset(dataset_id),
    sampling_event_id INTEGER NOT NULL REFERENCES sampling_event(sampling_event_id),
    accn VARCHAR(255),
    name VARCHAR(255) NOT NULL,
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
--    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    location GEOGRAPHY(POINT,4326)
);

CREATE TABLE experiment_type (
    experiment_type_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE experiment (
    experiment_id SERIAL PRIMARY KEY,
    sample_id INTEGER NOT NULL REFERENCES sample(sample_id),
    sampling_event_id INTEGER NOT NULL REFERENCES sampling_event(sampling_event_id),
    experiment_type_id INTEGER NOT NULL REFERENCES experiment_type(experiment_type_id),
    name VARCHAR(255) NOT NULL,
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
--    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE library (
    library_id SERIAL PRIMARY KEY,
    experiment_id INTEGER NOT NULL REFERENCES experiment(experiment_id)
    --TODO
);

CREATE TABLE run (
    run_id SERIAL PRIMARY KEY,
    library_id INTEGER NOT NULL REFERENCES library(library_id),
    num_spots BIGINT NOT NULL,
    total_size BIGINT NOT NULL,
    avg_read_len SMALLINT NOT NULL,
    gc_content REAL,
    time_of_run TIMESTAMP
);

CREATE TABLE file_type (
    file_type_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE file_format (
    file_format_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    extensions VARCHAR(10) []
);

CREATE TABLE sequence_file (
    sequence_file_id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES run(run_id),
    file_type_id INTEGER NOT NULL REFERENCES file_type(file_type_id),
    file_format_id INTEGER NOT NULL REFERENCES file_format(file_format_id),
    uri text NOT NULL -- path, e.g. datastore:/iplant/home/...

);

CREATE INDEX schema_id_idx ON dataset(schema_id);

CREATE INDEX location_gix ON sample USING GIST (location);
