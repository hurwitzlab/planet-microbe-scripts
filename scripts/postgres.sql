-- Load:
-- psql -d pm_test -f postgres.sql
-- delete from project_to_sample; delete from sample; delete from project; delete from schema; delete from sampling_event; delete from campaign;

CREATE EXTENSION Postgis;

CREATE TABLE ontology (
    ontology_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    purl TEXT UNIQUE NOT NULL
);

CREATE TABLE ontology_term (
    ontology_term_id SERIAL PRIMARY KEY,
    ontology_id INTEGER NOT NULL REFERENCES ontology(ontology_id),
    label VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    purl TEXT UNIQUE NOT NULL
);

CREATE TABLE schema (
    schema_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    fields JSON NOT NULL, --TODO remove, replaced by field tables
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE field_source_category (
    field_source_category_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE field_source (
    field_source_id SERIAL PRIMARY KEY,
    field_source_category_id INTEGER NOT NULL REFERENCES field_source_category(field_source_category_id),
    url TEXT UNIQUE NOT NULL
);

CREATE TYPE field_type AS ENUM ('number', 'string', 'datetime');

CREATE TABLE field (
    field_id SERIAL PRIMARY KEY,
    field_source_id INTEGER NOT NULL REFERENCES field_source(field_source_id),
    schema_id INTEGER NOT NULL REFERENCES schema(schema_id),
    ontology_term_id INTEGER NOT NULL REFERENCES ontology_term(ontology_term_id),
    unit_ontology_term_id INTEGER NOT NULL REFERENCES ontology_term(ontology_term_id),
    name VARCHAR(255) NOT NULL,
    type field_type NOT NULL,
    position INTEGER NOT NULL
);

CREATE TABLE campaign_type (
    campaign_type_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT
);

-- Campaign represents cruise
CREATE TABLE campaign (
    campaign_id SERIAL PRIMARY KEY,
--    campaign_type_id INTEGER NOT NULL REFERENCES campaign_type(campaign_type_id),
    campaign_type VARCHAR(255), --TODO change to campaign_type_id
    name VARCHAR(255) NOT NULL,
    description TEXT,
    deployment VARCHAR(255),
    start_location VARCHAR(255),
    end_location VARCHAR(255),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    urls TEXT []
);

CREATE TABLE sampling_event_type (
    sampling_event_type_id SERIAL PRIMARY KEY,
--    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE sampling_event (
    sampling_event_id SERIAL PRIMARY KEY,
--    sampling_event_type_id INTEGER NOT NULL REFERENCES sampling_event_type(sampling_event_type_id),
    sampling_event_type VARCHAR(255), --TODO change to sampling_event_type_id
    campaign_id INTEGER REFERENCES campaign(campaign_id),
    name VARCHAR(255) NOT NULL,
    locations GEOGRAPHY(MULTIPOINT,4326) NOT NULL, -- can't use LINESTRING because they require at least two points
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    data_url TEXT NOT NULL, -- path to CSV file, e.g. datastore:/iplant/home/...
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
--    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

--CREATE INDEX sampling_event_locations_gix ON sample USING GIST (locations);

CREATE TABLE sample (
    sample_id SERIAL PRIMARY KEY,
    sampling_event_id INTEGER REFERENCES sampling_event(sampling_event_id),
    schema_id INTEGER NOT NULL REFERENCES schema(schema_id),
    accn VARCHAR(255),
--    name VARCHAR(255) NOT NULL,
    locations GEOGRAPHY(MULTIPOINT,4326) NOT NULL, -- can't use LINESTRING because they require at least two points
    number_vals REAL [],
    string_vals TEXT [],
    datetime_vals TIMESTAMP [],
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
--    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
);

--CREATE INDEX sample_locations_gix ON sample USING GIST (locations);

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

CREATE TABLE project_to_sample (
    project_to_sample_id SERIAL PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES project(project_id),
    sample_id INTEGER NOT NULL REFERENCES sample(sample_id)
);

-- replaces project_group in iMicrobe
CREATE TABLE organization (
    organization_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    url VARCHAR(255),
    private BOOLEAN NOT NULL DEFAULT TRUE,
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE experiment_type (
    experiment_type_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE experiment (
    experiment_id SERIAL PRIMARY KEY,
    sample_id INTEGER NOT NULL REFERENCES sample(sample_id),
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
    url TEXT NOT NULL -- path, e.g. datastore:/iplant/home/...
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