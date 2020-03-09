CREATE EXTENSION Postgis;

-- Campaign represents a cruise
CREATE TABLE campaign (
    campaign_id SERIAL PRIMARY KEY,
    campaign_type VARCHAR(255),
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    deployment VARCHAR(255),
    start_location VARCHAR(255),
    end_location VARCHAR(255),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    urls TEXT []
);

CREATE TABLE project_type (
    project_type_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE project (
    project_id SERIAL PRIMARY KEY,
    project_type_id INTEGER NOT NULL REFERENCES project_type(project_type_id),
    accn VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    datapackage_url VARCHAR(255),
    url VARCHAR(255)
);

CREATE TYPE schema_type AS ENUM ('sample', 'ctd', 'niskin', 'other');

CREATE TABLE schema (
    schema_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    type schema_type NOT NULL,
    fields JSON NOT NULL
);

CREATE TABLE sampling_event (
    sampling_event_id SERIAL PRIMARY KEY,
    sampling_event_type VARCHAR(255),
    campaign_id INTEGER REFERENCES campaign(campaign_id),
    name VARCHAR(255) UNIQUE NOT NULL,
    locations GEOGRAPHY(MULTIPOINT,4326), -- can't use LINESTRING because it requires at least two points
    start_time TIMESTAMP,
    end_time TIMESTAMP
);

-- CTD/Niskin data
CREATE TABLE sampling_event_data (
    sampling_event_data_id SERIAL PRIMARY KEY,
    sampling_event_id INTEGER NOT NULL REFERENCES sampling_event(sampling_event_id),
    schema_id INTEGER NOT NULL REFERENCES schema(schema_id),
    number_vals REAL [],
    string_vals TEXT [],
    datetime_vals TIMESTAMP []
);

CREATE TABLE sample (
    sample_id SERIAL PRIMARY KEY,
    schema_id INTEGER NOT NULL REFERENCES schema(schema_id),
    accn VARCHAR(255) UNIQUE NOT NULL,
    locations GEOGRAPHY(MULTIPOINT,4326), -- can't use LINESTRING because it requires at least two points
    number_vals REAL [],
    string_vals TEXT [],
    datetime_vals TIMESTAMP []
);

CREATE TABLE sample_to_sampling_event (
    sample_to_sampling_event_id SERIAL PRIMARY KEY,
    sample_id INTEGER NOT NULL REFERENCES sample(sample_id),
    sampling_event_id INTEGER NOT NULL REFERENCES sampling_event(sampling_event_id),
    UNIQUE(sample_id, sampling_event_id)
);

CREATE TABLE project_to_sample (
    project_to_sample_id SERIAL PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES project(project_id),
    sample_id INTEGER NOT NULL REFERENCES sample(sample_id),
    UNIQUE(project_id, sample_id)
);

CREATE TABLE experiment (
    experiment_id SERIAL PRIMARY KEY,
    sample_id INTEGER NOT NULL REFERENCES sample(sample_id),
    name VARCHAR(255) NOT NULL,
    accn VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE library (
    library_id SERIAL PRIMARY KEY,
    experiment_id INTEGER NOT NULL REFERENCES experiment(experiment_id),
    name VARCHAR(255),
    strategy VARCHAR(255),
    source VARCHAR(255),
    selection VARCHAR(255),
    protocol TEXT,
    layout VARCHAR(255),
    length INTEGER
);

CREATE TABLE run (
    run_id SERIAL PRIMARY KEY,
    experiment_id INTEGER NOT NULL REFERENCES experiment(experiment_id),
    accn VARCHAR(255) UNIQUE NOT NULL,
    total_spots BIGINT NOT NULL,
    total_bases BIGINT NOT NULL,
    total_size BIGINT,
    avg_read_len SMALLINT,
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

CREATE TABLE "file" (
    file_id SERIAL PRIMARY KEY,
    file_type_id INTEGER NOT NULL REFERENCES file_type(file_type_id),
    file_format_id INTEGER NOT NULL REFERENCES file_format(file_format_id),
    url TEXT UNIQUE NOT NULL -- url or path, e.g. /iplant/home/...
);

CREATE TABLE run_to_file (
    run_to_file_id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES run(run_id),
    file_id INTEGER NOT NULL REFERENCES "file"(file_id),
    UNIQUE(run_id, file_id)
);

CREATE TABLE project_to_file (
    project_to_file_id SERIAL PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES project(project_id),
    file_id INTEGER NOT NULL REFERENCES "file"(file_id),
    UNIQUE(project_id, file_id)
);

CREATE TABLE taxonomy (
  tax_id INTEGER UNIQUE NOT NULL,
  name VARCHAR(255) NOT NULL DEFAULT ''
);

CREATE TABLE centrifuge (
  centrifuge_id SERIAL PRIMARY KEY,
  run_id INTEGER NOT NULL REFERENCES run(run_id),
  tax_id INTEGER NOT NULL REFERENCES taxonomy(tax_id),
  num_reads INTEGER NOT NULL,
  num_unique_reads INTEGER NOT NULL,
  abundance DOUBLE PRECISION NOT NULL DEFAULT 0,
  UNIQUE(run_id, tax_id)
);

CREATE TYPE provider AS ENUM ('plan-b', 'tacc-tapis');

CREATE TABLE "user" (
    user_id SERIAL PRIMARY KEY,
    user_name VARCHAR(50) UNIQUE NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(255),
    role SMALLINT, -- normal user 0, power user 1, admin 127
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE login (
  login_id SERIAL PRIMARY KEY,
  user_id INTEGER NOT NULL REFERENCES "user"(user_id),
  login_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE app (
  app_id SERIAL PRIMARY KEY,
  name VARCHAR(50) NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  is_maintenance BOOLEAN NOT NULL DEFAULT FALSE,
  provider provider
);

CREATE TABLE app_run (
  app_run_id SERIAL PRIMARY KEY,
  app_id INTEGER NOT NULL REFERENCES app(app_id),
  user_id INTEGER NOT NULL REFERENCES "user"(user_id),
  run_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  params TEXT
);

--CREATE TABLE app_result (
--  app_result_id SERIAL PRIMARY KEY,
--  app_id INTEGER NOT NULL REFERENCES app(app_id),
--  app_data_type_id INTEGER unsigned NOT NULL,
--  path VARCHAR(255) DEFAULT NULL
--);

-- Add default apps
INSERT INTO app (name,provider)
VALUES ('libra-1.0','plan-b'),
('centrifuge-1.0.4u2','plan-b'),
('ohana-blast-0.1.2u1','tacc-tapis'),
('mash-all-vs-all-0.0.6u1','tacc-tapis');