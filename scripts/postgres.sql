-- Load:
-- psql -d <db_name> -f postgres.sql
-- delete from project_to_sample; delete from sample; delete from project; delete from schema; delete from sampling_event; delete from campaign;

CREATE EXTENSION Postgis;

--CREATE TABLE ontology (
--    ontology_id SERIAL PRIMARY KEY,
--    name VARCHAR(255) UNIQUE NOT NULL,
--    description TEXT,
--    purl TEXT UNIQUE NOT NULL
--);

--CREATE TABLE ontology_term (
--    ontology_term_id SERIAL PRIMARY KEY,
--    ontology_id INTEGER NOT NULL REFERENCES ontology(ontology_id),
--    label VARCHAR(255) NOT NULL,
--    description TEXT NOT NULL,
--    purl TEXT UNIQUE NOT NULL
--);

CREATE TABLE schema (
    schema_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    fields JSON NOT NULL, --TODO remove, replaced by field tables
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

--CREATE TABLE field_source_category (
--    field_source_category_id SERIAL PRIMARY KEY,
--    name VARCHAR(255) UNIQUE NOT NULL,
--    description TEXT
--);
--
--CREATE TABLE field_source (
--    field_source_id SERIAL PRIMARY KEY,
--    field_source_category_id INTEGER NOT NULL REFERENCES field_source_category(field_source_category_id),
--    url TEXT UNIQUE NOT NULL
--);
--
--CREATE TYPE field_type AS ENUM ('number', 'string', 'datetime');
--
--CREATE TABLE field (
--    field_id SERIAL PRIMARY KEY,
--    field_source_id INTEGER NOT NULL REFERENCES field_source(field_source_id),
--    schema_id INTEGER NOT NULL REFERENCES schema(schema_id),
----    ontology_term_id INTEGER NOT NULL REFERENCES ontology_term(ontology_term_id),
----    unit_ontology_term_id INTEGER NOT NULL REFERENCES ontology_term(ontology_term_id),
--    name VARCHAR(255) NOT NULL,
--    type field_type NOT NULL,
--    position INTEGER NOT NULL
--);

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
    name VARCHAR(255) UNIQUE NOT NULL,
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
    sampling_event_type VARCHAR(255), --TODO change to sampling_event_type_id and NOT NULL
    campaign_id INTEGER REFERENCES campaign(campaign_id),
    name VARCHAR(255) UNIQUE NOT NULL,
    locations GEOGRAPHY(MULTIPOINT,4326), -- can't use LINESTRING because they require at least two points
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
--    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

--CREATE INDEX sampling_event_locations_gix ON sample USING GIST (locations);

CREATE TABLE sample (
    sample_id SERIAL PRIMARY KEY,
    schema_id INTEGER NOT NULL REFERENCES schema(schema_id),
    accn VARCHAR(255) UNIQUE NOT NULL,
--    name VARCHAR(255) NOT NULL,
    locations GEOGRAPHY(MULTIPOINT,4326), -- can't use LINESTRING because they require at least two points
    number_vals REAL [],
    string_vals TEXT [],
    datetime_vals TIMESTAMP [],
--    purl_index JSON,
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
--    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
);

--CREATE INDEX sample_locations_gix ON sample USING GIST (locations);

--CREATE TABLE sample_data (
--    sample_id INTEGER NOT NULL REFERENCES sample(sample_id),
--    purl VARCHAR(255) NOT NULL,
--    type VARCHAR(255) NOT NULL,
--    number_vals REAL [],
--    string_vals TEXT [],
--    datetime_vals TIMESTAMP [],
--    UNIQUE(sample_id, purl, type)
--);

CREATE TABLE sample_to_sampling_event (
    sample_to_sampling_event_id SERIAL PRIMARY KEY,
    sample_id INTEGER NOT NULL REFERENCES sample(sample_id),
    sampling_event_id INTEGER NOT NULL REFERENCES sampling_event(sampling_event_id),
    UNIQUE(sample_id, sampling_event_id)
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
    url VARCHAR(255),
    private BOOLEAN NOT NULL DEFAULT TRUE,
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
--    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE project_to_sample (
    project_to_sample_id SERIAL PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES project(project_id),
    sample_id INTEGER NOT NULL REFERENCES sample(sample_id),
    UNIQUE(project_id, sample_id)
);

-- replaces project_group in iMicrobe
--CREATE TABLE organization (
--    organization_id SERIAL PRIMARY KEY,
--    name VARCHAR(255) UNIQUE NOT NULL,
--    description TEXT,
--    url VARCHAR(255),
--    private BOOLEAN NOT NULL DEFAULT TRUE,
--    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
--);

CREATE TABLE experiment (
    experiment_id SERIAL PRIMARY KEY,
    sample_id INTEGER NOT NULL REFERENCES sample(sample_id),
    name VARCHAR(255) NOT NULL,
    accn VARCHAR(255) UNIQUE NOT NULL,
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
--    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
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
    orcid VARCHAR(30),
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

--CREATE TABLE organization_to_user (
--    organization_to_user_id SERIAL PRIMARY KEY,
--    organization_id INTEGER NOT NULL REFERENCES organization(organization_id),
--    user_id INTEGER NOT NULL REFERENCES "user"(user_id)
--);
--
--CREATE TABLE organization_to_project (
--    organization_to_project_id SERIAL PRIMARY KEY,
--    organization_id INTEGER NOT NULL REFERENCES organization(organization_id),
--    project_id INTEGER NOT NULL REFERENCES project(project_id)
--);
