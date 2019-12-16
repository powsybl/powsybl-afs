CREATE KEYSPACE IF NOT EXISTS afs WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

// CREATE KEYSPACE IF NOT EXISTS afs WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1, 'dc2' : 1 };

CREATE TABLE IF NOT EXISTS root_node (
    fs_name text,
    root_id timeuuid,
    PRIMARY KEY (fs_name)
);

CREATE TABLE IF NOT EXISTS children_by_name_and_class (
    id timeuuid,
    child_name text,
    child_pseudo_class text,
    child_creation_date timestamp,
    child_description text,
    child_consistent boolean,
    child_group text, // TODO
    child_id timeuuid,
    child_modification_date timestamp,
    child_owner text, // TODO
    child_version int,
    child_users_rights frozen<map<text,smallint>>, // TODO
    child_groups_rights frozen<map<text,smallint>>, // TODO
    child_others_rights smallint, // TODO
    cmt map<text, text>,
    cmi map<text, int>,
    cmd map<text, double>,
    cmb map<text, boolean>,
    creation_date timestamp static,
    consistent boolean static,
    description text static,
    group text static, // TODO
    modification_date timestamp static,
    name text static,
    owner text static, // TODO
    parent_id timeuuid static,
    pseudo_class text static,
    version int static,
    mt map<text, text> static,
    mi map<text, int> static,
    md map<text, double> static,
    mb map<text, boolean> static,
    users_rights frozen<map<text,smallint>> static, // TODO
    groups_rights frozen<map<text,smallint>> static, // TODO
    others_rights smallint static, // TODO
    PRIMARY KEY (id, child_name, child_pseudo_class)
);

CREATE MATERIALIZED VIEW children_by_class_and_name AS
    SELECT id, child_pseudo_class, child_name, child_creation_date, child_description, child_group, child_id, child_modification_date, child_owner,
    child_version, child_users_rights, child_groups_rights, child_others_rights, cmt, cmi, cmd, cmb
    FROM children_by_name_and_class
    WHERE id IS NOT NULL AND child_name IS NOT NULL AND child_pseudo_class IS NOT NULL
    PRIMARY KEY (id, child_pseudo_class, child_name);

CREATE TABLE IF NOT EXISTS dependencies (
    from_id timeuuid,
    name text,
    to_id timeuuid,
    dep_id timeuuid,
    PRIMARY KEY (from_id, name, to_id, dep_id)
);

CREATE TABLE IF NOT EXISTS backward_dependencies (
    to_id timeuuid,
    name text,
    from_id timeuuid,
    PRIMARY KEY (to_id, name, from_id)
);

CREATE TABLE IF NOT EXISTS node_data_names (
    id timeuuid,
    name text,
    PRIMARY KEY (id, name)
);

CREATE TABLE IF NOT EXISTS node_data (
    id timeuuid,
    name text,
    chunks_count int static,
    chunk blob,
    chunk_num int,
    PRIMARY KEY ((id, name), chunk_num)
);

CREATE TABLE IF NOT EXISTS regular_time_series (
    id timeuuid,
    time_series_name text,
    data_type text,
    time_series_tags frozen<map<text, text>>,
    start timestamp,
    end timestamp,
    spacing bigint,
    PRIMARY KEY (id, time_series_name)
);

CREATE TABLE IF NOT EXISTS time_series_data_chunk_types (
    id timeuuid,
    time_series_name text,
    version int,
    chunk_id timeuuid,
    chunk_type int,
    PRIMARY KEY (id, time_series_name, version, chunk_id, chunk_type)
);

CREATE TABLE IF NOT EXISTS double_time_series_data_uncompressed_chunks (
    id timeuuid,
    time_series_name text,
    version int,
    chunk_id timeuuid,
    offset int,
    values frozen<list<double>>,
    PRIMARY KEY ((id, time_series_name, version), chunk_id)
);

CREATE TABLE IF NOT EXISTS double_time_series_data_compressed_chunks (
    id timeuuid,
    time_series_name text,
    version int,
    chunk_id timeuuid,
    offset int,
    uncompressed_length int,
    step_values frozen<list<double>>,
    step_lengths frozen<list<int>>,
    PRIMARY KEY ((id, time_series_name, version), chunk_id)
);

CREATE TABLE IF NOT EXISTS string_time_series_data_uncompressed_chunks (
    id timeuuid,
    time_series_name text,
    version int,
    chunk_id timeuuid,
    offset int,
    values frozen<list<text>>,
    PRIMARY KEY ((id, time_series_name, version), chunk_id)
);

CREATE TABLE IF NOT EXISTS string_time_series_data_compressed_chunks (
    id timeuuid,
    time_series_name text,
    version int,
    chunk_id timeuuid,
    offset int,
    uncompressed_length int,
    step_values frozen<list<text>>,
    step_lengths frozen<list<int>>,
    PRIMARY KEY ((id, time_series_name, version), chunk_id)
);
