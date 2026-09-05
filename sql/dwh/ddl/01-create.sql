BEGIN;

CREATE SCHEMA IF NOT EXISTS music_dwh;

CREATE E

CREATE TABLE music_dwh.dim_event ON CLUSTER company_cluster (
    event_type_sk UInt16 PRIMARY KEY,
    event_type Varchar(16)
);

CREATE TABLE music_dwh.dim_device ON CLUSTER company_cluster (
    device_sk UInt16 PRIMARY KEY,
    name Varchar(32)
    device_version Varchar(32)

    os Varchar(32)
    os_version Varchar(32)
);

CREATE TABLE music_dwh.dim_installation ON CLUSTER company_cluster (
    device_sk UInt16 PRIMARY KEY,
    name Varchar(32)
    device_version Varchar(32)

    os Varchar(32)
    os_version Varchar(32)
);

CREATE TABLE music_dwh.dim_track ON CLUSTER company_cluster (
    track_sk UInt64 PRIMARY KEY,
    name Varchar(255),
    duration_ms Int64,
    is_available Bool
);

CREATE TABLE music_dwh.fct_events ON CLUSTER company_cluster (
    event_sk UInt64 PRIMARY KEY,
    event_id Varchar(64),

    event_type_sk UInt16,
    device_sk UInt32,
    track_sk UInt64


    event_timestamp DateTime64(3, 'europe/moscow')
)
ENGINE = MergeTree()
PARTITION BY event_id;

COMMIT;
