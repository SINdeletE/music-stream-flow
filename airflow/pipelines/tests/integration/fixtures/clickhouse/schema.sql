-- Standalone insertion schema. No replication, Distributed or Keeper defaults.
-- Column names and types follow 0001-create.sql and 0003-staging.sql.
CREATE DATABASE music_dwh;

CREATE TABLE music_dwh.dim_device_all
(
    device_sk BIGINT,
    device_id UUID,
    name String,
    device_version String,
    load_id UUID,
    source_created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree ORDER BY device_id;

CREATE TABLE music_dwh.dim_device_os_all
(
    device_os_sk BIGINT,
    device_os_id UUID,
    name String,
    device_os_version String,
    valid_from DateTime64(3, 'UTC'),
    load_id UUID,
    source_created_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree ORDER BY device_os_id;

CREATE TABLE music_dwh.dim_app_release
(
    app_release_sk BIGINT,
    app_release_id UUID,
    platform String,
    app_release_version String,
    build String,
    released_at DateTime64(3, 'UTC'),
    load_id UUID,
    source_created_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree ORDER BY app_release_id;

CREATE TABLE music_dwh.dim_track_all
(
    track_sk BIGINT,
    track_id UUID,
    name String,
    duration_ms Int32,
    is_available Bool DEFAULT false,
    load_id UUID,
    source_created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    deleted_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = MergeTree ORDER BY track_id;

CREATE TABLE music_dwh.dim_artist_all
(
    artist_sk BIGINT,
    artist_id UUID,
    nickname String,
    name Nullable(String),
    middle_name Nullable(String),
    last_name Nullable(String),
    country_code Nullable(String),
    biography Nullable(String),
    is_verified Bool,
    load_id UUID,
    source_created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    deleted_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = MergeTree ORDER BY artist_id;

CREATE TABLE music_dwh.dim_genre
(
    genre_sk BIGINT,
    genre_id UUID,
    name String,
    load_id UUID,
    source_created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    deleted_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = MergeTree ORDER BY genre_id;

CREATE TABLE music_dwh.dim_release_all
(
    release_sk BIGINT,
    release_id UUID,
    name String,
    is_available Bool DEFAULT false,
    released_at DateTime64(3, 'UTC'),
    load_id UUID,
    source_created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    deleted_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = MergeTree ORDER BY release_id;

CREATE TABLE music_dwh.dim_event_type
(
    event_type_sk BIGINT,
    name String,
    load_id UUID,
    deleted_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = ReplacingMergeTree ORDER BY name;

CREATE TABLE music_dwh.brg_device_app_release_all
(
    device_sk BIGINT,
    app_release_sk BIGINT,
    load_id UUID
)
ENGINE = MergeTree ORDER BY (device_sk, app_release_sk);

CREATE TABLE music_dwh.brg_track_genre_all
(
    track_sk BIGINT,
    genre_sk BIGINT,
    load_id UUID
)
ENGINE = MergeTree ORDER BY (track_sk, genre_sk);

CREATE TABLE music_dwh.brg_track_artist_all
(
    track_sk BIGINT,
    artist_sk BIGINT,
    artist_number Int32,
    load_id UUID
)
ENGINE = MergeTree ORDER BY (track_sk, artist_sk);

CREATE TABLE music_dwh.brg_track_release_all
(
    track_sk BIGINT,
    release_sk BIGINT,
    track_number Int32,
    disc_number Nullable(Int32),
    load_id UUID
)
ENGINE = MergeTree ORDER BY (track_sk, release_sk);

CREATE TABLE music_dwh.brg_artist_release_all
(
    artist_sk BIGINT,
    release_sk BIGINT,
    artist_number Int32,
    load_id UUID
)
ENGINE = MergeTree ORDER BY (artist_sk, release_sk);

CREATE TABLE music_dwh.fct_events_all
(
    event_sk BIGINT,
    event_id UUID,
    user_id Nullable(UUID),
    event_type_sk BIGINT,
    device_sk BIGINT,
    device_os_sk BIGINT,
    app_release_sk BIGINT,
    track_sk Nullable(BIGINT),
    session_id UUID,
    playback_id Nullable(UUID),
    event_timestamp DateTime64(3, 'UTC'),
    received_at DateTime64(3, 'UTC'),
    load_id UUID
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (event_timestamp, event_id);

CREATE TABLE music_dwh.etl_loads
(
    load_id UUID PRIMARY KEY,
    dag_id String,
    run_id String,
    task_id String,
    source_system String,
    target_table String,
    interval_start DateTime64(3, 'UTC'),
    interval_end DateTime64(3, 'UTC'),
    started_at DateTime64(3, 'UTC'),
    finished_at Nullable(DateTime64(3, 'UTC')),
    state LowCardinality(String),
    rows_read UInt64,
    rows_written UInt64,
    rows_rejected UInt64,
    error_msg Nullable(String)
)
ENGINE = MergeTree ORDER BY load_id;

CREATE TABLE music_dwh.stg_datasets_all
(
    source LowCardinality(String),
    dag_id String,
    run_id String,
    task_id String,
    interval_start DateTime64(6, 'UTC'),
    interval_end DateTime64(6, 'UTC'),
    dataset_id UUID,
    created_at DateTime64(6, 'UTC'),
    batch_count UInt64,
    row_count UInt64,
    dimension_counts Map(String, UInt64),
    topic Nullable(String),
    partition Nullable(Int32),
    committed_offset Nullable(Int64),
    first_offset Nullable(Int64),
    next_offset Nullable(Int64),
    data_expires_at Nullable(DateTime('UTC'))
)
ENGINE = MergeTree
ORDER BY dataset_id;

CREATE TABLE music_dwh.stg_batches_all
(
    dataset_id UUID,
    batch_number UInt64,
    payload String,
    row_count UInt64,
    next_offset Nullable(Int64),
    stored_at DateTime('UTC')
)
ENGINE = MergeTree
PARTITION BY toDate(stored_at)
ORDER BY (dataset_id, batch_number)
TTL stored_at + INTERVAL 3 DAY DELETE;

CREATE TABLE music_dwh.stg_missing_dimension_ids_all
(
    dataset_id UUID,
    dimension LowCardinality(String),
    entity_id UUID,
    stored_at DateTime('UTC')
)
ENGINE = MergeTree
PARTITION BY toDate(stored_at)
ORDER BY (dataset_id, dimension, entity_id)
TTL stored_at + INTERVAL 3 DAY DELETE;
