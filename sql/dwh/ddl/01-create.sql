CREATE DATABASE IF NOT EXISTS music_dwh ON CLUSTER company_cluster;

-- Dims

CREATE TABLE IF NOT EXISTS music_dwh.dim_device ON CLUSTER company_cluster
(
    device_sk BIGINT DEFAULT generateSerialID('music_dwh.dim_device', 1),
    device_id UUID,
    user_id UUID,
    name String,
    device_version String,

    load_id UUID,
    source_created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/music_dwh/dim_device', '{replica}')
ORDER BY device_sk;

CREATE TABLE IF NOT EXISTS music_dwh.dim_device_all ON CLUSTER company_cluster
AS music_dwh.dim_device
ENGINE = Distributed(company_cluster, music_dwh, dim_device, cityHash64(device_sk));

CREATE TABLE IF NOT EXISTS music_dwh.dim_device_os ON CLUSTER company_cluster
(
    device_os_sk BIGINT DEFAULT generateSerialID('music_dwh.dim_device_os', 1),
    device_os_id UUID,
    name String,
    device_os_version String,
    valid_from DateTime64(3, 'UTC'),

    load_id UUID,
    source_created_at DateTime64(3, 'UTC')
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/music_dwh/dim_device_os', '{replica}')
ORDER BY device_os_sk;

CREATE TABLE IF NOT EXISTS music_dwh.dim_device_os_all ON CLUSTER company_cluster
AS music_dwh.dim_device_os
ENGINE = Distributed(company_cluster, music_dwh, dim_device_os, cityHash64(device_os_sk));

CREATE TABLE IF NOT EXISTS music_dwh.dim_app_release ON CLUSTER company_cluster
(
    app_release_sk BIGINT DEFAULT generateSerialID('music_dwh.dim_app_release', 1),
    app_release_id UUID,
    platform String,
    app_release_version String,
    build String,
    released_at DateTime64(3, 'UTC'),

    load_id UUID,
    source_created_at DateTime64(3, 'UTC')
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/music_dwh/dim_app_release', '{replica}')
ORDER BY app_release_sk;

CREATE TABLE IF NOT EXISTS music_dwh.dim_track ON CLUSTER company_cluster
(
    track_sk BIGINT DEFAULT generateSerialID('music_dwh.dim_track', 1),
    track_id UUID,
    name String,
    duration_ms Int32,
    is_available Bool DEFAULT false,

    load_id UUID,
    source_created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    deleted_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/music_dwh/dim_track', '{replica}')
ORDER BY track_sk;

CREATE TABLE IF NOT EXISTS music_dwh.dim_track_all ON CLUSTER company_cluster
AS music_dwh.dim_track
ENGINE = Distributed(company_cluster, music_dwh, dim_track, cityHash64(track_sk));

CREATE TABLE IF NOT EXISTS music_dwh.dim_artist ON CLUSTER company_cluster
(
    artist_sk BIGINT DEFAULT generateSerialID('music_dwh.dim_artist', 1),
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
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/music_dwh/dim_artist', '{replica}')
ORDER BY artist_sk;

CREATE TABLE IF NOT EXISTS music_dwh.dim_artist_all ON CLUSTER company_cluster
AS music_dwh.dim_artist
ENGINE = Distributed(company_cluster, music_dwh, dim_artist, cityHash64(artist_sk));

CREATE TABLE IF NOT EXISTS music_dwh.dim_genre ON CLUSTER company_cluster
(
    genre_sk BIGINT DEFAULT generateSerialID('music_dwh.dim_genre', 1),
    genre_id UUID,
    name String,

    load_id UUID,
    source_created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    deleted_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/music_dwh/dim_genre', '{replica}')
ORDER BY genre_sk;

CREATE TABLE IF NOT EXISTS music_dwh.dim_release ON CLUSTER company_cluster
(
    release_sk BIGINT DEFAULT generateSerialID('music_dwh.dim_release', 1),
    release_id UUID,
    name String,
    is_available Bool DEFAULT false,
    released_at DateTime64(3, 'UTC'),

    load_id UUID,
    source_created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    deleted_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/music_dwh/dim_release', '{replica}')
ORDER BY release_sk;

CREATE TABLE IF NOT EXISTS music_dwh.dim_release_all ON CLUSTER company_cluster
AS music_dwh.dim_release
ENGINE = Distributed(company_cluster, music_dwh, dim_release, cityHash64(release_sk));

CREATE TABLE IF NOT EXISTS music_dwh.dim_event_type ON CLUSTER company_cluster
(
    event_type_sk BIGINT DEFAULT generateSerialID('music_dwh.dim_event_type', 1),
    name String,
    load_id UUID,
    updated_at DateTime64(3, 'UTC'),
    deleted_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/music_dwh/dim_event_type', '{replica}')
ORDER BY event_type_sk;

-- Bridges

CREATE TABLE IF NOT EXISTS music_dwh.brg_device_app_release ON CLUSTER company_cluster
(
    device_sk BIGINT,
    app_release_sk BIGINT,

    load_id UUID
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/music_dwh/brg_device_app_release', '{replica}')
ORDER BY (device_sk, app_release_sk);

CREATE TABLE IF NOT EXISTS music_dwh.brg_device_app_release_all ON CLUSTER company_cluster
AS music_dwh.brg_device_app_release
ENGINE = Distributed(company_cluster, music_dwh, brg_device_app_release, cityHash64(device_sk));

CREATE TABLE IF NOT EXISTS music_dwh.brg_track_genre ON CLUSTER company_cluster
(
    track_sk BIGINT,
    genre_sk BIGINT,

    load_id UUID
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/music_dwh/brg_track_genre', '{replica}')
ORDER BY (track_sk, genre_sk);

CREATE TABLE IF NOT EXISTS music_dwh.brg_track_genre_all ON CLUSTER company_cluster
AS music_dwh.brg_track_genre
ENGINE = Distributed(company_cluster, music_dwh, brg_track_genre, cityHash64(track_sk));

CREATE TABLE IF NOT EXISTS music_dwh.brg_track_artist ON CLUSTER company_cluster
(
    track_sk BIGINT,
    artist_sk BIGINT,
    artist_number Int32,

    load_id UUID
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/music_dwh/brg_track_artist', '{replica}')
ORDER BY (track_sk, artist_sk);

CREATE TABLE IF NOT EXISTS music_dwh.brg_track_artist_all ON CLUSTER company_cluster
AS music_dwh.brg_track_artist
ENGINE = Distributed(company_cluster, music_dwh, brg_track_artist, cityHash64(track_sk));

CREATE TABLE IF NOT EXISTS music_dwh.brg_track_release ON CLUSTER company_cluster
(
    track_sk BIGINT,
    release_sk BIGINT,
    track_number Int32,
    disc_number Nullable(Int32),

    load_id UUID
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/music_dwh/brg_track_release', '{replica}')
ORDER BY (track_sk, release_sk);

CREATE TABLE IF NOT EXISTS music_dwh.brg_track_release_all ON CLUSTER company_cluster
AS music_dwh.brg_track_release
ENGINE = Distributed(company_cluster, music_dwh, brg_track_release, cityHash64(track_sk));

CREATE TABLE IF NOT EXISTS music_dwh.brg_artist_release ON CLUSTER company_cluster
(
    artist_sk BIGINT,
    release_sk BIGINT,
    artist_number Int32,

    load_id UUID
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/music_dwh/brg_artist_release', '{replica}')
ORDER BY (artist_sk, release_sk);

CREATE TABLE IF NOT EXISTS music_dwh.brg_artist_release_all ON CLUSTER company_cluster
AS music_dwh.brg_artist_release
ENGINE = Distributed(company_cluster, music_dwh, brg_artist_release, cityHash64(release_sk));

-- Fact

CREATE TABLE IF NOT EXISTS music_dwh.fct_events ON CLUSTER company_cluster
(
    event_sk BIGINT DEFAULT generateSerialID('music_dwh.fct_events', 1),
    event_id UUID,
    event_type_sk BIGINT,
    device_sk BIGINT,
    device_os_sk BIGINT,
    app_release_sk BIGINT,
    track_sk BIGINT,
    session_id UUID,

    playback_id Nullable(UUID),
    event_timestamp DateTime64(3, 'UTC'),
    received_at DateTime64(3, 'UTC'),
    load_id UUID,
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/music_dwh/fct_events', '{replica}')
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY event_timestamp;

CREATE TABLE IF NOT EXISTS music_dwh.fct_events_all ON CLUSTER company_cluster
AS music_dwh.fct_events
ENGINE = Distributed(company_cluster, music_dwh, fct_events, cityHash64(event_sk));


-- Load audit table

CREATE TABLE IF NOT EXISTS music_dwh.etl_loads ON CLUSTER company_cluster
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
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/music_dwh/etl_loads', '{replica}')
ORDER BY event_type_sk;
