-- Requires ClickHouse 25.1+ and a configured ZooKeeper / ClickHouse Keeper.
-- Omit generated *_sk columns from INSERT to use DEFAULT; 0 is reserved for sentinel rows.
-- Existing tables require ALTER TABLE MODIFY COLUMN: CREATE IF NOT EXISTS does not update defaults.
CREATE DATABASE IF NOT EXISTS music_dwh ON CLUSTER company_cluster;

-- Dims

CREATE TABLE IF NOT EXISTS music_dwh.dim_device ON CLUSTER company_cluster
(
    device_sk Int64 DEFAULT generateSerialID('music_dwh.dim_device', 1),
    device_id UUID,
    user_id UUID,
    name String,
    device_version String,
    loaded_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    deleted_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/music_dwh/dim_device', '{replica}')
ORDER BY device_sk;

CREATE TABLE IF NOT EXISTS music_dwh.dim_device_all ON CLUSTER company_cluster
AS music_dwh.dim_device
ENGINE = Distributed(company_cluster, music_dwh, dim_device, cityHash64(device_sk));

CREATE TABLE IF NOT EXISTS music_dwh.dim_device_os ON CLUSTER company_cluster
(
    device_os_sk Int64 DEFAULT generateSerialID('music_dwh.dim_device_os', 1),
    device_os_id UUID,
    name String,
    device_os_version String,
    valid_from DateTime64(3, 'UTC'),
    loaded_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    deleted_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/music_dwh/dim_device_os', '{replica}')
ORDER BY device_os_sk;

CREATE TABLE IF NOT EXISTS music_dwh.dim_device_os_all ON CLUSTER company_cluster
AS music_dwh.dim_device_os
ENGINE = Distributed(company_cluster, music_dwh, dim_device_os, cityHash64(device_os_sk));

CREATE TABLE IF NOT EXISTS music_dwh.dim_app_release ON CLUSTER company_cluster
(
    app_release_sk Int64 DEFAULT generateSerialID('music_dwh.dim_app_release', 1),
    app_release_id UUID,
    platform String,
    app_release_version String,
    build String,
    released_at DateTime64(3, 'UTC'),
    loaded_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    deleted_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/music_dwh/dim_app_release', '{replica}')
ORDER BY app_release_sk;

CREATE TABLE IF NOT EXISTS music_dwh.dim_track ON CLUSTER company_cluster
(
    track_sk Int64 DEFAULT generateSerialID('music_dwh.dim_track', 1),
    track_id UUID,
    name String,
    duration_ms Int32,
    is_available Bool DEFAULT false,
    track_created_at DateTime64(3, 'UTC'),
    track_deleted_at Nullable(DateTime64(3, 'UTC')),
    loaded_at DateTime64(3, 'UTC'),
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
    artist_sk Int64 DEFAULT generateSerialID('music_dwh.dim_artist', 1),
    artist_id UUID,
    nickname String,
    name Nullable(String),
    middle_name Nullable(String),
    last_name Nullable(String),
    country_code Nullable(String),
    biography Nullable(String),
    is_verified Bool,
    created_at DateTime64(3, 'UTC'),
    loaded_at DateTime64(3, 'UTC'),
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
    genre_sk Int64 DEFAULT generateSerialID('music_dwh.dim_genre', 1),
    genre_id UUID,
    name String,
    loaded_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    deleted_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/music_dwh/dim_genre', '{replica}')
ORDER BY genre_sk;

CREATE TABLE IF NOT EXISTS music_dwh.dim_release ON CLUSTER company_cluster
(
    release_sk Int64 DEFAULT generateSerialID('music_dwh.dim_release', 1),
    release_id UUID,
    name String,
    is_available Bool DEFAULT false,
    released_at DateTime64(3, 'UTC'),
    loaded_at DateTime64(3, 'UTC'),
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
    event_type_sk Int64 DEFAULT generateSerialID('music_dwh.dim_event_type', 1),
    name String,
    loaded_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    deleted_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/music_dwh/dim_event_type', '{replica}')
ORDER BY event_type_sk;

-- Bridges

CREATE TABLE IF NOT EXISTS music_dwh.brg_device_app_release ON CLUSTER company_cluster
(
    device_sk Int64,
    app_release_sk Int64,
    loaded_at DateTime64(3, 'UTC')
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/music_dwh/brg_device_app_release', '{replica}')
ORDER BY (device_sk, app_release_sk);

CREATE TABLE IF NOT EXISTS music_dwh.brg_device_app_release_all ON CLUSTER company_cluster
AS music_dwh.brg_device_app_release
ENGINE = Distributed(company_cluster, music_dwh, brg_device_app_release, cityHash64(device_sk));

CREATE TABLE IF NOT EXISTS music_dwh.brg_track_genre ON CLUSTER company_cluster
(
    track_sk Int64,
    genre_sk Int64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/music_dwh/brg_track_genre', '{replica}')
ORDER BY (track_sk, genre_sk);

CREATE TABLE IF NOT EXISTS music_dwh.brg_track_genre_all ON CLUSTER company_cluster
AS music_dwh.brg_track_genre
ENGINE = Distributed(company_cluster, music_dwh, brg_track_genre, cityHash64(track_sk));

CREATE TABLE IF NOT EXISTS music_dwh.brg_track_artist ON CLUSTER company_cluster
(
    track_sk Int64,
    artist_sk Int64,
    artist_number Int32
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/music_dwh/brg_track_artist', '{replica}')
ORDER BY (track_sk, artist_sk);

CREATE TABLE IF NOT EXISTS music_dwh.brg_track_artist_all ON CLUSTER company_cluster
AS music_dwh.brg_track_artist
ENGINE = Distributed(company_cluster, music_dwh, brg_track_artist, cityHash64(track_sk));

CREATE TABLE IF NOT EXISTS music_dwh.brg_track_release ON CLUSTER company_cluster
(
    track_sk Int64,
    release_sk Int64,
    track_number Int32,
    disc_number Nullable(Int32)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/music_dwh/brg_track_release', '{replica}')
ORDER BY (track_sk, release_sk);

CREATE TABLE IF NOT EXISTS music_dwh.brg_track_release_all ON CLUSTER company_cluster
AS music_dwh.brg_track_release
ENGINE = Distributed(company_cluster, music_dwh, brg_track_release, cityHash64(track_sk));

CREATE TABLE IF NOT EXISTS music_dwh.brg_artist_release ON CLUSTER company_cluster
(
    artist_sk Int64,
    release_sk Int64,
    artist_number Int32
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/music_dwh/brg_artist_release', '{replica}')
ORDER BY (artist_sk, release_sk);

CREATE TABLE IF NOT EXISTS music_dwh.brg_artist_release_all ON CLUSTER company_cluster
AS music_dwh.brg_artist_release
ENGINE = Distributed(company_cluster, music_dwh, brg_artist_release, cityHash64(release_sk));

-- Fact

CREATE TABLE IF NOT EXISTS music_dwh.fct_events ON CLUSTER company_cluster
(
    event_sk Int64 DEFAULT generateSerialID('music_dwh.fct_events', 1),
    event_id UUID,
    event_type_sk Int64,
    device_sk Int64,
    device_os_sk Int64,
    app_release_sk Int64,
    track_sk Int64,
    session_id UUID,

    playback_id Nullable(UUID),
    event_timestamp DateTime64(3, 'UTC'),
    received_at DateTime64(3, 'UTC'),
    loaded_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/music_dwh/fct_events', '{replica}')
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY event_timestamp;

CREATE TABLE IF NOT EXISTS music_dwh.fct_events_all ON CLUSTER company_cluster
AS music_dwh.fct_events
ENGINE = Distributed(company_cluster, music_dwh, fct_events, cityHash64(event_sk));
