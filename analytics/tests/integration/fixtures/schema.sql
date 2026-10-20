-- Standalone analytics schema matching the columns and keys of sql/dwh/ddl/0001-create.sql.
-- Distributed table names are kept, without replication or Keeper-generated surrogate keys.
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
ENGINE = ReplacingMergeTree(updated_at) ORDER BY device_id;

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
ENGINE = ReplacingMergeTree ORDER BY device_os_id;

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
ENGINE = ReplacingMergeTree(updated_at) ORDER BY track_id;

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
ENGINE = ReplacingMergeTree(updated_at) ORDER BY artist_id;

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
ENGINE = ReplacingMergeTree(updated_at) ORDER BY release_id;

CREATE TABLE music_dwh.dim_event_type
(
    event_type_sk BIGINT,
    name String,
    load_id UUID,
    deleted_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = ReplacingMergeTree ORDER BY name;

CREATE TABLE music_dwh.brg_track_artist_all
(
    track_sk BIGINT,
    artist_sk BIGINT,
    artist_number Int32,
    load_id UUID
)
ENGINE = ReplacingMergeTree ORDER BY (track_sk, artist_sk);

CREATE TABLE music_dwh.brg_track_release_all
(
    track_sk BIGINT,
    release_sk BIGINT,
    track_number Int32,
    disc_number Nullable(Int32),
    load_id UUID
)
ENGINE = ReplacingMergeTree ORDER BY (track_sk, release_sk);

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
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (event_timestamp, event_id);
