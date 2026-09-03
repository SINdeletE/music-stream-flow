BEGIN;

CREATE SCHEMA IF NOT EXISTS music_oltp;

CREATE TABLE IF NOT EXISTS music_oltp.device (
    id UUID PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    device_version VARCHAR(16) NOT NULL,
    os_name VARCHAR(32) NOT NULL,
    os_version VARCHAR(32) NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS music_oltp.app_release (
    id UUID PRIMARY KEY,
    platform VARCHAR(16) NOT NULL,
    app_release_version VARCHAR(16) NOT NULL,
    build VARCHAR(32) NOT NULL,
    released_at TIMESTAMPTZ NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS music_oltp.installation (
    id UUID PRIMARY KEY,
    device_id UUID NOT NULL REFERENCES music_oltp.device(id) ON DELETE RESTRICT,
    app_release_id UUID NOT NULL REFERENCES music_oltp.app_release(id) ON DELETE RESTRICT,
    installed_at TIMESTAMPTZ NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS music_oltp.track (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    duration_ms INTEGER NOT NULL,
    image_url VARCHAR(255),
    is_available BOOLEAN NOT NULL DEFAULT FALSE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS music_oltp.genre (
    id UUID PRIMARY KEY,
    name VARCHAR(64) NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS music_oltp.artist (
    id UUID PRIMARY KEY,
    nickname VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    middle_name VARCHAR(255),
    last_name VARCHAR(255),
    country_code VARCHAR(2),
    biography VARCHAR(512),
    image_url VARCHAR(255),
    is_verified BOOLEAN NOT NULL DEFAULT FALSE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS music_oltp.release (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    is_available BOOLEAN NOT NULL DEFAULT FALSE,
    released_at TIMESTAMPTZ NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS music_oltp.track_genre (
    track_id UUID NOT NULL REFERENCES music_oltp.track(id) ON DELETE CASCADE,
    genre_id UUID NOT NULL REFERENCES music_oltp.genre(id) ON DELETE CASCADE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (track_id, genre_id)
);

CREATE TABLE IF NOT EXISTS music_oltp.track_artist (
    track_id UUID NOT NULL REFERENCES music_oltp.track(id) ON DELETE CASCADE,
    artist_id UUID NOT NULL REFERENCES music_oltp.artist(id) ON DELETE CASCADE,
    artist_number INTEGER NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (track_id, artist_id)
);

CREATE TABLE IF NOT EXISTS music_oltp.track_release (
    track_id UUID NOT NULL REFERENCES music_oltp.track(id) ON DELETE CASCADE,
    release_id UUID NOT NULL REFERENCES music_oltp.release(id) ON DELETE CASCADE,
    track_number INTEGER NOT NULL,
    disc_number INTEGER,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (track_id, release_id)
);

CREATE TABLE IF NOT EXISTS music_oltp.artist_release (
    artist_id UUID NOT NULL REFERENCES music_oltp.artist(id) ON DELETE CASCADE,
    release_id UUID NOT NULL REFERENCES music_oltp.release(id) ON DELETE CASCADE,
    artist_number INTEGER NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (artist_id, release_id)
);

COMMIT;
