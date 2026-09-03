BEGIN;

ALTER TABLE music_oltp.track 
    ADD CONSTRAINT ck_device_duration_ms_non_negative CHECK (duration_ms >= 0);

ALTER TABLE music_oltp.track_artist 
    ADD CONSTRAINT ck_track_artist_artist_number_positive CHECK (artist_number > 0);
ALTER TABLE music_oltp.track_release 
    ADD CONSTRAINT ck_track_release_track_number_positive CHECK (track_number > 0);
ALTER TABLE music_oltp.track_release 
    ADD CONSTRAINT ck_track_release_disc_number_positive_or_null CHECK (disc_number IS NULL OR disc_number > 0);
ALTER TABLE music_oltp.artist_release 
    ADD CONSTRAINT ck_artist_release_artist_number_positive CHECK (artist_number > 0);

COMMIT;
