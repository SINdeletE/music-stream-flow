-- MANUAL RESET: drops all tables defined in 0001-create.sql and their data.
-- This script must not be included in automatic initialization.
-- Keep the schema and do not cascade into unrelated dependent objects.
BEGIN;

-- Drop linking and dependent tables before their referenced entities.
DROP TABLE IF EXISTS music_oltp.artist_release;
DROP TABLE IF EXISTS music_oltp.track_release;
DROP TABLE IF EXISTS music_oltp.track_artist;
DROP TABLE IF EXISTS music_oltp.track_genre;
DROP TABLE IF EXISTS music_oltp.installation;
DROP TABLE IF EXISTS music_oltp.device_os_history;

DROP TABLE IF EXISTS music_oltp.release;
DROP TABLE IF EXISTS music_oltp.artist;
DROP TABLE IF EXISTS music_oltp.genre;
DROP TABLE IF EXISTS music_oltp.track;
DROP TABLE IF EXISTS music_oltp.app_release;
DROP TABLE IF EXISTS music_oltp.device;

COMMIT;
