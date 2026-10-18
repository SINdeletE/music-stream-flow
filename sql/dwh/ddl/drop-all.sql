-- MANUAL RESET: drops all tables defined in 01-create.sql and 03-staging.sql and their data.
-- This script must not be included in automatic initialization.

-- Drop Distributed tables before their underlying local tables.
DROP TABLE IF EXISTS music_dwh.stg_missing_dimension_ids_all ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.stg_batches_all ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.stg_datasets_all ON CLUSTER company_cluster SYNC;

DROP TABLE IF EXISTS music_dwh.fct_events_all ON CLUSTER company_cluster SYNC;

DROP TABLE IF EXISTS music_dwh.brg_artist_release_all ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.brg_track_release_all ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.brg_track_artist_all ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.brg_track_genre_all ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.brg_device_app_release_all ON CLUSTER company_cluster SYNC;

DROP TABLE IF EXISTS music_dwh.dim_release_all ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.dim_artist_all ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.dim_track_all ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.dim_device_os_all ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.dim_device_all ON CLUSTER company_cluster SYNC;

-- Drop the fact and bridges before dimensions.
DROP TABLE IF EXISTS music_dwh.stg_missing_dimension_ids ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.stg_batches ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.stg_datasets ON CLUSTER company_cluster SYNC;

DROP TABLE IF EXISTS music_dwh.fct_events ON CLUSTER company_cluster SYNC;

DROP TABLE IF EXISTS music_dwh.brg_artist_release ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.brg_track_release ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.brg_track_artist ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.brg_track_genre ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.brg_device_app_release ON CLUSTER company_cluster SYNC;

DROP TABLE IF EXISTS music_dwh.dim_event_type ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.dim_release ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.dim_genre ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.dim_artist ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.dim_track ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.dim_app_release ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.dim_device_os ON CLUSTER company_cluster SYNC;
DROP TABLE IF EXISTS music_dwh.dim_device ON CLUSTER company_cluster SYNC;

DROP TABLE IF EXISTS music_dwh.etl_loads ON CLUSTER company_cluster SYNC;
