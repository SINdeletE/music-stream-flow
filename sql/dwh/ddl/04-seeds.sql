-- Execute on one node: dim_event_type is replicated across the cluster.
-- Keep existing surrogate keys when the initialization is run again.
-- The zero load_id identifies bootstrap data, not an Airflow load.
INSERT INTO music_dwh.dim_event_type (name, load_id, deleted_at)
SELECT
    seed.name,
    toUUID('00000000-0000-0000-0000-000000000000'),
    NULL
FROM
(
    SELECT arrayJoin([
        'session_started',
        'session_ended',
        'track_selected',
        'track_liked',
        'track_disliked'
    ]) AS name
) AS seed
WHERE seed.name NOT IN
(
    SELECT name
    FROM music_dwh.dim_event_type FINAL
);
