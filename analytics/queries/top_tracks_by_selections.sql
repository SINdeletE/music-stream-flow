-- Инициализация: измени примерные даты UTC и выполни весь файл в одной сессии.
SET param_timestamp_start = '2026-10-01 00:00:00.000';
SET param_timestamp_end = '2026-10-20 00:00:00.000';
SET param_limit = 10;

-- Прослушивание = одно событие track_selected, не завершение проигрывания.
-- Параметры: timestamp_start, timestamp_end (UTC, интервал [start, end)), limit.
WITH track_selections AS
(
    SELECT track_sk, count() AS selections_count
    FROM music_dwh.fct_events_all FINAL
    WHERE event_timestamp >= {timestamp_start:DateTime64(3, 'UTC')}
      AND event_timestamp < {timestamp_end:DateTime64(3, 'UTC')}
      AND track_sk IS NOT NULL
      AND event_type_sk GLOBAL IN
      (
          SELECT event_type_sk
          FROM music_dwh.dim_event_type FINAL
          WHERE name = 'track_selected'
      )
    GROUP BY track_sk
)
SELECT track.track_id, track.name AS track_name, selected.selections_count
FROM track_selections AS selected
GLOBAL ALL INNER JOIN
(
    SELECT track_sk, track_id, name
    FROM music_dwh.dim_track_all FINAL
) AS track ON selected.track_sk = track.track_sk
ORDER BY selections_count DESC, track_id
LIMIT {limit:UInt32};
