-- Инициализация: с 1970 года — за всё время. Измени конец UTC и выполни файл в одной сессии.
SET param_timestamp_start = '1970-01-01 00:00:00.000';
SET param_timestamp_end = '2026-10-20 00:00:00.000';
SET param_limit = 10;

-- Считаются события track_liked, а не текущее состояние лайка и не уникальные пользователи.
-- Параметры: timestamp_start, timestamp_end (UTC, интервал [start, end)), limit.
-- Для всего времени: timestamp_start = '1970-01-01 00:00:00', timestamp_end = текущее UTC.
WITH track_likes AS
(
    SELECT track_sk, count() AS likes_count
    FROM music_dwh.fct_events_all FINAL
    WHERE event_timestamp >= {timestamp_start:DateTime64(3, 'UTC')}
      AND event_timestamp < {timestamp_end:DateTime64(3, 'UTC')}
      AND track_sk IS NOT NULL
      AND event_type_sk GLOBAL IN
      (
          SELECT event_type_sk
          FROM music_dwh.dim_event_type FINAL
          WHERE name = 'track_liked'
      )
    GROUP BY track_sk
)
SELECT track.track_id, track.name AS track_name, liked.likes_count
FROM track_likes AS liked
GLOBAL ALL INNER JOIN
(
    SELECT track_sk, track_id, name
    FROM music_dwh.dim_track_all FINAL
) AS track ON liked.track_sk = track.track_sk
ORDER BY likes_count DESC, track_id
LIMIT {limit:UInt32};
