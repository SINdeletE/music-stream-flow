-- Инициализация: измени примерные даты UTC и выполни весь файл в одной сессии.
SET param_timestamp_start = '2026-10-01 00:00:00.000';
SET param_timestamp_end = '2026-10-20 00:00:00.000';
SET param_limit = 10;

-- Сумма track_selected по всем трекам каждого релиза.
-- При нескольких релизах трека выбор полностью засчитывается каждому релизу.
-- Тип релиза в DWH отсутствует: отличить альбом от сингла этим запросом нельзя.
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
SELECT
    releases.release_id,
    releases.name AS release_name,
    sum(selected.selections_count) AS selections_count
FROM track_selections AS selected
GLOBAL ALL INNER JOIN
(
    SELECT DISTINCT track_sk, release_sk
    FROM music_dwh.brg_track_release_all
) AS link ON selected.track_sk = link.track_sk
GLOBAL ALL INNER JOIN
(
    SELECT release_sk, release_id, name
    FROM music_dwh.dim_release_all FINAL
) AS releases ON link.release_sk = releases.release_sk
GROUP BY releases.release_id, releases.name
ORDER BY selections_count DESC, release_id
LIMIT {limit:UInt32};
