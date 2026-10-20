-- Инициализация: измени примерные даты UTC и выполни весь файл в одной сессии.
SET param_timestamp_start = '2026-10-01 00:00:00.000';
SET param_timestamp_end = '2026-10-20 00:00:00.000';
SET param_limit = 10;

-- Выбор трека полностью засчитывается каждому связанному исполнителю.
-- Используется текущий состав исполнителей трека, а не исторический состав на дату события.
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
    artist.artist_id,
    artist.nickname AS artist_nickname,
    sum(selected.selections_count) AS selections_count
FROM track_selections AS selected
GLOBAL ALL INNER JOIN
(
    SELECT DISTINCT track_sk, artist_sk
    FROM music_dwh.brg_track_artist_all
) AS link ON selected.track_sk = link.track_sk
GLOBAL ALL INNER JOIN
(
    SELECT artist_sk, artist_id, nickname
    FROM music_dwh.dim_artist_all FINAL
) AS artist ON link.artist_sk = artist.artist_sk
GROUP BY artist.artist_id, artist.nickname
ORDER BY selections_count DESC, artist_id
LIMIT {limit:UInt32};
