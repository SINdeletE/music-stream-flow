-- Инициализация: измени примерные даты UTC и выполни весь файл в одной сессии.
SET param_timestamp_start = '2026-10-01 00:00:00.000';
SET param_timestamp_end = '2026-10-20 00:00:00.000';

-- Средняя длительность по авторизованным пользователям, без смешивания анонимов в одного пользователя.
-- Сессии выбираются по началу в [timestamp_start, timestamp_end), время в UTC.
-- Открытая сессия длится до timestamp_end (текущего времени для текущего отчёта).
-- Пользователь берётся из первого session_started, а не из произвольного события сессии.
WITH sessions AS
(
    SELECT
        events.session_id,
        tupleElement(argMinIf(
            tuple(events.user_id), events.event_timestamp, types.name = 'session_started'
        ), 1) AS user_id,
        minIf(events.event_timestamp, types.name = 'session_started') AS started_at,
        minOrNullIf(events.event_timestamp, types.name = 'session_ended') AS ended_at
    FROM music_dwh.fct_events_all AS events FINAL
    GLOBAL ALL INNER JOIN
    (
        SELECT event_type_sk, name
        FROM music_dwh.dim_event_type FINAL
        WHERE name IN ('session_started', 'session_ended')
    ) AS types ON events.event_type_sk = types.event_type_sk
    WHERE events.event_timestamp >= {timestamp_start:DateTime64(3, 'UTC')}
      AND events.event_timestamp < {timestamp_end:DateTime64(3, 'UTC')}
    GROUP BY events.session_id
    HAVING countIf(types.name = 'session_started') > 0
)
SELECT
    user_id,
    count() AS sessions_count,
    countIf(ended_at IS NOT NULL) AS completed_sessions_count,
    countIf(ended_at IS NULL) AS open_sessions_count,
    round(avgOrNull(dateDiff(
        'millisecond', started_at, coalesce(ended_at, {timestamp_end:DateTime64(3, 'UTC')})
    ) / 1000.0), 3) AS average_duration_seconds
FROM sessions
WHERE user_id IS NOT NULL
  AND (ended_at IS NULL OR ended_at >= started_at)
GROUP BY user_id
ORDER BY average_duration_seconds DESC, user_id;
