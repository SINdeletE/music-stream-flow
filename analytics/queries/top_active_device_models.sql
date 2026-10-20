-- Инициализация: измени примерную дату UTC и выполни весь файл в одной сессии.
SET param_as_of = '2026-10-20 00:00:00.000';
SET param_limit = 10;

-- Топ моделей телефона по числу уникальных сессий за последние 7 дней.
-- Аппаратные ревизии с одинаковым name считаются одной моделью.
-- last_seen вычисляется из факта, а не из текущего состояния OLTP.
-- Параметры: as_of (UTC, текущее время), limit (например, 10).
SELECT
    device.name AS device_name,
    count() AS events_count,
    uniqExact(events.session_id) AS sessions_count,
    max(events.event_timestamp) AS last_seen_at
FROM music_dwh.fct_events_all AS events FINAL
GLOBAL ALL INNER JOIN
(
    SELECT device_sk, name
    FROM music_dwh.dim_device_all FINAL
) AS device ON events.device_sk = device.device_sk
WHERE events.event_timestamp > {as_of:DateTime64(3, 'UTC')} - INTERVAL 7 DAY
  AND events.event_timestamp < {as_of:DateTime64(3, 'UTC')}
GROUP BY device.name
ORDER BY sessions_count DESC, device_name
LIMIT {limit:UInt32};
