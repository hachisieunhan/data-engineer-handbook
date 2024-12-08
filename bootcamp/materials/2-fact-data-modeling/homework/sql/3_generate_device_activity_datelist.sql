-- Dedupped events
CREATE TABLE events_devices_dedupped (
    url TEXT,
    referrer TEXT,
    user_id NUMERIC,
    device_id NUMERIC,
    browser_type TEXT,
    host TEXT,
    event_time TIMESTAMP
);

INSERT INTO events_devices_dedupped
WITH e_dedupped AS (
	SELECT
		*
		, ROW_NUMBER() OVER (PARTITION BY user_id, device_id, host, event_time) AS row_no
	FROM events
),
    d_dedupped AS (
	SELECT
		*
		, ROW_NUMBER() OVER (PARTITION BY device_id) AS row_no
	FROM devices
)
SELECT
	ed.url
	, ed.referrer
	, ed.user_id
	, ed.device_id
	, dd.browser_type
	, ed.host
	, CAST(ed.event_time AS timestamp)
FROM e_dedupped ed
JOIN d_dedupped dd
    ON ed.device_id = dd.device_id
WHERE ed.row_no = 1 AND dd.row_no = 1;


-- Cumulative query
DELETE FROM user_devices_cumulated;

DO
$do$
DECLARE
    start_date DATE := '2022-12-31'::date;
BEGIN
    FOR i IN 0..30 LOOP
		RAISE NOTICE '%', start_date + ((i + 1)::text || ' day')::interval;
        WITH yesterday AS (
            SELECT * FROM user_devices_cumulated
            WHERE date = start_date + (i::text || ' day')::interval
        ),
            today AS (
                SELECT
                    user_id
                    , browser_type
                    , DATE_TRUNC('day', event_time) AS today_date
                    , COUNT(1) AS num_events
                FROM events_devices_dedupped
                WHERE DATE_TRUNC('day', event_time) = start_date + ((i + 1)::text || ' day')::interval
                    AND user_id IS NOT NULL
                    AND browser_type IS NOT NULL
                GROUP BY user_id, browser_type, DATE_TRUNC('day', event_time)
        )
        INSERT INTO user_devices_cumulated
        SELECT
            COALESCE(t.user_id, y.user_id),
            COALESCE(t.browser_type, y.browser_type),
            CASE
                WHEN t.user_id IS NOT NULL OR t.browser_type IS NOT NULL
                    THEN ARRAY[t.today_date]::DATE[]
                ELSE ARRAY[]::DATE[]
            END || COALESCE(y.device_activity_datelist, ARRAY[]::DATE[])
            AS device_activity_datelist,
            COALESCE(t.today_date, y.date + INTERVAL '1 day')::date as date
        FROM yesterday y
        FULL OUTER JOIN today t
            ON t.user_id = y.user_id
            AND t.browser_type = y.browser_type;
    END LOOP;
END
$do$;
