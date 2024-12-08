-- Cumulative query
DELETE FROM hosts_cumulated;

DO
$do$
DECLARE
    start_date DATE := '2022-12-31'::date;
BEGIN
    FOR i IN 0..30 LOOP
		RAISE NOTICE '%', start_date + ((i + 1)::text || ' day')::interval;
        WITH yesterday AS (
            SELECT * FROM hosts_cumulated
            WHERE date = start_date + (i::text || ' day')::interval
        ),
            today AS (
                SELECT
                    host
                    , DATE_TRUNC('day', event_time) AS today_date
                    , COUNT(1) AS num_events
                FROM events_devices_dedupped
                WHERE DATE_TRUNC('day', event_time) = start_date + ((i + 1)::text || ' day')::interval
                    AND host IS NOT NULL
                GROUP BY host, DATE_TRUNC('day', event_time)
        )
        INSERT INTO hosts_cumulated
        SELECT
            COALESCE(t.host, y.host),
            CASE
                WHEN t.host IS NOT NULL
                    THEN ARRAY[t.today_date]::DATE[]
                ELSE ARRAY[]::DATE[]
            END || COALESCE(y.host_activity_datelist, ARRAY[]::DATE[])
            AS host_activity_datelist,
            COALESCE(t.today_date, y.date + INTERVAL '1 day')::date as date
        FROM yesterday y
        FULL OUTER JOIN today t
            ON t.host = y.host;
    END LOOP;
END
$do$;
