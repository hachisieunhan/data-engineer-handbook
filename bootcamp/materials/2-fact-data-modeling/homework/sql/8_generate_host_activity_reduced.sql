DELETE FROM host_activity_reduced;

DO
$do$
DECLARE
    start_date DATE := '2022-12-31'::date;
BEGIN
    FOR i IN 0..30 LOOP
		RAISE NOTICE '%', start_date + ((i + 1)::text || ' day')::interval;

        WITH yesterday AS (
            SELECT *
            FROM host_activity_reduced
            WHERE date = start_date + (i::text || ' day')::interval
        ),
        today AS (
            SELECT
                DATE_TRUNC('month', event_time) as month,
                host,
                COUNT(1) as hits,
                COUNT(DISTINCT user_id) as unique_visitors
            FROM events_dedupped
            WHERE user_id IS NOT NULL
            AND event_time::date = start_date + ((i + 1)::text || ' day')::interval
            GROUP BY month, host
        )
        INSERT INTO host_activity_reduced
        SELECT
            COALESCE(t.month, y.month),
            COALESCE(t.host, y.host),
            COALESCE(y.hits_array, ARRAY[]::INTEGER[])
                || CASE
                    WHEN t.hits IS NULL THEN ARRAY[]::INTEGER[]
                    ELSE ARRAY[t.hits]::INTEGER[]
                    END AS hits_array,
            COALESCE(y.unique_visitors_array, ARRAY[]::INTEGER[])
            || CASE
                WHEN t.unique_visitors IS NULL THEN ARRAY[]::INTEGER[]
                ELSE ARRAY[t.unique_visitors]::INTEGER[]
                END AS unique_visitors_array,
            (start_date + ((i + 1)::text || ' day')::interval)::date AS date
        FROM today t
        FULL OUTER JOIN yesterday y
        ON t.host = y.host;
    END LOOP;
END
$do$;