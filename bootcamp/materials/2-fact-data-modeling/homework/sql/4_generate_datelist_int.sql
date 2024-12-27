
WITH series_date AS (
    SELECT
        generate_series('2023-01-01', '2023-01-31', INTERVAL '1 day') AS valid_date
    ),
    starter AS (
    SELECT
        udc.device_activity_datelist @> ARRAY[DATE(sd.valid_date)] AS is_active,
        EXTRACT(DAY FROM DATE('2023-01-31') - sd.valid_date) AS days_since,
        udc.user_id,
        udc.browser_type
    FROM user_devices_cumulated udc
    CROSS JOIN series_date sd
    WHERE date = DATE('2023-01-31')
),
    bits AS (
        SELECT
            user_id,
            browser_type,
            SUM(CASE
                    WHEN is_active THEN POW(2, 32 - days_since)
                    ELSE 0 END)::bigint::bit(32) AS datelist_int,
            DATE('2023-01-31') as date
        FROM starter
        GROUP BY user_id, browser_type
)

SELECT * FROM bits
