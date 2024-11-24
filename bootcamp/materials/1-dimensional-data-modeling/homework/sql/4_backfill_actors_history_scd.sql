INSERT INTO actors_history_scd
WITH streak_started AS (
    SELECT
        actor,
        actorid,
        current_year,
        is_active,
        quality_class,
        LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) <> is_active
            OR LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) IS NULL
            AS did_is_active_changed,
        LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) <> quality_class
            OR LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) IS NULL
            AS did_quality_class_changed
    FROM actors
),
    streak_identified AS (
        SELECT
            actor,
            actorid,
            is_active,
            quality_class,
            current_year,
            SUM(CASE WHEN did_is_active_changed THEN 1 WHEN did_quality_class_changed THEN 1 ELSE 0 END)
                OVER (PARTITION BY actorid ORDER BY current_year) as streak_identifier
        FROM streak_started
),
    aggregated AS (
        SELECT
            actor,
            actorid,
            is_active,
            quality_class,
            streak_identifier,
            MIN(current_year) AS start_date,
            MAX(current_year) AS end_date
        FROM streak_identified
        GROUP BY actor, actorid, is_active, quality_class, streak_identifier
)

SELECT
    actor,
    actorid,
    is_active,
    quality_class,
    start_date,
    end_date
FROM aggregated
