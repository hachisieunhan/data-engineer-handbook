-- Create table actors_history_scd_2020 to get the scd until the year 2020 to test the incremental load
-- Since this is the incremental loaded table, we need the current_year column as the indicator
DROP TABLE IF EXISTS actors_history_scd_2020;
CREATE TABLE actors_history_scd_2020 (
    actor TEXT,
    actorid TEXT,
    is_active BOOLEAN,
    quality_class quality_class,
    start_date INTEGER,
    end_date INTEGER,
    current_year INTEGER,
	PRIMARY KEY (actorid, start_date)
);

-- Insert data into actors_history_scd_2020 with data from actors table up until the year 2020
INSERT INTO actors_history_scd_2020
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
    WHERE current_year <= 2020
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
    end_date,
    2020 AS current_year
FROM aggregated;

-- Create scd_type
DROP TYPE IF EXISTS scd_type;
CREATE TYPE scd_type AS (
    is_active boolean,
    quality_class quality_class,
    start_date INTEGER,
    end_date INTEGER
);

-- Incremental query to combine data from previous year SCD data with the new incoming data from actors table
WITH
    last_year_scd AS (
        SELECT * FROM actors_history_scd_2020
        WHERE current_year = 2020
        AND end_date = 2020
),
    historical_scd AS (
        SELECT
            actor,
            actorid,
            is_active,
            quality_class,
            start_date,
            end_date
        FROM actors_history_scd_2020
        WHERE current_year = 2020
        AND end_date < 2020
),
    this_year_data AS (
        SELECT * FROM actors
        WHERE current_year = 2021
),
    unchanged_records AS (
        SELECT
            ty.actor,
            ty.actorid,
            ty.is_active,
            ty.quality_class,
            ly.start_date,
            ty.current_year as end_date
        FROM this_year_data ty
        JOIN last_year_scd ly
        ON ly.actorid = ty.actorid
        WHERE ty.quality_class = ly.quality_class
            AND ty.is_active = ly.is_active
),
    changed_records AS (
        SELECT
            ty.actor,
            ty.actorid,
            UNNEST(ARRAY[
                ROW(
                    ly.is_active,
                    ly.quality_class,
                    ly.start_date,
                    ly.end_date
                )::scd_type,
                ROW(
                    ty.is_active,
                    ty.quality_class,
                    ty.current_year,
                    ty.current_year
                )::scd_type
            ]) AS records
        FROM this_year_data ty
        LEFT JOIN last_year_scd ly
        ON ly.actorid = ty.actorid
        WHERE (ty.quality_class <> ly.quality_class
            OR ty.is_active <> ly.is_active)
),
    unnested_changed_records AS (
        SELECT
            actor,
            actorid,
            (records::scd_type).is_active,
            (records::scd_type).quality_class,
            (records::scd_type).start_date,
            (records::scd_type).end_date
        FROM changed_records
),
    new_records AS (
        SELECT
            ty.actor,
            ty.actorid,
            ty.is_active,
            ty.quality_class,
            ty.current_year AS start_date,
            ty.current_year AS end_date
        FROM this_year_data ty
        LEFT JOIN last_year_scd ly
            ON ty.actorid = ly.actorid
        WHERE ly.actorid IS NULL
)

SELECT
    *,
    2021 AS current_season
FROM (
    SELECT *
    FROM historical_scd

    UNION ALL

    SELECT *
    FROM unchanged_records

    UNION ALL

    SELECT *
    FROM unnested_changed_records

    UNION ALL

    SELECT *
    FROM new_records
) a
ORDER BY actorid, start_date
