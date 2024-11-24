DELETE FROM actors;

DO
$do$
DECLARE
    start_year INT := 1969;
BEGIN
    FOR i IN 0..51 LOOP
		RAISE NOTICE '%', start_year + i;
        INSERT INTO actors
        WITH last_year AS (
            SELECT *
            FROM actors
            WHERE current_year = start_year + i
        ),
        this_year AS (
            SELECT
				year,
				actorid,
                MAX(actor) as actor,
                ARRAY_AGG(ARRAY[ROW(film, votes, rating, filmid)::films]) AS films,
                (CASE
                    WHEN AVG(rating) > 8 THEN 'star'
                    WHEN AVG(rating) > 7 THEN 'good'
                    WHEN AVG(rating) > 6 THEN 'average'
                    ELSE 'bad'
                END)::quality_class AS quality_class
            FROM actor_films
            WHERE year = start_year + i + 1
            GROUP BY actorid, year
        )

        SELECT
            COALESCE(ty.actor, ly.actor) AS actor,
            COALESCE(ty.actorid, ly.actorid) AS actorid,
            COALESCE(ly.films, ARRAY[]::films[][]) || CASE
                WHEN ty.year IS NOT NULL THEN ty.films
                ELSE ARRAY[]::films[][] END
            AS films,
            ty.year IS NOT NULL AS is_active,
            CASE
                WHEN ty.year IS NOT NULL THEN ty.quality_class
                ELSE ly.quality_class
            END AS quality_class,
            start_year + i + 1 AS current_year
        FROM this_year ty
        FULL OUTER JOIN last_year ly
        ON ty.actorid = ly.actorid;
    END LOOP;
END
$do$;
