DROP TABLE IF EXISTS actors_history_scd;

CREATE TABLE actors_history_scd (
    actor TEXT,
    actorid TEXT,
    is_active BOOLEAN,
    quality_class quality_class,
    start_date INTEGER,
    end_date INTEGER,
	PRIMARY KEY (actorid, start_date)
)
