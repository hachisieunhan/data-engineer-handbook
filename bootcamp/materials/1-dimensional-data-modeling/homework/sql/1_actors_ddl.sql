CREATE TYPE films AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);

CREATE TYPE quality_class AS
    ENUM ('bad', 'average', 'good', 'star');

CREATE TABLE actors (
    actor TEXT,
    actorid TEXT,
    films films[],
    is_active BOOLEAN,
    quality_class quality_class,
    current_year INTEGER,
    PRIMARY KEY (actorid, current_year)
);



