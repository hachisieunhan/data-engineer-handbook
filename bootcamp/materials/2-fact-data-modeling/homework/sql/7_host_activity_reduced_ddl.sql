DROP TABLE IF EXISTS host_activity_reduced;

CREATE TABLE host_activity_reduced (
    month DATE,
    host TEXT,
    hits_array INTEGER[],
    unique_visitors_array INTEGER[],
    date DATE,
    PRIMARY KEY (host, date)
);
