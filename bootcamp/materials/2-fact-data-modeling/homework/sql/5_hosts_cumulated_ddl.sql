DROP TABLE IF EXISTS hosts_cumulated;

CREATE TABLE hosts_cumulated (
    host TEXT,
    host_activity_datelist DATE[],
    date DATE,
    PRIMARY KEY (host, date)
);
