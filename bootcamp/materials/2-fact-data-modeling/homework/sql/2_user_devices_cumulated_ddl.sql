DROP TABLE IF EXISTS user_devices_cumulated;

CREATE TABLE user_devices_cumulated (
    user_id NUMERIC,
    browser_type TEXT,
    device_activity_datelist DATE[],
    date DATE,
    PRIMARY KEY (user_id, browser_type, date)
);
