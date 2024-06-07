create table if not exists sensor_data_processed
(
    sensor_id CHAR(8) PRIMARY KEY,
    controller_id CHAR(32) NOT NULL,
    latitude DECIMAL(9, 6) NOT NULL,
    longitude DECIMAL(9, 6) NOT NULL,
    temperature DECIMAL(4, 2) NOT NULL,
    event_datetime TIMESTAMP NOT NULL
);
