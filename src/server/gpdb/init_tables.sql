create table if not exists sensor_data
(
    sensor_id CHAR(8) PRIMARY KEY,
    controller_id CHAR(32) NOT NULL,
    latitude DECIMAL(9, 6) NOT NULL,
    longitude DECIMAL(9, 6) NOT NULL,
    temperature DECIMAL(4, 2) NOT NULL,
    event_datetime TIMESTAMP NOT NULL
);

-- create table if not exists sensor_data_processed
-- (
--   id bigserial primary key,
--   fld varchar(255),
--   mark varchar(255),
--   nil varchar(255)
-- );
