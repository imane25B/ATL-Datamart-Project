SELECT dblink_connect('etoile_en_flocon', 'host=data-warehouse port=5432 dbname=nyc_warehouse user=postgres password=admin');

INSERT INTO dim_location (location_name, borough, latitude, longitude)
SELECT DISTINCT pulocationid, NULL, CAST(NULL AS double precision), CAST(NULL AS double precision) FROM dblink('etoile_en_flocon', 'SELECT DISTINCT pulocationid FROM nyc_raw') AS t(pulocationid INTEGER);

INSERT INTO dim_location (location_name, borough, latitude, longitude)
SELECT DISTINCT dolocationid, NULL, CAST(NULL AS double precision), CAST(NULL AS double precision) FROM dblink('etoile_en_flocon', 'SELECT DISTINCT dolocationid FROM nyc_raw') AS t(dolocationid INTEGER);
INSERT INTO dim_payment_type (payment_type)
SELECT DISTINCT payment_type FROM dblink('etoile_en_flocon', 'SELECT DISTINCT payment_type FROM nyc_raw') AS t(payment_type TEXT);

INSERT INTO dim_time (datetime, hour, day, month, year)
SELECT DISTINCT tpep_pickup_datetime, 
                EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour,
                EXTRACT(DAY FROM tpep_pickup_datetime) AS day,
                EXTRACT(MONTH FROM tpep_pickup_datetime) AS month,
                EXTRACT(YEAR FROM tpep_pickup_datetime) AS year
FROM dblink('etoile_en_flocon', 'SELECT DISTINCT tpep_pickup_datetime FROM nyc_raw') AS t(tpep_pickup_datetime TIMESTAMP);

INSERT INTO fact_trip (pickup_datetime, dropoff_datetime, passenger_count, trip_distance, fare_amount, total_amount, payment_type_id, pickup_location_id, dropoff_location_id, time_id)
SELECT tpep_pickup_datetime, 
       tpep_dropoff_datetime, 
       passenger_count, 
       trip_distance, 
       fare_amount, 
       total_amount, 
       dim_payment_type.payment_type_id, 
       dim_location_pu.location_id, 
       dim_location_do.location_id, 
       dim_time.time_id
FROM dblink('etoile_en_flocon', 'SELECT tpep_pickup_datetime::timestamp, 
                                       tpep_dropoff_datetime::timestamp, 
                                       passenger_count::integer, 
                                       trip_distance::double precision, 
                                       fare_amount::double precision, 
                                       total_amount::double precision, 
                                       payment_type, 
                                       pulocationid::integer, 
                                       dolocationid::integer 
                                   FROM nyc_raw') 
AS t(tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, passenger_count INTEGER, trip_distance DOUBLE PRECISION, fare_amount DOUBLE PRECISION, total_amount DOUBLE PRECISION, payment_type TEXT, pulocationid INTEGER, dolocationid INTEGER)
JOIN dim_payment_type ON t.payment_type = dim_payment_type.payment_type
JOIN dim_location AS dim_location_pu ON t.pulocationid = dim_location_pu.location_id
JOIN dim_location AS dim_location_do ON t.dolocationid = dim_location_do.location_id
JOIN dim_time ON t.tpep_pickup_datetime = dim_time.datetime;

SELECT dblink_disconnect('etoile_en_flocon');