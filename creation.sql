CREATE TABLE dim_time (
    time_id SERIAL PRIMARY KEY,
    datetime TIMESTAMP,
    hour INTEGER,
    day INTEGER,
    month INTEGER,
    year INTEGER
);

CREATE TABLE dim_location (
    location_id SERIAL PRIMARY KEY,
    location_name TEXT,
    borough TEXT,
    latitude FLOAT,
    longitude FLOAT
);

CREATE TABLE dim_payment_type (
    payment_type_id SERIAL PRIMARY KEY,
    payment_type TEXT
);

CREATE TABLE fact_trip (
    trip_id SERIAL PRIMARY KEY,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance FLOAT,
    fare_amount FLOAT,
    total_amount FLOAT,
    payment_type_id INTEGER REFERENCES dim_payment_type(payment_type_id),
    pickup_location_id INTEGER REFERENCES dim_location(location_id),
    dropoff_location_id INTEGER REFERENCES dim_location(location_id),
    time_id INTEGER REFERENCES dim_time(time_id)
);