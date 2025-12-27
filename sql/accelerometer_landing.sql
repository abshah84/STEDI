CREATE EXTERNAL TABLE IF NOT EXISTS stedi_db.accelerometer_landing (
    timestamp BIGINT,
    user STRING,
    x DOUBLE,
    y DOUBLE,
    z DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://d609-stedi-ankur-2025/landing/accelerometer_landing/';
