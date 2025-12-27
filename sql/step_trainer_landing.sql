CREATE EXTERNAL TABLE IF NOT EXISTS stedi_db.step_trainer_landing (
    sensorreadingtime BIGINT,
    serialnumber STRING,
    distancefromobject DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://d609-stedi-ankur-2025/landing/step_trainer_landing/';
