CREATE EXTERNAL TABLE IF NOT EXISTS stedi_db.customer_landing (
    serialnumber STRING,
    sharewithpublicasofdate STRING,
    birthday STRING,
    registrationdate STRING,
    sharewithresearchasofdate STRING,
    customername STRING,
    email STRING,
    lastupdatedate STRING,
    phone STRING,
    sharewithfriendsasofdate STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://d609-stedi-ankur-2025/landing/customer_landing/';
