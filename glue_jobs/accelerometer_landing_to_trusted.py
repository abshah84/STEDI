from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import trim, col

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read accelerometer landing table from Glue Data Catalog
accel_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="accelerometer_landing"
)
accel_df = accel_landing.toDF()

# Read customer_trusted emails to filter consenting users
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="customer_trusted"
).toDF().select("email").distinct()

# Normalize join keys
accel_df = accel_df.withColumn("user", trim(col("user")))
customer_trusted = customer_trusted.withColumn("email", trim(col("email")))

# Keep only accelerometer records for consenting customers
accel_filtered = accel_df.join(customer_trusted, accel_df.user == customer_trusted.email, "inner").drop(customer_trusted.email)

# Remove exact duplicate rows
accel_filtered = accel_filtered.dropDuplicates()

# Convert back to DynamicFrame and write Parquet to S3 (no catalog update)
accel_dyf = DynamicFrame.fromDF(accel_filtered, glueContext, "accel_dyf")
glueContext.write_dynamic_frame.from_options(
    frame=accel_dyf,
    connection_type="s3",
    connection_options={"path":"s3://d609-stedi-ankur-2025/trusted/accelerometer_trusted/"},
    format="parquet"
)
