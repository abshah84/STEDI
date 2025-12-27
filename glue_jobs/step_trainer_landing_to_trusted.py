from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import trim, col

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read step trainer landing table from Glue Data Catalog
step_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="step_trainer_landing"
)
step_df = step_landing.toDF()

# Read customer_trusted to filter consenting users by serialNumber mapping
# If your join key is different (e.g., serialNumber vs user/email), adjust accordingly.
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="customer_trusted"
).toDF().select("serialNumber").distinct()

# Normalize join keys
step_df = step_df.withColumn("serialNumber", trim(col("serialNumber")))
customer_trusted = customer_trusted.withColumn("serialNumber", trim(col("serialNumber")))

# Keep only step trainer records for consenting customers
step_filtered = step_df.join(customer_trusted, step_df.serialNumber == customer_trusted.serialNumber, "inner").drop(customer_trusted.serialNumber)

# Remove exact duplicate rows
step_filtered = step_filtered.dropDuplicates()

# Convert back to DynamicFrame and write Parquet to S3 (no catalog update)
step_dyf = DynamicFrame.fromDF(step_filtered, glueContext, "step_dyf")
glueContext.write_dynamic_frame.from_options(
    frame=step_dyf,
    connection_type="s3",
    connection_options={"path":"s3://d609-stedi-ankur-2025/trusted/step_trainer_trusted/"},
    format="parquet"
)
