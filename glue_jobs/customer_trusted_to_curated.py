from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import trim, col
from awsglue.dynamicframe import DynamicFrame

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read trusted tables from the Glue Data Catalog
cust_dyf = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted")
accel_dyf = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_trusted")

cust_df = cust_dyf.toDF()
accel_df = accel_dyf.toDF()

# Normalize and enforce consistent types
cust_df = cust_df.withColumn("email", trim(col("email"))) \
                 .withColumn("serialNumber", trim(col("serialNumber"))) \
                 .withColumn("shareWithResearchAsOfDate", col("shareWithResearchAsOfDate").cast("string")) \
                 .filter(
                     (col("shareWithResearchAsOfDate").isNotNull()) &
                     (F.trim(col("shareWithResearchAsOfDate")) != "")
                 )

# Build distinct set of consenting users present in accelerometer_trusted
# Assumes accelerometer.user contains the customer email
consenting_emails_df = accel_df.select(trim(col("user")).alias("email")).distinct()

# Keep only customers who appear in accelerometer_trusted (inner join)
cust_curated_df = cust_df.join(consenting_emails_df, on="email", how="inner").dropDuplicates()

# Select and order columns to match curated DDL (use actual column names)
cust_curated_df = cust_curated_df.select(
    col("serialNumber"),
    col("customerName"),
    col("email"),
    col("phone"),
    col("registrationDate"),
    col("birthDay"),
    col("lastUpdateDate"),
    col("shareWithResearchAsOfDate")
)

# Write Parquet to curated prefix (overwrite)
out_path = "s3://d609-stedi-ankur-2025/curated/customer_curated/"
cust_curated_df.write.mode("overwrite").parquet(out_path)
