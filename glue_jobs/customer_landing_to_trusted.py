from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import trim, col

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read landing table from Glue Data Catalog
customer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="customer_landing"
)
df = customer_landing.toDF()

# Filter consenting customers
consenting_df = df.filter(
    (col("sharewithresearchasofdate").isNotNull()) &
    (trim(col("sharewithresearchasofdate")) != "")
)

# Convert back to DynamicFrame and write Parquet
consenting_dyf = DynamicFrame.fromDF(consenting_df, glueContext, "consenting_dyf")
glueContext.write_dynamic_frame.from_options(
    frame=consenting_dyf,
    connection_type="s3",
    connection_options={"path":"s3://d609-stedi-ankur-2025/trusted/customer_trusted/"},
    format="parquet"
)
