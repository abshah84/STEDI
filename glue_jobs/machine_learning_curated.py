import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx):
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read Step Trainer Trusted from catalog
StepTrainerTrusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node",
)

# Read Accelerometer Trusted from catalog
AccelerometerTrusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node",
)

# SQL Query - Simple exact timestamp match
SqlQuery0 = """
SELECT 
    s.sensorreadingtime,
    s.serialnumber,
    s.distancefromobject,
    a.user,
    a.x,
    a.y,
    a.z
FROM step_trainer_trusted s
JOIN accelerometer_trusted a 
    ON s.sensorreadingtime = a.timestamp
"""

SQLQuery_node = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "step_trainer_trusted": StepTrainerTrusted_node,
        "accelerometer_trusted": AccelerometerTrusted_node,
    },
    transformation_ctx="SQLQuery_node",
)

# Write to S3 as JSON
glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://d609-stedi-ankur-2025/curated/machine_learning_curated/",
        "partitionKeys": []
    },
    transformation_ctx="write_ml_curated"
)

job.commit()