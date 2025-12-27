import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1766872189011 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://d609-stedi-ankur-2025/landing/accelerometer_landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1766872189011")

# Script generated for node Amazon S3
AmazonS3_node1766872263719 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://d609-stedi-ankur-2025/trusted/customer_trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1766872263719")

# Script generated for node Join
Join_node1766872391051 = Join.apply(frame1=AmazonS3_node1766872189011, frame2=AmazonS3_node1766872263719, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1766872391051")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1766872391051, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766872163394", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1766872439377 = glueContext.getSink(path="s3://d609-stedi-ankur-2025/trusted/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1766872439377")
AmazonS3_node1766872439377.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="accelerometer_trusted")
AmazonS3_node1766872439377.setFormat("json")
AmazonS3_node1766872439377.writeFrame(Join_node1766872391051)
job.commit()