import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="trungt",
    table_name="step_trainer_landing",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1676016574929 = glueContext.create_dynamic_frame.from_catalog(
    database="trungt",
    table_name="customer_curated",
    transformation_ctx="AmazonS3_node1676016574929",
)

# Script generated for node PrivacyFilter
PrivacyFilter_node2 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1676016574929,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="PrivacyFilter_node2",
)

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=PrivacyFilter_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://trungt-lake-house/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="TrustedCustomerZone_node3",
)

job.commit()
