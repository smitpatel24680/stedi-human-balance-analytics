import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1730753210544 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://project-stedi-lake-house/accelerometer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1730753210544")

# Script generated for node Amazon S3
AmazonS3_node1730753209056 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://project-stedi-lake-house/customer/trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1730753209056")

# Script generated for node Join
Join_node1730753266355 = Join.apply(frame1=AmazonS3_node1730753210544, frame2=AmazonS3_node1730753209056, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1730753266355")

# Script generated for node Drop Fields
DropFields_node1730753319761 = DropFields.apply(frame=Join_node1730753266355, paths=["email", "phone"], transformation_ctx="DropFields_node1730753319761")

# Script generated for node Amazon S3
AmazonS3_node1730753365401 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1730753319761, connection_type="s3", format="json", connection_options={"path": "s3://project-stedi-lake-house/accelerometer/trusted/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1730753365401")

job.commit()