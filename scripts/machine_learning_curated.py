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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1730764731678 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_db", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1730764731678")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1730764730943 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_db", table_name="step_trainer_trusted", transformation_ctx="AWSGlueDataCatalog_node1730764730943")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1730764793616 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1730764730943, mappings=[("sensorreadingtime", "long", "right_sensorreadingtime", "long"), ("serialnumber", "string", "right_serialnumber", "string"), ("distancefromobject", "int", "right_distancefromobject", "int")], transformation_ctx="RenamedkeysforJoin_node1730764793616")

# Script generated for node Join
Join_node1730764783147 = Join.apply(frame1=AWSGlueDataCatalog_node1730764731678, frame2=RenamedkeysforJoin_node1730764793616, keys1=["timestamp"], keys2=["right_sensorreadingtime"], transformation_ctx="Join_node1730764783147")

# Script generated for node Amazon S3
AmazonS3_node1730764920373 = glueContext.write_dynamic_frame.from_options(frame=Join_node1730764783147, connection_type="s3", format="json", connection_options={"path": "s3://project-stedi-lake-house/step_trainer/machine_learning/curated/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1730764920373")

job.commit()