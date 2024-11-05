import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1730763408057 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_db", table_name="step_trainer_landing", transformation_ctx="AWSGlueDataCatalog_node1730763408057")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1730763408851 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_db", table_name="customer_curated", transformation_ctx="AWSGlueDataCatalog_node1730763408851")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1730763468748 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1730763408851, mappings=[("customername", "string", "right_customername", "string"), ("email", "string", "right_email", "string"), ("phone", "string", "right_phone", "string"), ("birthday", "string", "right_birthday", "string"), ("serialnumber", "string", "right_serialnumber", "string"), ("registrationdate", "long", "right_registrationdate", "long"), ("lastupdatedate", "long", "right_lastupdatedate", "long"), ("sharewithresearchasofdate", "long", "right_sharewithresearchasofdate", "long"), ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"), ("sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "long")], transformation_ctx="RenamedkeysforJoin_node1730763468748")

# Script generated for node Join
AWSGlueDataCatalog_node1730763408057DF = AWSGlueDataCatalog_node1730763408057.toDF()
RenamedkeysforJoin_node1730763468748DF = RenamedkeysforJoin_node1730763468748.toDF()
Join_node1730763456036 = DynamicFrame.fromDF(AWSGlueDataCatalog_node1730763408057DF.join(RenamedkeysforJoin_node1730763468748DF, (AWSGlueDataCatalog_node1730763408057DF['serialnumber'] == RenamedkeysforJoin_node1730763468748DF['right_serialnumber']), "leftsemi"), glueContext, "Join_node1730763456036")

# Script generated for node Amazon S3
AmazonS3_node1730763569517 = glueContext.write_dynamic_frame.from_options(frame=Join_node1730763456036, connection_type="s3", format="json", connection_options={"path": "s3://project-stedi-lake-house/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1730763569517")

job.commit()