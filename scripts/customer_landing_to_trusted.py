import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1730751312620 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_db", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1730751312620")

# Script generated for node Privacy Filter
PrivacyFilter_node1730320162727 = Filter.apply(frame=AWSGlueDataCatalog_node1730751312620, f=lambda row: (not(row["sharewithresearchasofdate"] == 0)), transformation_ctx="PrivacyFilter_node1730320162727")

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node1730320258641 = glueContext.getSink(path="s3://project-stedi-lake-house/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="TrustedCustomerZone_node1730320258641")
TrustedCustomerZone_node1730320258641.setCatalogInfo(catalogDatabase="stedi_project_db",catalogTableName="customer_trusted")
TrustedCustomerZone_node1730320258641.setFormat("json")
TrustedCustomerZone_node1730320258641.writeFrame(PrivacyFilter_node1730320162727)
job.commit()