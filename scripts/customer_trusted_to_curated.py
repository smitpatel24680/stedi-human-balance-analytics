import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1730754834738 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_db", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1730754834738")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1730754835776 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project_db", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1730754835776")

# Script generated for node Join
Join_node1730754899779 = Join.apply(frame1=AWSGlueDataCatalog_node1730754835776, frame2=AWSGlueDataCatalog_node1730754834738, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1730754899779")

# Script generated for node SQL Query
SqlQuery5470 = '''
select distinct customername, email, phone, birthday,
serialnumber, registrationdate, lastupdatedate,
sharewithresearchasofdate, sharewithpublicasofdate, 
sharewithfriendsasofdate from myDataSource

'''
SQLQuery_node1730754955879 = sparkSqlQuery(glueContext, query = SqlQuery5470, mapping = {"myDataSource":Join_node1730754899779}, transformation_ctx = "SQLQuery_node1730754955879")

# Script generated for node Amazon S3
AmazonS3_node1730755099802 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1730754955879, connection_type="s3", format="json", connection_options={"path": "s3://project-stedi-lake-house/customer/curated/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1730755099802")

job.commit()