import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1768980866463 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://sri-lake-house/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1768980866463")

# Script generated for node Customer Trusted
CustomerTrusted_node1768980865542 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://sri-lake-house/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1768980865542")

# Script generated for node Join Query
JoinQuery_node1768982382065 = Join.apply(frame1=CustomerTrusted_node1768980865542, frame2=AccelerometerTrusted_node1768980866463, keys1=["email"], keys2=["user"], transformation_ctx="JoinQuery_node1768982382065")

# Script generated for node Drop Fields
SqlQuery0 = '''
select distinct customerName, email, phone, birthDay, serialNumber, 
registrationDate, lastUpdateDate, shareWithResearchAsOfDate, 
shareWithPublicAsOfDate, shareWithFriendsAsOfDate from myDataSource;
'''
DropFields_node1768985309697 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":JoinQuery_node1768982382065}, transformation_ctx = "DropFields_node1768985309697")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=DropFields_node1768985309697, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1768980250568", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1768980871191 = glueContext.getSink(path="s3://sri-lake-house/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1768980871191")
CustomerCurated_node1768980871191.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="customer_curated")
CustomerCurated_node1768980871191.setFormat("json")
CustomerCurated_node1768980871191.writeFrame(DropFields_node1768985309697)
job.commit()
