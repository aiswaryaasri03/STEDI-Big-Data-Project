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

# Script generated for node Customer Curated
CustomerCurated_node1768980866463 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://sri-lake-house/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1768980866463")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1768980865542 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://sri-lake-house/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1768980865542")

# Script generated for node Join Query
JoinQuery_node1768982382065 = Join.apply(frame1=CustomerCurated_node1768980866463, frame2=StepTrainerLanding_node1768980865542, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="JoinQuery_node1768982382065")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=JoinQuery_node1768982382065, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1768980250568", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1768980871191 = glueContext.getSink(path="s3://sri-lake-house/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1768980871191")
StepTrainerTrusted_node1768980871191.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1768980871191.setFormat("json")
StepTrainerTrusted_node1768980871191.writeFrame(JoinQuery_node1768982382065)
job.commit()
