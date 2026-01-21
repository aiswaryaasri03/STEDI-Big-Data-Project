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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1768980866463 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://sri-lake-house/step_trainer/trusted/"], "recurse": True}, transformation_ctx="StepTrainerTrusted_node1768980866463")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1768980865542 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://sri-lake-house/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1768980865542")

# Script generated for node Join Query
SqlQuery0 = '''
select * from step_trainer_trusted s join accelerometer_trusted a 
on s.sensorReadingTime = a.timestamp;
'''
JoinQuery_node1768988309312 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_trusted":StepTrainerTrusted_node1768980866463, "accelerometer_trusted":AccelerometerTrusted_node1768980865542}, transformation_ctx = "JoinQuery_node1768988309312")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=JoinQuery_node1768988309312, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1768980250568", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1768980871191 = glueContext.getSink(path="s3://sri-lake-house/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1768980871191")
MachineLearningCurated_node1768980871191.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1768980871191.setFormat("json")
MachineLearningCurated_node1768980871191.writeFrame(JoinQuery_node1768988309312)
job.commit()
